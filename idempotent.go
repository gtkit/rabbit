package rabbit

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"
)

// defaultMemorySweepThreshold 是 MemoryStore 触发一次过期清理前累计的写入次数。
const defaultMemorySweepThreshold = 1024

// IdempotentStore 是消费去重的存储后端。
//
// 实现必须并发安全；key 的过期（TTL）由实现自行管理。
// 分布式多实例部署需使用跨进程共享的实现（如 Redis / 数据库唯一键）；
// 单机 / 测试可用库内置的 MemoryStore。
type IdempotentStore interface {
	// IsDuplicate 报告 key 是否已被成功处理过（已 Commit 且未过期）。
	IsDuplicate(ctx context.Context, key string) (bool, error)
	// Commit 在消息成功处理后记录 key。
	Commit(ctx context.Context, key string) error
}

// ErrIdempotentInProgress 表示同一去重 key 已被其他投递抢占且尚未完成。
//
// 使用 IdempotentClaimHandler 时，调用方可用 errors.Is 判断该错误。
var ErrIdempotentInProgress = errors.New("mq: idempotent key is already processing")

// ErrIdempotentClaimLost 表示当前消费者已经不再持有该 key 的处理权。
//
// 常见原因是处理中租约过期后，其他消费者重新抢占了同一 key。
var ErrIdempotentClaimLost = errors.New("mq: idempotent claim is no longer owned")

// IdempotentClaimStatus 表示一次原子 claim 的结果。
type IdempotentClaimStatus uint8

const (
	// IdempotentClaimAcquired 表示本次调用成功抢占 key，应继续执行业务处理。
	IdempotentClaimAcquired IdempotentClaimStatus = iota + 1
	// IdempotentClaimDuplicate 表示 key 已完成处理，应跳过业务并 ACK 当前消息。
	IdempotentClaimDuplicate
	// IdempotentClaimInProgress 表示 key 正在处理中，应让当前消息走重试 / DLX。
	IdempotentClaimInProgress
)

// IdempotentClaim 表示一次原子 claim 的结果。
//
// Token 是存储实现返回的不透明租约令牌；Status 为 IdempotentClaimAcquired 时，
// IdempotentClaimHandler 会把该 Token 原样传给 Commit / Release，避免租约过期后
// 旧消费者误提交或释放其他消费者的新 claim。
type IdempotentClaim struct {
	Status IdempotentClaimStatus
	Token  string
}

// IdempotentClaimStore 是支持原子 claim 的消费幂等存储后端。
//
// 实现必须并发安全；分布式多实例部署必须使用跨进程共享的实现。
// 典型实现可用 Redis SETNX + TTL + value token，或数据库唯一键 / 去重表状态机。
type IdempotentClaimStore interface {
	// Claim 原子抢占 key，并返回抢占结果。
	Claim(ctx context.Context, key string) (IdempotentClaim, error)
	// Commit 在消息成功处理后将 key 标记为已完成。
	Commit(ctx context.Context, key, token string) error
	// Release 在消息处理失败后释放未完成的 key，使后续重试可重新抢占。
	Release(ctx context.Context, key, token string) error
}

// DedupKeyFunc 从消息体与 msgID 中提取去重键。
// 返回 ok=false 表示该消息不参与去重，装饰器将直接调用业务处理。
type DedupKeyFunc func(body []byte, msgID string) (key string, ok bool)

// defaultDedupKey 默认以 msgID 作为去重键；msgID 为空时不参与去重。
func defaultDedupKey(_ []byte, msgID string) (string, bool) {
	return msgID, msgID != ""
}

// DedupOption 配置 IdempotentHandler。
type DedupOption func(*idempotentHandler)

// WithDedupKey 自定义去重键提取函数，覆盖默认的 msgID 取键。
// 传入 nil 时保持默认行为。
func WithDedupKey(fn DedupKeyFunc) DedupOption {
	return func(h *idempotentHandler) {
		if fn != nil {
			h.keyFn = fn
		}
	}
}

// ClaimOption 配置 IdempotentClaimHandler。
type ClaimOption func(*idempotentClaimHandler)

// WithClaimDedupKey 自定义原子 claim 的去重键提取函数，覆盖默认的 msgID 取键。
// 传入 nil 时保持默认行为。
func WithClaimDedupKey(fn DedupKeyFunc) ClaimOption {
	return func(h *idempotentClaimHandler) {
		if fn != nil {
			h.keyFn = fn
		}
	}
}

// idempotentHandler 是带消费去重的 MsgHandler 装饰器。
type idempotentHandler struct {
	next  MsgHandler
	store IdempotentStore
	keyFn DedupKeyFunc
}

// IdempotentHandler 把 next 包装成带消费去重能力的 MsgHandler，
// 返回值可直接传给任意 Consume* 方法，不改变库的任何默认行为。
//
// 流程：按 keyFn 取键 → IsDuplicate 命中则跳过 next 并返回 nil（消息被 ACK）→
// 未命中则调用 next.Process → 仅在处理成功后 Commit。
// 处理失败不 Commit，原样返回 error，复用现有 x-retry 重试机制；
// 因此后续重投仍会重新处理，保留 at-least-once 语义。
//
// 故障策略为 fail-open：存储查询 / 提交报错时记录日志并放行，
// 保证存储故障不丢消息，代价是故障期间可能漏去重（出现重复处理）。
//
// 该装饰器无法消除“处理成功但 Commit 前进程崩溃”这一极小窗口的重复——
// 那是 exactly-once 的固有难题，需要业务侧把处理与去重记录放进同一事务，
// 库不假装能解决。
//
// 去重非原子：IsDuplicate → Process → Commit 三步之间没有锁。它能拦截
// 顺序重投（ACK 丢失后 broker 重发同一条），但拦不住同一 key 的并发重复——
// 多个 worker 同时收到时可能都先通过 IsDuplicate、再各自处理一次。
// 若需并发去重，请使用实现原子 claim 语义的 IdempotentClaimStore
// （如 Redis SETNX、数据库唯一键），仅靠 MemoryStore 无法保证。
//
// next 为 nil 时返回 nil，使后续 Consume* 的 nil 校验（ErrHandlerRequired）正常生效；
// store 为 nil 时返回 next 本身（等价于不开启去重）。
func IdempotentHandler(next MsgHandler, store IdempotentStore, opts ...DedupOption) MsgHandler {
	if next == nil {
		return nil
	}
	if store == nil {
		return next
	}

	h := &idempotentHandler{
		next:  next,
		store: store,
		keyFn: defaultDedupKey,
	}
	for _, apply := range opts {
		if apply != nil {
			apply(h)
		}
	}

	return h
}

// Process 执行去重逻辑：查重 → 跳过 / 处理 → 成功提交。
func (h *idempotentHandler) Process(body []byte, msgID string) error {
	key, ok := h.keyFn(body, msgID)
	if !ok {
		return h.next.Process(body, msgID)
	}

	ctx := context.Background()

	dup, err := h.store.IsDuplicate(ctx, key)
	switch {
	case err != nil:
		// fail-open：查询故障按首次消息继续处理。
		currentLogger().Errorf("idempotent: IsDuplicate failed (key=%s): %v; processing anyway", key, err)
	case dup:
		return nil
	}

	if perr := h.next.Process(body, msgID); perr != nil {
		return perr
	}

	if cerr := h.store.Commit(ctx, key); cerr != nil {
		// fail-open：业务已成功，提交故障不触发重试，仅记录。
		currentLogger().Errorf("idempotent: Commit failed (key=%s): %v; message acked without dedup record", key, cerr)
	}

	return nil
}

// Failed 透传给被包装的 handler。
func (h *idempotentHandler) Failed(msg FailedMsg) {
	h.next.Failed(msg)
}

// idempotentClaimHandler 是带原子 claim 消费幂等的 MsgHandler 装饰器。
type idempotentClaimHandler struct {
	next  MsgHandler
	store IdempotentClaimStore
	keyFn DedupKeyFunc
}

// IdempotentClaimHandler 把 next 包装成带原子 claim 幂等能力的 MsgHandler，
// 返回值可直接传给任意 Consume* 方法，不改变库的任何默认行为。
//
// 流程：按 keyFn 取键 → Claim 原子抢占 → 已完成重复则跳过 next 并返回 nil（消息被 ACK）→
// 处理中重复则返回 ErrIdempotentInProgress，复用现有 retry / DLX → 抢占成功则调用
// next.Process → 失败 Release 并返回 error → 成功 Commit 并返回 nil。
//
// 与 IdempotentHandler 的区别：本装饰器要求存储提供原子抢占与状态机语义；
// 存储错误会返回 error（fail-closed），适合生产级并发去重。严格 exactly-once 仍需
// 业务侧把业务写入与去重状态放入同一事务。
//
// next 为 nil 时返回 nil，使后续 Consume* 的 nil 校验（ErrHandlerRequired）正常生效；
// store 为 nil 时返回 next 本身（等价于不开启原子 claim）。
func IdempotentClaimHandler(next MsgHandler, store IdempotentClaimStore, opts ...ClaimOption) MsgHandler {
	if next == nil {
		return nil
	}
	if store == nil {
		return next
	}

	h := &idempotentClaimHandler{
		next:  next,
		store: store,
		keyFn: defaultDedupKey,
	}
	for _, apply := range opts {
		if apply != nil {
			apply(h)
		}
	}

	return h
}

// Process 执行原子 claim 幂等逻辑：抢占 → 跳过 / 重试 / 处理 → 释放或提交。
func (h *idempotentClaimHandler) Process(body []byte, msgID string) error {
	key, ok := h.keyFn(body, msgID)
	if !ok {
		return h.next.Process(body, msgID)
	}

	ctx := context.Background()

	claim, err := h.store.Claim(ctx, key)
	if err != nil {
		return fmt.Errorf("idempotent claim: claim key %q: %w", key, err)
	}

	switch claim.Status {
	case IdempotentClaimAcquired:
	case IdempotentClaimDuplicate:
		return nil
	case IdempotentClaimInProgress:
		return fmt.Errorf("idempotent claim: key %q: %w", key, ErrIdempotentInProgress)
	default:
		return fmt.Errorf("idempotent claim: invalid claim status %d for key %q", claim.Status, key)
	}

	if perr := h.next.Process(body, msgID); perr != nil {
		if rerr := h.store.Release(ctx, key, claim.Token); rerr != nil {
			return errors.Join(perr, fmt.Errorf("idempotent claim: release key %q: %w", key, rerr))
		}
		return perr
	}

	if cerr := h.store.Commit(ctx, key, claim.Token); cerr != nil {
		return fmt.Errorf("idempotent claim: commit key %q: %w", key, cerr)
	}

	return nil
}

// Failed 透传给被包装的 handler。
func (h *idempotentClaimHandler) Failed(msg FailedMsg) {
	h.next.Failed(msg)
}

// MemoryStore 是 IdempotentStore 的内存实现：并发安全、按 TTL 过期。
//
// 适用于单机 / 测试；分布式多实例无法跨进程去重，应改用 Redis 等共享存储。
// 构造函数不启动后台 goroutine：过期项在 Commit 时摊还清理，
// 并在 IsDuplicate 命中过期项时惰性删除。
//
// 零值可直接使用，等价于 ttl=0 的永不过期存储（仅适合测试，map 会无限增长）；
// 长跑进程请用 NewMemoryStore 设置 TTL。
type MemoryStore struct {
	ttl time.Duration

	mu            sync.Mutex
	seen          map[string]int64 // key -> 过期时刻（UnixNano）；0 表示永不过期
	opsSinceSweep int
}

// NewMemoryStore 创建内存去重存储。
//
// ttl 是 key 的存活时长；ttl <= 0 时 key 永不过期——仅适合测试，
// 长跑进程请设置 TTL，否则 map 会随消息量无限增长。
func NewMemoryStore(ttl time.Duration) *MemoryStore {
	return &MemoryStore{
		ttl:  ttl,
		seen: make(map[string]int64),
	}
}

// IsDuplicate 报告 key 是否已记录且未过期；命中过期项时惰性删除。
func (s *MemoryStore) IsDuplicate(_ context.Context, key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	exp, ok := s.seen[key]
	if !ok {
		return false, nil
	}
	if exp != 0 && time.Now().UnixNano() >= exp {
		delete(s.seen, key)
		return false, nil
	}

	return true, nil
}

// Commit 记录 key，按 ttl 设置过期时刻。
func (s *MemoryStore) Commit(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.seen == nil {
		s.seen = make(map[string]int64)
	}

	var exp int64
	if s.ttl > 0 {
		exp = time.Now().Add(s.ttl).UnixNano()
	}
	s.seen[key] = exp

	s.opsSinceSweep++
	if s.opsSinceSweep >= defaultMemorySweepThreshold {
		s.sweepLocked()
		s.opsSinceSweep = 0
	}

	return nil
}

// sweepLocked 删除所有已过期的 key，调用方须持有 s.mu。
func (s *MemoryStore) sweepLocked() {
	if s.ttl <= 0 {
		return
	}

	now := time.Now().UnixNano()
	for k, exp := range s.seen {
		if exp != 0 && now >= exp {
			delete(s.seen, k)
		}
	}
}

// MemoryClaimStore 是 IdempotentClaimStore 的内存实现：并发安全、按状态 TTL 过期。
//
// 适用于单机 / 测试；分布式多实例无法跨进程去重，应改用 Redis / 数据库等共享存储。
// 构造函数不启动后台 goroutine：过期项在 Claim / Commit 时摊还清理。
//
// 零值可直接使用，等价于 processingTTL=0、doneTTL=0 的永不过期存储（仅适合测试）；
// 长跑进程请用 NewMemoryClaimStore 设置处理中 TTL 与完成 TTL。
type MemoryClaimStore struct {
	processingTTL time.Duration
	doneTTL       time.Duration

	mu            sync.Mutex
	records       map[string]memoryClaimRecord
	opsSinceSweep int
	nextToken     uint64
}

type memoryClaimRecord struct {
	status    IdempotentClaimStatus
	token     string
	expiresAt int64 // UnixNano；0 表示永不过期
}

// NewMemoryClaimStore 创建内存原子 claim 去重存储。
//
// processingTTL 是处理中占位的存活时长；doneTTL 是完成记录的存活时长。
// 任一 TTL <= 0 时对应状态永不过期——仅适合测试；生产长跑进程请设置正 TTL。
func NewMemoryClaimStore(processingTTL, doneTTL time.Duration) *MemoryClaimStore {
	return &MemoryClaimStore{
		processingTTL: processingTTL,
		doneTTL:       doneTTL,
		records:       make(map[string]memoryClaimRecord),
	}
}

// Claim 原子抢占 key；已完成返回 IdempotentClaimDuplicate，处理中返回 IdempotentClaimInProgress。
func (s *MemoryClaimStore) Claim(_ context.Context, key string) (IdempotentClaim, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensureRecordsLocked()
	now := time.Now().UnixNano()
	if rec, ok := s.records[key]; ok {
		if !rec.expired(now) {
			return IdempotentClaim{Status: rec.status}, nil
		}
		delete(s.records, key)
	}

	token := s.nextTokenLocked()
	s.records[key] = memoryClaimRecord{
		status:    IdempotentClaimInProgress,
		token:     token,
		expiresAt: expiresAt(s.processingTTL),
	}
	s.sweepAfterWriteLocked()

	return IdempotentClaim{Status: IdempotentClaimAcquired, Token: token}, nil
}

// Commit 将 key 标记为已完成，按 doneTTL 设置过期时刻。
func (s *MemoryClaimStore) Commit(_ context.Context, key, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensureRecordsLocked()
	rec, ok := s.records[key]
	now := time.Now().UnixNano()
	if !ok || rec.status != IdempotentClaimInProgress || rec.token != token || rec.expired(now) {
		if ok && rec.expired(now) {
			delete(s.records, key)
		}
		return ErrIdempotentClaimLost
	}

	s.records[key] = memoryClaimRecord{
		status:    IdempotentClaimDuplicate,
		expiresAt: expiresAt(s.doneTTL),
	}
	s.sweepAfterWriteLocked()

	return nil
}

// Release 释放处理中 key；已完成或未知 key 会被保留 / 忽略。
func (s *MemoryClaimStore) Release(_ context.Context, key, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok := s.records[key]
	if ok && rec.status == IdempotentClaimInProgress && rec.token == token {
		delete(s.records, key)
	}

	return nil
}

func (s *MemoryClaimStore) ensureRecordsLocked() {
	if s.records == nil {
		s.records = make(map[string]memoryClaimRecord)
	}
}

func (s *MemoryClaimStore) nextTokenLocked() string {
	s.nextToken++
	return strconv.FormatUint(s.nextToken, 10)
}

func (s *MemoryClaimStore) sweepAfterWriteLocked() {
	s.opsSinceSweep++
	if s.opsSinceSweep < defaultMemorySweepThreshold {
		return
	}

	now := time.Now().UnixNano()
	for k, rec := range s.records {
		if rec.expired(now) {
			delete(s.records, k)
		}
	}
	s.opsSinceSweep = 0
}

func (r memoryClaimRecord) expired(now int64) bool {
	return r.expiresAt != 0 && now >= r.expiresAt
}

func expiresAt(ttl time.Duration) int64 {
	if ttl <= 0 {
		return 0
	}
	return time.Now().Add(ttl).UnixNano()
}
