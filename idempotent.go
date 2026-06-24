package rabbit

import (
	"context"
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
// store 为 nil 时返回 next 本身（等价于不开启去重）。
func IdempotentHandler(next MsgHandler, store IdempotentStore, opts ...DedupOption) MsgHandler {
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

// MemoryStore 是 IdempotentStore 的内存实现：并发安全、按 TTL 过期。
//
// 适用于单机 / 测试；分布式多实例无法跨进程去重，应改用 Redis 等共享存储。
// 构造函数不启动后台 goroutine：过期项在 Commit 时摊还清理，
// 并在 IsDuplicate 命中过期项时惰性删除。
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
