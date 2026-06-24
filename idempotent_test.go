package rabbit

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"
)

// idemRecHandler 是测试用 MsgHandler，记录 Process / Failed 调用。
type idemRecHandler struct {
	mu         sync.Mutex
	processIDs []string
	failed     []FailedMsg
	err        error // Process 返回的错误
}

func (h *idemRecHandler) Process(_ []byte, msgID string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.processIDs = append(h.processIDs, msgID)
	return h.err
}

func (h *idemRecHandler) Failed(msg FailedMsg) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.failed = append(h.failed, msg)
}

func (h *idemRecHandler) processCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.processIDs)
}

// idemFakeStore 是可编程的 IdempotentStore。
type idemFakeStore struct {
	mu          sync.Mutex
	committed   map[string]struct{}
	dupErr      error
	commitErr   error
	commitCalls int
}

func newIdemFakeStore() *idemFakeStore {
	return &idemFakeStore{committed: make(map[string]struct{})}
}

func (s *idemFakeStore) IsDuplicate(_ context.Context, key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dupErr != nil {
		return false, s.dupErr
	}
	_, ok := s.committed[key]
	return ok, nil
}

func (s *idemFakeStore) Commit(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commitCalls++
	if s.commitErr != nil {
		return s.commitErr
	}
	s.committed[key] = struct{}{}
	return nil
}

func (s *idemFakeStore) isCommitted(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.committed[key]
	return ok
}

// idemFakeClaimStore 是可编程的 IdempotentClaimStore。
type idemFakeClaimStore struct {
	mu            sync.Mutex
	claimStatus   IdempotentClaimStatus
	claimErr      error
	commitErr     error
	releaseErr    error
	claimKeys     []string
	commitKeys    []string
	commitTokens  []string
	releaseKeys   []string
	releaseTokens []string
	claimCalls    int
	commitCalls   int
	releaseCalls  int
}

func (s *idemFakeClaimStore) Claim(_ context.Context, key string) (IdempotentClaim, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claimCalls++
	s.claimKeys = append(s.claimKeys, key)
	if s.claimErr != nil {
		return IdempotentClaim{}, s.claimErr
	}
	if s.claimStatus == 0 {
		return IdempotentClaim{Status: IdempotentClaimAcquired, Token: "token"}, nil
	}
	return IdempotentClaim{Status: s.claimStatus, Token: "token"}, nil
}

func (s *idemFakeClaimStore) Commit(_ context.Context, key, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commitCalls++
	s.commitKeys = append(s.commitKeys, key)
	s.commitTokens = append(s.commitTokens, token)
	return s.commitErr
}

func (s *idemFakeClaimStore) Release(_ context.Context, key, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releaseCalls++
	s.releaseKeys = append(s.releaseKeys, key)
	s.releaseTokens = append(s.releaseTokens, token)
	return s.releaseErr
}

func TestIdempotentHandler_FirstMessageProcessedAndCommitted(t *testing.T) {
	next := &idemRecHandler{}
	store := newIdemFakeStore()
	h := IdempotentHandler(next, store)

	if err := h.Process([]byte("payload"), "msg-1"); err != nil {
		t.Fatalf("Process returned error: %v", err)
	}
	if got := next.processCount(); got != 1 {
		t.Fatalf("next.Process called %d times, want 1", got)
	}
	if !store.isCommitted("msg-1") {
		t.Fatalf("key msg-1 not committed after successful process")
	}
}

func TestIdempotentHandler_DuplicateSkipped(t *testing.T) {
	next := &idemRecHandler{}
	store := newIdemFakeStore()
	h := IdempotentHandler(next, store)

	if err := h.Process([]byte("p"), "msg-dup"); err != nil {
		t.Fatalf("first Process error: %v", err)
	}
	// 模拟重投：同一 key 再次到达。
	if err := h.Process([]byte("p"), "msg-dup"); err != nil {
		t.Fatalf("second Process error: %v", err)
	}

	if got := next.processCount(); got != 1 {
		t.Fatalf("next.Process called %d times, want 1 (duplicate must be skipped)", got)
	}
}

func TestIdempotentHandler_ProcessFailureNotCommitted(t *testing.T) {
	wantErr := errors.New("business failure")
	next := &idemRecHandler{err: wantErr}
	store := newIdemFakeStore()
	h := IdempotentHandler(next, store)

	if err := h.Process([]byte("p"), "msg-fail"); !errors.Is(err, wantErr) {
		t.Fatalf("Process error = %v, want %v", err, wantErr)
	}
	if store.commitCalls != 0 {
		t.Fatalf("Commit called %d times on failure, want 0", store.commitCalls)
	}
	if store.isCommitted("msg-fail") {
		t.Fatalf("failed message must not be committed")
	}
}

func TestIdempotentHandler_KeyUnavailableBypasses(t *testing.T) {
	next := &idemRecHandler{}
	store := newIdemFakeStore()
	h := IdempotentHandler(next, store)

	// 默认 keyFn 在 msgID 为空时返回 ok=false。
	if err := h.Process([]byte("p"), ""); err != nil {
		t.Fatalf("Process error: %v", err)
	}
	if got := next.processCount(); got != 1 {
		t.Fatalf("next.Process called %d times, want 1", got)
	}
	if store.commitCalls != 0 {
		t.Fatalf("Commit called %d times for bypassed message, want 0", store.commitCalls)
	}
}

func TestIdempotentHandler_CustomKeyFunc(t *testing.T) {
	next := &idemRecHandler{}
	store := newIdemFakeStore()
	// 用 body 作为业务键，忽略 msgID。
	h := IdempotentHandler(next, store, WithDedupKey(func(body []byte, _ string) (string, bool) {
		return string(body), len(body) > 0
	}))

	if err := h.Process([]byte("order-7"), "msg-a"); err != nil {
		t.Fatalf("first Process error: %v", err)
	}
	if err := h.Process([]byte("order-7"), "msg-b"); err != nil {
		t.Fatalf("second Process error: %v", err)
	}

	if got := next.processCount(); got != 1 {
		t.Fatalf("next.Process called %d times, want 1 (same business key)", got)
	}
	if !store.isCommitted("order-7") {
		t.Fatalf("business key order-7 not committed")
	}
}

func TestIdempotentHandler_NilStoreReturnsNext(t *testing.T) {
	next := &idemRecHandler{}
	h := IdempotentHandler(next, nil)

	if h != MsgHandler(next) {
		t.Fatalf("nil store must return next handler unchanged")
	}
}

func TestIdempotentHandler_NilNextReturnsNil(t *testing.T) {
	// next 为 nil 时必须返回 nil，而非包裹 nil 的非 nil 装饰器，
	// 否则会绕过 Consume* 的 ErrHandlerRequired 校验并在处理时 panic。
	store := newIdemFakeStore()
	if h := IdempotentHandler(nil, store); h != nil {
		t.Fatalf("nil next must return nil handler, got %#v", h)
	}
}

func TestIdempotentHandler_IsDuplicateErrorFailOpen(t *testing.T) {
	next := &idemRecHandler{}
	store := newIdemFakeStore()
	store.dupErr = errors.New("redis timeout")
	h := IdempotentHandler(next, store)

	if err := h.Process([]byte("p"), "msg-x"); err != nil {
		t.Fatalf("fail-open: Process should not error on IsDuplicate failure, got %v", err)
	}
	if got := next.processCount(); got != 1 {
		t.Fatalf("fail-open: next.Process called %d times, want 1", got)
	}
}

func TestIdempotentHandler_CommitErrorFailOpen(t *testing.T) {
	next := &idemRecHandler{}
	store := newIdemFakeStore()
	store.commitErr = errors.New("redis down")
	h := IdempotentHandler(next, store)

	// Commit 失败时业务已成功，不应触发重试（返回 nil）。
	if err := h.Process([]byte("p"), "msg-y"); err != nil {
		t.Fatalf("fail-open: Process should not error on Commit failure, got %v", err)
	}
	if got := next.processCount(); got != 1 {
		t.Fatalf("next.Process called %d times, want 1", got)
	}
}

func TestIdempotentHandler_FailedPassthrough(t *testing.T) {
	next := &idemRecHandler{}
	store := newIdemFakeStore()
	h := IdempotentHandler(next, store)

	h.Failed(FailedMsg{QueueName: "q", MessageID: "m", Message: []byte("b")})

	next.mu.Lock()
	defer next.mu.Unlock()
	if len(next.failed) != 1 || next.failed[0].MessageID != "m" {
		t.Fatalf("Failed not passed through to next: %+v", next.failed)
	}
}

func TestMemoryStore_DuplicateWithinTTL(t *testing.T) {
	store := NewMemoryStore(time.Minute)
	ctx := context.Background()

	if commitErr := store.Commit(ctx, "k1"); commitErr != nil {
		t.Fatalf("Commit error: %v", commitErr)
	}
	dup, err := store.IsDuplicate(ctx, "k1")
	if err != nil {
		t.Fatalf("IsDuplicate error: %v", err)
	}
	if !dup {
		t.Fatalf("key within TTL must be duplicate")
	}
}

func TestMemoryStore_ExpiredTreatedAsNew(t *testing.T) {
	store := NewMemoryStore(20 * time.Millisecond)
	ctx := context.Background()

	if err := store.Commit(ctx, "k2"); err != nil {
		t.Fatalf("Commit error: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	dup, err := store.IsDuplicate(ctx, "k2")
	if err != nil {
		t.Fatalf("IsDuplicate error: %v", err)
	}
	if dup {
		t.Fatalf("expired key must be treated as new")
	}
}

func TestMemoryStore_UnknownKey(t *testing.T) {
	store := NewMemoryStore(time.Minute)
	dup, err := store.IsDuplicate(context.Background(), "never-seen")
	if err != nil {
		t.Fatalf("IsDuplicate error: %v", err)
	}
	if dup {
		t.Fatalf("unknown key must not be duplicate")
	}
}

func TestMemoryStore_SweepEvictsExpired(t *testing.T) {
	store := NewMemoryStore(time.Millisecond)
	ctx := context.Background()

	// 提交一个 key 并等其过期。
	if err := store.Commit(ctx, "old"); err != nil {
		t.Fatalf("Commit error: %v", err)
	}
	time.Sleep(5 * time.Millisecond)

	// 触发足量写入越过清理阈值，引发一次 sweep。
	for i := range defaultMemorySweepThreshold {
		if err := store.Commit(ctx, "k-"+strconv.Itoa(i)); err != nil {
			t.Fatalf("Commit error: %v", err)
		}
	}

	store.mu.Lock()
	_, exists := store.seen["old"]
	store.mu.Unlock()
	if exists {
		t.Fatalf("expired key should be swept after threshold writes")
	}
}

func TestMemoryStore_ZeroValueUsable(t *testing.T) {
	// 零值 MemoryStore 必须可直接使用（懒初始化 seen），等价于永不过期。
	var store MemoryStore
	ctx := context.Background()

	if err := store.Commit(ctx, "z1"); err != nil {
		t.Fatalf("zero-value Commit error: %v", err)
	}
	dup, err := store.IsDuplicate(ctx, "z1")
	if err != nil {
		t.Fatalf("zero-value IsDuplicate error: %v", err)
	}
	if !dup {
		t.Fatalf("zero-value store must report committed key as duplicate")
	}
}

func TestMemoryStore_ZeroValueIsDuplicateUnknown(t *testing.T) {
	// 未 Commit 前对零值 store 查重不得 panic（读 nil map 安全）。
	var store MemoryStore
	dup, err := store.IsDuplicate(context.Background(), "never")
	if err != nil {
		t.Fatalf("zero-value IsDuplicate error: %v", err)
	}
	if dup {
		t.Fatalf("unknown key must not be duplicate")
	}
}

func TestMemoryStore_Concurrent(t *testing.T) {
	store := NewMemoryStore(time.Minute)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := "k-" + strconv.Itoa(n%10)
			_, _ = store.IsDuplicate(ctx, key)
			_ = store.Commit(ctx, key)
			_, _ = store.IsDuplicate(ctx, key)
		}(i)
	}
	wg.Wait()
}

func TestIdempotentClaimHandler_AcquiredProcessedAndCommitted(t *testing.T) {
	next := &idemRecHandler{}
	store := &idemFakeClaimStore{}
	h := IdempotentClaimHandler(next, store)

	if err := h.Process([]byte("payload"), "msg-1"); err != nil {
		t.Fatalf("Process error: %v", err)
	}
	if got := next.processCount(); got != 1 {
		t.Fatalf("next.Process called %d times, want 1", got)
	}
	if store.commitCalls != 1 || store.commitKeys[0] != "msg-1" {
		t.Fatalf("Commit calls = %d keys=%v, want key msg-1", store.commitCalls, store.commitKeys)
	}
	if store.commitTokens[0] != "token" {
		t.Fatalf("Commit token = %q, want token", store.commitTokens[0])
	}
	if store.releaseCalls != 0 {
		t.Fatalf("Release called %d times, want 0", store.releaseCalls)
	}
}

func TestIdempotentClaimHandler_CompletedDuplicateSkipped(t *testing.T) {
	next := &idemRecHandler{}
	store := &idemFakeClaimStore{claimStatus: IdempotentClaimDuplicate}
	h := IdempotentClaimHandler(next, store)

	if err := h.Process([]byte("payload"), "msg-dup"); err != nil {
		t.Fatalf("Process error: %v", err)
	}
	if got := next.processCount(); got != 0 {
		t.Fatalf("next.Process called %d times, want 0", got)
	}
	if store.commitCalls != 0 || store.releaseCalls != 0 {
		t.Fatalf("commit/release calls = %d/%d, want 0/0", store.commitCalls, store.releaseCalls)
	}
}

func TestIdempotentClaimHandler_InProgressReturnsSentinel(t *testing.T) {
	next := &idemRecHandler{}
	store := &idemFakeClaimStore{claimStatus: IdempotentClaimInProgress}
	h := IdempotentClaimHandler(next, store)

	err := h.Process([]byte("payload"), "msg-busy")
	if !errors.Is(err, ErrIdempotentInProgress) {
		t.Fatalf("Process error = %v, want ErrIdempotentInProgress", err)
	}
	if got := next.processCount(); got != 0 {
		t.Fatalf("next.Process called %d times, want 0", got)
	}
}

func TestIdempotentClaimHandler_KeyUnavailableBypassesClaim(t *testing.T) {
	next := &idemRecHandler{}
	store := &idemFakeClaimStore{}
	h := IdempotentClaimHandler(next, store)

	if err := h.Process([]byte("payload"), ""); err != nil {
		t.Fatalf("Process error: %v", err)
	}
	if got := next.processCount(); got != 1 {
		t.Fatalf("next.Process called %d times, want 1", got)
	}
	if store.claimCalls != 0 {
		t.Fatalf("Claim called %d times, want 0", store.claimCalls)
	}
}

func TestIdempotentClaimHandler_CustomBusinessKey(t *testing.T) {
	next := &idemRecHandler{}
	store := NewMemoryClaimStore(time.Minute, time.Minute)
	h := IdempotentClaimHandler(next, store, WithClaimDedupKey(func(body []byte, _ string) (string, bool) {
		return string(body), len(body) > 0
	}))

	if err := h.Process([]byte("order-9"), "msg-a"); err != nil {
		t.Fatalf("first Process error: %v", err)
	}
	if err := h.Process([]byte("order-9"), "msg-b"); err != nil {
		t.Fatalf("second Process error: %v", err)
	}
	if got := next.processCount(); got != 1 {
		t.Fatalf("next.Process called %d times, want 1", got)
	}
}

func TestIdempotentClaimHandler_ProcessFailureReleasesClaim(t *testing.T) {
	wantErr := errors.New("business failure")
	next := &idemRecHandler{err: wantErr}
	store := &idemFakeClaimStore{}
	h := IdempotentClaimHandler(next, store)

	if err := h.Process([]byte("payload"), "msg-fail"); !errors.Is(err, wantErr) {
		t.Fatalf("Process error = %v, want %v", err, wantErr)
	}
	if store.releaseCalls != 1 || store.releaseKeys[0] != "msg-fail" {
		t.Fatalf("Release calls = %d keys=%v, want key msg-fail", store.releaseCalls, store.releaseKeys)
	}
	if store.releaseTokens[0] != "token" {
		t.Fatalf("Release token = %q, want token", store.releaseTokens[0])
	}
	if store.commitCalls != 0 {
		t.Fatalf("Commit called %d times, want 0", store.commitCalls)
	}
}

func TestIdempotentClaimHandler_ReleaseErrorPreservesBothErrors(t *testing.T) {
	businessErr := errors.New("business failure")
	releaseErr := errors.New("release failed")
	next := &idemRecHandler{err: businessErr}
	store := &idemFakeClaimStore{releaseErr: releaseErr}
	h := IdempotentClaimHandler(next, store)

	err := h.Process([]byte("payload"), "msg-release")
	if !errors.Is(err, businessErr) {
		t.Fatalf("Process error = %v, want business error", err)
	}
	if !errors.Is(err, releaseErr) {
		t.Fatalf("Process error = %v, want release error", err)
	}
}

func TestIdempotentClaimHandler_StoreErrors(t *testing.T) {
	t.Run("claim error", func(t *testing.T) {
		wantErr := errors.New("redis down")
		next := &idemRecHandler{}
		store := &idemFakeClaimStore{claimErr: wantErr}
		h := IdempotentClaimHandler(next, store)

		if err := h.Process([]byte("payload"), "msg-claim"); !errors.Is(err, wantErr) {
			t.Fatalf("Process error = %v, want %v", err, wantErr)
		}
		if got := next.processCount(); got != 0 {
			t.Fatalf("next.Process called %d times, want 0", got)
		}
	})

	t.Run("commit error", func(t *testing.T) {
		wantErr := errors.New("commit failed")
		next := &idemRecHandler{}
		store := &idemFakeClaimStore{commitErr: wantErr}
		h := IdempotentClaimHandler(next, store)

		if err := h.Process([]byte("payload"), "msg-commit"); !errors.Is(err, wantErr) {
			t.Fatalf("Process error = %v, want %v", err, wantErr)
		}
		if got := next.processCount(); got != 1 {
			t.Fatalf("next.Process called %d times, want 1", got)
		}
	})
}

func TestIdempotentClaimHandler_InvalidClaimStatus(t *testing.T) {
	next := &idemRecHandler{}
	store := &idemFakeClaimStore{claimStatus: IdempotentClaimStatus(99)}
	h := IdempotentClaimHandler(next, store)

	if err := h.Process([]byte("payload"), "msg-invalid"); err == nil {
		t.Fatal("Process error = nil, want invalid status error")
	}
	if got := next.processCount(); got != 0 {
		t.Fatalf("next.Process called %d times, want 0", got)
	}
}

func TestIdempotentClaimHandler_NilInputs(t *testing.T) {
	next := &idemRecHandler{}
	if h := IdempotentClaimHandler(next, nil); h != MsgHandler(next) {
		t.Fatalf("nil store must return next handler unchanged")
	}
	if h := IdempotentClaimHandler(nil, &idemFakeClaimStore{}); h != nil {
		t.Fatalf("nil next must return nil handler, got %#v", h)
	}
}

func TestIdempotentClaimHandler_FailedPassthrough(t *testing.T) {
	next := &idemRecHandler{}
	h := IdempotentClaimHandler(next, &idemFakeClaimStore{})

	h.Failed(FailedMsg{QueueName: "q", MessageID: "m", Message: []byte("b")})

	next.mu.Lock()
	defer next.mu.Unlock()
	if len(next.failed) != 1 || next.failed[0].MessageID != "m" {
		t.Fatalf("Failed not passed through to next: %+v", next.failed)
	}
}

func TestMemoryClaimStore_CompletedDuplicateWithinTTL(t *testing.T) {
	store := NewMemoryClaimStore(time.Minute, time.Minute)
	ctx := context.Background()

	claim, err := store.Claim(ctx, "k1")
	if err != nil || claim.Status != IdempotentClaimAcquired {
		t.Fatalf("Claim = %+v, %v; want acquired nil", claim, err)
	}
	if commitErr := store.Commit(ctx, "k1", claim.Token); commitErr != nil {
		t.Fatalf("Commit error: %v", commitErr)
	}
	claim, err = store.Claim(ctx, "k1")
	if err != nil || claim.Status != IdempotentClaimDuplicate {
		t.Fatalf("Claim duplicate = %+v, %v; want duplicate nil", claim, err)
	}
}

func TestMemoryClaimStore_ProcessingDuplicateWithinTTL(t *testing.T) {
	store := NewMemoryClaimStore(time.Minute, time.Minute)
	ctx := context.Background()

	claim, err := store.Claim(ctx, "k2")
	if err != nil || claim.Status != IdempotentClaimAcquired {
		t.Fatalf("first Claim = %+v, %v; want acquired nil", claim, err)
	}
	claim, err = store.Claim(ctx, "k2")
	if err != nil || claim.Status != IdempotentClaimInProgress {
		t.Fatalf("second Claim = %+v, %v; want in-progress nil", claim, err)
	}
}

func TestMemoryClaimStore_ProcessingExpiryAllowsReclaim(t *testing.T) {
	store := NewMemoryClaimStore(20*time.Millisecond, time.Minute)
	ctx := context.Background()

	claim, err := store.Claim(ctx, "k3")
	if err != nil || claim.Status != IdempotentClaimAcquired {
		t.Fatalf("first Claim = %+v, %v; want acquired nil", claim, err)
	}
	time.Sleep(50 * time.Millisecond)
	claim, err = store.Claim(ctx, "k3")
	if err != nil || claim.Status != IdempotentClaimAcquired {
		t.Fatalf("expired Claim = %+v, %v; want acquired nil", claim, err)
	}
}

func TestMemoryClaimStore_ReleaseAllowsReclaim(t *testing.T) {
	store := NewMemoryClaimStore(time.Minute, time.Minute)
	ctx := context.Background()

	claim, err := store.Claim(ctx, "k4")
	if err != nil || claim.Status != IdempotentClaimAcquired {
		t.Fatalf("Claim = %+v, %v; want acquired nil", claim, err)
	}
	if releaseErr := store.Release(ctx, "k4", claim.Token); releaseErr != nil {
		t.Fatalf("Release error: %v", releaseErr)
	}
	claim, err = store.Claim(ctx, "k4")
	if err != nil || claim.Status != IdempotentClaimAcquired {
		t.Fatalf("Claim after release = %+v, %v; want acquired nil", claim, err)
	}
}

func TestMemoryClaimStore_ReleaseDoesNotDeleteCompleted(t *testing.T) {
	store := NewMemoryClaimStore(time.Minute, time.Minute)
	ctx := context.Background()

	claim, err := store.Claim(ctx, "k5")
	if err != nil || claim.Status != IdempotentClaimAcquired {
		t.Fatalf("Claim = %+v, %v; want acquired nil", claim, err)
	}
	if commitErr := store.Commit(ctx, "k5", claim.Token); commitErr != nil {
		t.Fatalf("Commit error: %v", commitErr)
	}
	if releaseErr := store.Release(ctx, "k5", claim.Token); releaseErr != nil {
		t.Fatalf("Release error: %v", releaseErr)
	}
	claim, err = store.Claim(ctx, "k5")
	if err != nil || claim.Status != IdempotentClaimDuplicate {
		t.Fatalf("Claim after release completed = %+v, %v; want duplicate nil", claim, err)
	}
}

func TestMemoryClaimStore_StaleReleaseDoesNotDeleteNewClaim(t *testing.T) {
	store := NewMemoryClaimStore(20*time.Millisecond, time.Minute)
	ctx := context.Background()

	first, err := store.Claim(ctx, "k6")
	if err != nil || first.Status != IdempotentClaimAcquired {
		t.Fatalf("first Claim = %+v, %v; want acquired nil", first, err)
	}
	time.Sleep(50 * time.Millisecond)
	second, err := store.Claim(ctx, "k6")
	if err != nil || second.Status != IdempotentClaimAcquired {
		t.Fatalf("second Claim = %+v, %v; want acquired nil", second, err)
	}
	if first.Token == second.Token {
		t.Fatalf("tokens must differ after reclaim, got %q", first.Token)
	}
	if releaseErr := store.Release(ctx, "k6", first.Token); releaseErr != nil {
		t.Fatalf("stale Release error: %v", releaseErr)
	}
	claim, err := store.Claim(ctx, "k6")
	if err != nil || claim.Status != IdempotentClaimInProgress {
		t.Fatalf("Claim after stale release = %+v, %v; want in-progress nil", claim, err)
	}
}

func TestMemoryClaimStore_StaleCommitReturnsClaimLost(t *testing.T) {
	store := NewMemoryClaimStore(20*time.Millisecond, time.Minute)
	ctx := context.Background()

	first, err := store.Claim(ctx, "k7")
	if err != nil || first.Status != IdempotentClaimAcquired {
		t.Fatalf("first Claim = %+v, %v; want acquired nil", first, err)
	}
	time.Sleep(50 * time.Millisecond)
	second, err := store.Claim(ctx, "k7")
	if err != nil || second.Status != IdempotentClaimAcquired {
		t.Fatalf("second Claim = %+v, %v; want acquired nil", second, err)
	}
	if commitErr := store.Commit(ctx, "k7", first.Token); !errors.Is(commitErr, ErrIdempotentClaimLost) {
		t.Fatalf("stale Commit error = %v, want ErrIdempotentClaimLost", commitErr)
	}
	claim, err := store.Claim(ctx, "k7")
	if err != nil || claim.Status != IdempotentClaimInProgress {
		t.Fatalf("Claim after stale commit = %+v, %v; want in-progress nil", claim, err)
	}
}

func TestMemoryClaimStore_Concurrent(t *testing.T) {
	store := NewMemoryClaimStore(time.Minute, time.Minute)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := "k-" + strconv.Itoa(n%10)
			claim, _ := store.Claim(ctx, key)
			if claim.Status == IdempotentClaimAcquired {
				if n%2 == 0 {
					_ = store.Commit(ctx, key, claim.Token)
				} else {
					_ = store.Release(ctx, key, claim.Token)
				}
			}
			_, _ = store.Claim(ctx, key)
		}(i)
	}
	wg.Wait()
}
