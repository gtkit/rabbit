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

	if err := store.Commit(ctx, "k1"); err != nil {
		t.Fatalf("Commit error: %v", err)
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
