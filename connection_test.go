package rabbit

import "testing"

// blockAwareObserver 同时实现 Observer 与 BlockObserver。
type blockAwareObserver struct {
	blocked []BlockedEvent
}

func (o *blockAwareObserver) OnPublish(PublishEvent)     {}
func (o *blockAwareObserver) OnConsume(ConsumeEvent)     {}
func (o *blockAwareObserver) OnReconnect(ReconnectEvent) {}
func (o *blockAwareObserver) OnBlocked(e BlockedEvent)   { o.blocked = append(o.blocked, e) }

// plainObserver 只实现旧的 Observer 接口，不实现 BlockObserver。
type plainObserver struct {
	publishes int
}

func (o *plainObserver) OnPublish(PublishEvent)     { o.publishes++ }
func (o *plainObserver) OnConsume(ConsumeEvent)     {}
func (o *plainObserver) OnReconnect(ReconnectEvent) {}

func TestEmitBlockedDispatchesToBlockObserver(t *testing.T) {
	t.Parallel()

	rec := &blockAwareObserver{}
	m := &MQ{opt: MQOption{Observer: rec}, mode: "simple"}

	m.emitBlocked(BlockedEvent{Blocked: true, Reason: "low on memory"})
	m.emitBlocked(BlockedEvent{Blocked: false})

	if len(rec.blocked) != 2 {
		t.Fatalf("OnBlocked calls = %d, want 2", len(rec.blocked))
	}
	if !rec.blocked[0].Blocked || rec.blocked[0].Reason != "low on memory" {
		t.Fatalf("first event = %+v, want blocked with reason", rec.blocked[0])
	}
	if rec.blocked[0].Mode != "simple" {
		t.Fatalf("event Mode = %q, want simple", rec.blocked[0].Mode)
	}
	if rec.blocked[1].Blocked {
		t.Fatalf("second event should be unblocked, got %+v", rec.blocked[1])
	}
}

func TestEmitBlockedIgnoresPlainObserver(t *testing.T) {
	t.Parallel()

	obs := &plainObserver{}
	m := &MQ{opt: MQOption{Observer: obs}, mode: "simple"}

	// 旧 Observer 未实现 BlockObserver：emitBlocked 应为 no-op，不 panic。
	m.emitBlocked(BlockedEvent{Blocked: true})

	if obs.publishes != 0 {
		t.Fatalf("plain observer should not be affected, publishes = %d", obs.publishes)
	}
}

func TestEmitBlockedNilObserverIsNoop(t *testing.T) {
	t.Parallel()

	m := &MQ{opt: MQOption{}, mode: "simple"}
	m.emitBlocked(BlockedEvent{Blocked: true}) // 不 panic 即通过
}
