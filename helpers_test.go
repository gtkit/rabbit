package rabbit

import (
	"context"
	"errors"
	"testing"
)

type stubLogger struct{}

func (stubLogger) Info(...any)           {}
func (stubLogger) Infof(string, ...any)  {}
func (stubLogger) Errorf(string, ...any) {}

type formattedOnlyLogger struct {
	infofCalls  int
	errorfCalls int
}

func (l *formattedOnlyLogger) Infof(string, ...any) {
	l.infofCalls++
}

func (l *formattedOnlyLogger) Errorf(string, ...any) {
	l.errorfCalls++
}

func TestDestroyIsIdempotent(t *testing.T) {
	t.Parallel()

	var mq MQ

	mq.Destroy()
	mq.Destroy()
}

func TestDestroyCancelsOwnedContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	mq := &MQ{
		opt: MQOption{
			Ctx: ctx,
		},
		cancel: cancel,
	}

	mq.Destroy()

	select {
	case <-ctx.Done():
	default:
		t.Fatal("Destroy() did not cancel the owned context")
	}
}

func TestPublishOnCanceledContextWrapsContextError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var captured []FailedMsg
	mq := &MQSimple{
		MQ: &MQ{
			opt: MQOption{
				QueueName:     "jobs",
				Ctx:           ctx,
				PubFailNotify: func(msg FailedMsg) { captured = append(captured, msg) },
			},
		},
	}

	msgID, err := mq.Publish([]byte("payload"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish() error = %v, want wrapped context cancellation", err)
	}

	if msgID != "" {
		t.Fatalf("Publish() msgID = %q, want empty on ctx cancel", msgID)
	}

	if len(captured) != 1 {
		t.Fatalf("PubFailNotify calls = %d, want 1", len(captured))
	}

	if string(captured[0].Message) != "payload" {
		t.Fatalf("PubFailNotify message = %q, want %q", string(captured[0].Message), "payload")
	}
}

func TestPublishOnCanceledContextWithoutNotifyDoesNotPanic(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	mq := &MQSimple{
		MQ: &MQ{
			opt: MQOption{
				QueueName: "jobs",
				Ctx:       ctx,
			},
		},
	}

	_, err := mq.Publish([]byte("payload"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish() error = %v, want wrapped context cancellation", err)
	}
}

func TestPublishAfterDestroyReturnsErrDestroyed(t *testing.T) {
	t.Parallel()

	mq := &MQSimple{
		MQ: &MQ{
			opt: MQOption{
				QueueName: "jobs",
				Ctx:       context.Background(),
			},
		},
	}

	// 模拟已 destroyed 状态（无需真实 broker）。
	mq.lifeMu.Lock()
	mq.destroyed = true
	mq.lifeMu.Unlock()

	_, err := mq.Publish([]byte("after destroy"))
	if !errors.Is(err, ErrDestroyed) {
		t.Fatalf("Publish() error = %v, want ErrDestroyed", err)
	}
}

func TestSetLoggerReplacesGlobalLogger(t *testing.T) {
	original := currentLogger()
	if !SetLogger(stubLogger{}) {
		t.Fatal("SetLogger() = false, want true")
	}
	t.Cleanup(func() {
		SetLogger(original)
	})

	if _, ok := currentLogger().(stubLogger); !ok {
		t.Fatal("SetLogger() did not replace the global logger")
	}
}

func TestSetLoggerAdaptsFormattedLogger(t *testing.T) {
	original := currentLogger()
	external := &formattedOnlyLogger{}

	if !SetLogger(external) {
		t.Fatal("SetLogger() = false, want true")
	}

	t.Cleanup(func() {
		SetLogger(original)
	})

	currentLogger().Info("payload")
	currentLogger().Errorf("err: %s", "boom")

	if external.infofCalls != 1 {
		t.Fatalf("Infof() calls = %d, want 1", external.infofCalls)
	}

	if external.errorfCalls != 1 {
		t.Fatalf("Errorf() calls = %d, want 1", external.errorfCalls)
	}
}

func TestSetLoggerRejectsUnsupportedType(t *testing.T) {
	t.Parallel()

	if SetLogger(struct{}{}) {
		t.Fatal("SetLogger() = true, want false")
	}
}

func TestSetLoggerRejectsNil(t *testing.T) {
	t.Parallel()

	if SetLogger(nil) {
		t.Fatal("SetLogger(nil) = true, want false")
	}
}

func TestWithLoggerOverridesGlobal(t *testing.T) {
	t.Parallel()

	external := &formattedOnlyLogger{}
	logger, ok := adaptLogger(external)
	if !ok {
		t.Fatal("adaptLogger() ok = false")
	}

	mq := &MQ{
		opt: MQOption{
			Logger: logger,
		},
	}

	mq.logger().Errorf("boom")

	if external.errorfCalls != 1 {
		t.Fatalf("instance logger Errorf calls = %d, want 1", external.errorfCalls)
	}
}

func TestSafeProcessRecoversPanic(t *testing.T) {
	t.Parallel()

	mq := &MQ{}

	err := mq.safeProcess(panickyHandler{}, []byte("p"), "m1")
	if err == nil {
		t.Fatal("safeProcess() error = nil, want non-nil")
	}

	if got := err.Error(); got == "" {
		t.Fatal("safeProcess() returned empty error message")
	}
}

type panickyHandler struct{}

func (panickyHandler) Process([]byte, string) error {
	panic("boom")
}

func (panickyHandler) Failed(FailedMsg) {}

type panickyFailedHandler struct{}

func (panickyFailedHandler) Process([]byte, string) error { return nil }

func (panickyFailedHandler) Failed(FailedMsg) {
	panic("failed callback boom")
}

func TestNotifyConsumeFailedRecoversPanic(t *testing.T) {
	t.Parallel()

	mq := &MQ{}

	// 不应 panic 即认为通过
	mq.notifyConsumeFailed(panickyFailedHandler{}, FailedMsg{MessageID: "m1"})
}

func TestNotifyPubFailedRecoversPanic(t *testing.T) {
	t.Parallel()

	mq := &MQ{
		opt: MQOption{
			PubFailNotify: func(FailedMsg) {
				panic("pub fail boom")
			},
		},
	}

	// 不应 panic 即认为通过
	mq.notifyPubFailed(FailedMsg{MessageID: "m1"})
}
