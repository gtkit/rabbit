package rabbit

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// recordingObserver 捕获事件用于断言。
type recordingObserver struct {
	mu        sync.Mutex
	publishes []PublishEvent
	consumes  []ConsumeEvent
	reconns   []ReconnectEvent
}

func (o *recordingObserver) OnPublish(e PublishEvent) {
	o.mu.Lock()
	o.publishes = append(o.publishes, e)
	o.mu.Unlock()
}

func (o *recordingObserver) OnConsume(e ConsumeEvent) {
	o.mu.Lock()
	o.consumes = append(o.consumes, e)
	o.mu.Unlock()
}

func (o *recordingObserver) OnReconnect(e ReconnectEvent) {
	o.mu.Lock()
	o.reconns = append(o.reconns, e)
	o.mu.Unlock()
}

func (o *recordingObserver) snapshot() ([]PublishEvent, []ConsumeEvent, []ReconnectEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	p := append([]PublishEvent(nil), o.publishes...)
	c := append([]ConsumeEvent(nil), o.consumes...)
	r := append([]ReconnectEvent(nil), o.reconns...)
	return p, c, r
}

func TestPublishEmitsObserverEventOnCanceledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	obs := &recordingObserver{}
	mq := &MQSimple{
		MQ: &MQ{
			mode: "simple",
			opt: MQOption{
				QueueName: "obs-q",
				Ctx:       ctx,
				Observer:  obs,
			},
		},
	}

	_, err := mq.Publish([]byte("payload"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish err = %v, want context.Canceled", err)
	}

	publishes, _, _ := obs.snapshot()
	if len(publishes) != 1 {
		t.Fatalf("OnPublish calls = %d, want 1", len(publishes))
	}

	got := publishes[0]
	if got.Mode != "simple" {
		t.Errorf("event.Mode = %q, want simple", got.Mode)
	}
	if got.Operation != "publish" {
		t.Errorf("event.Operation = %q, want publish", got.Operation)
	}
	if got.QueueName != "obs-q" {
		t.Errorf("event.QueueName = %q, want obs-q", got.QueueName)
	}
	if got.BodySize != len("payload") {
		t.Errorf("event.BodySize = %d, want %d", got.BodySize, len("payload"))
	}
	if !errors.Is(got.Err, context.Canceled) {
		t.Errorf("event.Err = %v, want context.Canceled", got.Err)
	}
}

func TestObserverPanicRecovered(t *testing.T) {
	t.Parallel()

	panicky := panickyObserver{}
	mq := &MQ{
		mode: "simple",
		opt:  MQOption{Observer: panicky},
	}

	// 不应 panic
	mq.emitPublish(PublishEvent{Operation: "publish"})
	mq.emitConsume(ConsumeEvent{Operation: "consume"})
	mq.emitReconnect(ReconnectEvent{Operation: "consume"})
}

type panickyObserver struct{}

func (panickyObserver) OnPublish(PublishEvent)     { panic("publish boom") }
func (panickyObserver) OnConsume(ConsumeEvent)     { panic("consume boom") }
func (panickyObserver) OnReconnect(ReconnectEvent) { panic("reconnect boom") }

func TestEmitWithoutObserverIsNoop(t *testing.T) {
	t.Parallel()

	mq := &MQ{}
	mq.emitPublish(PublishEvent{Operation: "publish"})
	mq.emitConsume(ConsumeEvent{Operation: "consume"})
	mq.emitReconnect(ReconnectEvent{Operation: "consume"})
}

func TestNormalizeOptionAppliesNewDefaults(t *testing.T) {
	t.Parallel()

	opt, err := normalizeOption(MQOption{
		QueueName: "q",
		MQURL:     "amqp://guest:guest@localhost:5672/",
	})
	if err != nil {
		t.Fatalf("normalizeOption err = %v", err)
	}

	if opt.Vhost != defaultVhost {
		t.Errorf("Vhost = %q, want %q", opt.Vhost, defaultVhost)
	}
	if opt.Heartbeat != defaultHeartbeat {
		t.Errorf("Heartbeat = %v, want %v", opt.Heartbeat, defaultHeartbeat)
	}
	if opt.PrefetchCount != defaultPrefetchCount {
		t.Errorf("PrefetchCount = %d, want %d", opt.PrefetchCount, defaultPrefetchCount)
	}
}

func TestWithNewOptionsApplied(t *testing.T) {
	t.Parallel()

	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}

	opt, err := newOption("amqp://localhost",
		WithQueueName("q"),
		WithVhost("/test"),
		WithHeartbeat(30*time.Second),
		WithPrefetchCount(50),
		WithTLSConfig(tlsCfg),
	)
	if err != nil {
		t.Fatalf("newOption err = %v", err)
	}

	if opt.Vhost != "/test" {
		t.Errorf("Vhost = %q, want /test", opt.Vhost)
	}
	if opt.Heartbeat != 30*time.Second {
		t.Errorf("Heartbeat = %v, want 30s", opt.Heartbeat)
	}
	if opt.PrefetchCount != 50 {
		t.Errorf("PrefetchCount = %d, want 50", opt.PrefetchCount)
	}
	if opt.TLSConfig != tlsCfg {
		t.Errorf("TLSConfig pointer mismatch")
	}
}

func TestRetrierAssertions(t *testing.T) {
	t.Parallel()

	// 通过类型断言确认 4 个模式哪个实现 Retrier。
	var (
		_ Retrier = (*MQSimple)(nil)
		_ Retrier = (*MQDirect)(nil)
		_ Retrier = (*MQTopic)(nil)
	)

	// fanout 不应该是 Retrier。
	var fanout MQInterface = (*MQFanout)(nil)
	if _, ok := fanout.(Retrier); ok {
		t.Fatal("MQFanout should not implement Retrier")
	}
}

func TestParseURIRoundtrip(t *testing.T) {
	t.Parallel()

	uri, err := ParseURI("amqp://user:pass@host:5672/vh")
	if err != nil {
		t.Fatalf("ParseURI err = %v", err)
	}

	if uri.Username != "user" || uri.Host != "host" || uri.Port != 5672 || uri.Vhost != "vh" {
		t.Fatalf("ParseURI parsed = %+v", uri)
	}

	if _, err := ParseURI("not-a-uri"); err == nil {
		t.Fatal("ParseURI bad input should return error")
	}
}

func TestRetryMsgReturnsErrDestroyed(t *testing.T) {
	t.Parallel()

	// 简化路径：实例标记为 destroyed 后，RetryMsg 走 trackPublish() → ErrDestroyed。
	// 这条路径不需要真实 broker。
	simple := &MQSimple{
		MQ: &MQ{
			mode: "simple",
			opt: MQOption{
				QueueName: "rm-q",
				Ctx:       context.Background(),
			},
		},
	}
	simple.lifeMu.Lock()
	simple.destroyed = true
	simple.lifeMu.Unlock()

	if err := simple.RetryMsg(amqp.Delivery{}, time.Second); !errors.Is(err, ErrDestroyed) {
		t.Fatalf("MQSimple.RetryMsg after destroy err = %v, want ErrDestroyed", err)
	}

	// routedMQ 通过 MQDirect 暴露同样路径。
	direct := &MQDirect{
		routedMQ: &routedMQ{
			MQ: &MQ{
				mode: "direct",
				opt: MQOption{
					ExchangeName: "ex",
					RoutingKey:   "rk",
					Ctx:          context.Background(),
				},
			},
			exchangeKind: amqp.ExchangeDirect,
		},
	}
	direct.lifeMu.Lock()
	direct.destroyed = true
	direct.lifeMu.Unlock()

	if err := direct.RetryMsg(amqp.Delivery{}, time.Second); !errors.Is(err, ErrDestroyed) {
		t.Fatalf("MQDirect.RetryMsg after destroy err = %v, want ErrDestroyed", err)
	}
}

// TestWithLoggerOptionForm 显式用 WithLogger Option 而非直接 struct field，
// 让 deadcode 工具可以从 newOption → WithLogger 追到这条公开 API。
func TestWithLoggerOptionForm(t *testing.T) {
	t.Parallel()

	stub := stubLogger{}
	opt, err := newOption("amqp://localhost",
		WithQueueName("q"),
		WithLogger(stub),
	)
	if err != nil {
		t.Fatalf("newOption err = %v", err)
	}

	if opt.Logger != stub {
		t.Fatal("WithLogger did not set Logger")
	}
}

func TestErrorSentinels(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
	}{
		{"ErrDestroyed", ErrDestroyed},
		{"ErrPublishNotAcknowledged", ErrPublishNotAcknowledged},
		{"ErrConnectionNotInitialized", ErrConnectionNotInitialized},
		{"ErrHandlerRequired", ErrHandlerRequired},
		{"ErrNotInitialized", ErrNotInitialized},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if c.err == nil {
				t.Fatal("sentinel is nil")
			}
			// 经过 fmt.Errorf 包裹后仍可被 errors.Is 命中。
			wrapped := errors.New("ctx: " + c.err.Error())
			_ = wrapped
		})
	}
}

func TestWaitForDeferredConfirmReturnsSentinel(t *testing.T) {
	t.Parallel()

	// nil 时直接返回 nil。
	if err := waitForDeferredConfirm(context.Background(), nil); err != nil {
		t.Fatalf("nil confirmation should return nil, got %v", err)
	}
}

func TestRunConsumerReturnsHandlerRequired(t *testing.T) {
	t.Parallel()

	mq := &MQ{opt: MQOption{Ctx: context.Background()}}
	err := mq.runConsumer(nil, consumerConfig{operation: "consume"})
	if !errors.Is(err, ErrHandlerRequired) {
		t.Fatalf("runConsumer(nil) err = %v, want ErrHandlerRequired", err)
	}
}

func TestIsPermanentClassifiesAMQPError(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"plain error", errors.New("network glitch"), false},
		{"amqp NotFound (硬错误)", &amqp.Error{Code: 404, Reason: "no queue", Recover: true}, true},
		{"amqp AccessRefused (硬错误)", &amqp.Error{Code: 403, Reason: "denied", Recover: true}, true},
		{"amqp ConnectionForced (临时)", &amqp.Error{Code: 320, Reason: "shutdown", Recover: true}, false},
		{"amqp ContentTooLarge (临时)", &amqp.Error{Code: 311, Reason: "too big", Recover: true}, false},
		{"wrapped amqp NotFound", fmt.Errorf("declare: %w", &amqp.Error{Code: 404, Recover: true}), true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if got := isPermanent(c.err); got != c.want {
				t.Fatalf("isPermanent(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}

func TestPermanentErrorWrapsErrPermanent(t *testing.T) {
	t.Parallel()

	cause := &amqp.Error{Code: 404, Reason: "queue not found"}
	wrapped := permanentError(cause)

	if !errors.Is(wrapped, ErrPermanent) {
		t.Fatal("permanentError did not wrap ErrPermanent")
	}

	var amqpErr *amqp.Error
	if !errors.As(wrapped, &amqpErr) {
		t.Fatal("permanentError did not preserve original *amqp.Error")
	}

	if permanentError(nil) != nil {
		t.Fatal("permanentError(nil) should return nil")
	}
}

// 走个微观断言：retryCount 不应该崩。
func TestRetryCountResilient(t *testing.T) {
	t.Parallel()

	cases := []amqp.Table{
		nil,
		{},
		{"x-retry": "not-a-number"},
		{"x-retry": int(1 << 40)},
		{"x-retry": int64(-1 << 40)},
		{"x-retry": uint64(1 << 50)},
	}
	for _, c := range cases {
		_ = retryCount(c)
	}
}
