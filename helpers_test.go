package rabbit

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
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

// --------------------------------------------------------------------------
// failedMessage
// --------------------------------------------------------------------------

func TestFailedMessageDeepCopiesBody(t *testing.T) {
	t.Parallel()

	mq := &MQ{opt: MQOption{ExchangeName: "ex", QueueName: "q", RoutingKey: "rk"}}
	original := []byte("hello")
	fm := mq.failedMessage(original, "mid-1")

	if string(fm.Message) != "hello" {
		t.Fatalf("failedMessage body = %q, want hello", string(fm.Message))
	}
	if fm.ExchangeName != "ex" || fm.QueueName != "q" || fm.RoutingKey != "rk" || fm.MessageID != "mid-1" {
		t.Fatal("failedMessage fields mismatch")
	}

	original[0] = 'X'
	if string(fm.Message) != "hello" {
		t.Fatal("failedMessage did not deep-copy body")
	}
}

func TestFailedMessageNilMQ(t *testing.T) {
	t.Parallel()

	var m *MQ
	fm := m.failedMessage([]byte("data"), "mid-nil")
	if string(fm.Message) != "data" || fm.MessageID != "mid-nil" {
		t.Fatal("failedMessage nil mq should still populate message and msgID")
	}
}

// --------------------------------------------------------------------------
// contextOrBackground
// --------------------------------------------------------------------------

func TestContextOrBackgroundNilMQIsStable(t *testing.T) {
	t.Parallel()

	var m *MQ
	ctx := m.contextOrBackground()
	select {
	case <-ctx.Done():
		t.Fatal("nil mq ctx should not be done")
	default:
	}
}

func TestContextOrBackgroundNilCtx(t *testing.T) {
	t.Parallel()

	m := &MQ{}
	ctx := m.contextOrBackground()
	select {
	case <-ctx.Done():
		t.Fatal("nil ctx should return background")
	default:
	}
}

func TestContextOrBackgroundReturnsAssignedContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	m := &MQ{opt: MQOption{Ctx: ctx}}
	<-m.contextOrBackground().Done()
}

// --------------------------------------------------------------------------
// canceledError
// --------------------------------------------------------------------------

func TestCanceledErrorWrapsContextCanceled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	m := &MQ{opt: MQOption{Ctx: ctx}}
	err := m.canceledError("publish")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceledError = %v, want context.Canceled", err)
	}
}

func TestCanceledErrorActiveContext(t *testing.T) {
	t.Parallel()

	m := &MQ{opt: MQOption{Ctx: context.Background()}}
	if err := m.canceledError("publish"); err != nil {
		t.Fatalf("active context should return nil, got %v", err)
	}
}

// --------------------------------------------------------------------------
// trackPublish
// --------------------------------------------------------------------------

func TestTrackPublishDestroyedReturnsFalse(t *testing.T) {
	t.Parallel()

	m := &MQ{}
	m.lifeMu.Lock()
	m.destroyed = true
	m.lifeMu.Unlock()

	_, ok := m.trackPublish()
	if ok {
		t.Fatal("destroyed trackPublish should be false")
	}
}

func TestTrackPublishNormal(t *testing.T) {
	t.Parallel()

	m := &MQ{}
	done, ok := m.trackPublish()
	if !ok {
		t.Fatal("normal trackPublish should return ok")
	}
	done()
}

// --------------------------------------------------------------------------
// logger
// --------------------------------------------------------------------------

func TestLoggerFallbackUsesInstanceWhenSet(t *testing.T) {
	t.Parallel()

	external := &formattedOnlyLogger{}
	_, _ = adaptLogger(external)
	m := &MQ{opt: MQOption{}}

	// instance logger not set - use global (which is stubLogger from init)
	_ = m.logger()
}

// --------------------------------------------------------------------------
// IsReady
// --------------------------------------------------------------------------

func TestIsReadyNilMQReturnsFalse(t *testing.T) {
	t.Parallel()

	var m *MQ
	if m.IsReady() {
		t.Fatal("nil MQ IsReady should be false")
	}
}

func TestIsReadyNoConnectionReturnsFalse(t *testing.T) {
	t.Parallel()

	m := &MQ{}
	if m.IsReady() {
		t.Fatal("no conn IsReady should be false")
	}
}

// --------------------------------------------------------------------------
// WithQueueArgs / WithExchangeArg
// --------------------------------------------------------------------------

func TestWithQueueArgsApplied(t *testing.T) {
	t.Parallel()

	opt, err := newOption("amqp://localhost",
		WithQueueName("q"),
		WithQueueArgs(map[string]any{"x-message-ttl": int64(60000), "x-max-length": int64(1000)}),
	)
	if err != nil {
		t.Fatalf("newOption err = %v", err)
	}
	if opt.QueueArgs["x-message-ttl"] != int64(60000) {
		t.Fatalf("QueueArgs x-message-ttl = %v", opt.QueueArgs["x-message-ttl"])
	}
	if opt.QueueArgs["x-max-length"] != int64(1000) {
		t.Fatalf("QueueArgs x-max-length = %v", opt.QueueArgs["x-max-length"])
	}
}

func TestWithExchangeArgApplied(t *testing.T) {
	t.Parallel()

	opt, err := newOption("amqp://localhost",
		WithQueueName("q"),
		WithExchangeArg("alternate-exchange", "alt.exchange"),
	)
	if err != nil {
		t.Fatalf("newOption err = %v", err)
	}
	if opt.ExchangeArgs["alternate-exchange"] != "alt.exchange" {
		t.Fatalf("ExchangeArgs alternate-exchange = %v", opt.ExchangeArgs["alternate-exchange"])
	}
}

// --------------------------------------------------------------------------
// MustNew* panic
// --------------------------------------------------------------------------

func TestMustNewSimplePanicsOnMissingQueue(t *testing.T) {
	t.Parallel()

	defer func() {
		if rec := recover(); rec == nil {
			t.Fatal("MustNewSimple with empty queue should panic")
		}
	}()
	MustNewSimple("", MQOption{MQURL: "amqp://localhost"})
}

func TestMustNewDirectPanicsOnMissingArgs(t *testing.T) {
	t.Parallel()

	defer func() {
		if rec := recover(); rec == nil {
			t.Fatal("MustNewDirect with empty exchange should panic")
		}
	}()
	MustNewDirect("", "rk", MQOption{MQURL: "amqp://localhost"})
}

func TestMustNewFanoutPanicsOnMissingExchange(t *testing.T) {
	t.Parallel()

	defer func() {
		if rec := recover(); rec == nil {
			t.Fatal("MustNewFanout with empty exchange should panic")
		}
	}()
	MustNewFanout("", MQOption{MQURL: "amqp://localhost"})
}

func TestMustNewTopicPanicsOnMissingArgs(t *testing.T) {
	t.Parallel()

	defer func() {
		if rec := recover(); rec == nil {
			t.Fatal("MustNewTopic with empty exchange should panic")
		}
	}()
	MustNewTopic("", "", MQOption{MQURL: "amqp://localhost"})
}

func TestMustNewHeadersPanicsOnMissingExchange(t *testing.T) {
	t.Parallel()

	defer func() {
		if rec := recover(); rec == nil {
			t.Fatal("MustNewHeaders with empty exchange should panic")
		}
	}()
	MustNewHeaders("", HeaderBinding{}, MQOption{MQURL: "amqp://localhost"})
}

// --------------------------------------------------------------------------
// Constructor validation errors
// --------------------------------------------------------------------------

func TestNewSimpleMissingQueueReturnsError(t *testing.T) {
	t.Parallel()

	_, err := NewSimple("", MQOption{MQURL: "amqp://localhost"})
	if err == nil {
		t.Fatal("NewSimple empty queue should return error")
	}
}

func TestNewDirectMissingExchangeReturnsError(t *testing.T) {
	t.Parallel()

	_, err := NewDirect("", "", MQOption{MQURL: "amqp://localhost"})
	if err == nil {
		t.Fatal("NewDirect empty exchange should return error")
	}
}

func TestNewFanoutMissingExchangeReturnsError(t *testing.T) {
	t.Parallel()

	_, err := NewFanout("", MQOption{MQURL: "amqp://localhost"})
	if err == nil {
		t.Fatal("NewFanout empty exchange should return error")
	}
}

func TestNewHeadersMissingExchangeReturnsError(t *testing.T) {
	t.Parallel()

	_, err := NewHeaders("", HeaderBinding{}, MQOption{MQURL: "amqp://localhost"})
	if err == nil {
		t.Fatal("NewHeaders empty exchange should return error")
	}
}

func TestTotalSizeBytesNilInput(t *testing.T) {
	t.Parallel()

	if got := totalSizeBytes(nil); got != 0 {
		t.Fatalf("totalSizeBytes(nil) = %d", got)
	}
}

func TestTotalSizeBytesMultiple(t *testing.T) {
	t.Parallel()

	if got := totalSizeBytes([][]byte{{1, 2, 3}, {4, 5}}); got != 5 {
		t.Fatalf("totalSizeBytes = %d, want 5", got)
	}
}

func TestResolveDeliveryModeNil(t *testing.T) {
	t.Parallel()

	var m *MQ
	if got := m.resolveDeliveryMode(); got != 2 {
		t.Fatalf("nil resolveDeliveryMode = %d, want 2 (persistent)", got)
	}
}

func TestResolveDeliveryModeCustom(t *testing.T) {
	t.Parallel()

	m := &MQ{opt: MQOption{DeliveryMode: 1}}
	if got := m.resolveDeliveryMode(); got != 1 {
		t.Fatalf("custom resolveDeliveryMode = %d, want 1 (transient)", got)
	}
}

// --------------------------------------------------------------------------
// buildPublishing
// --------------------------------------------------------------------------

func TestBuildPublishingDefaults(t *testing.T) {
	t.Parallel()

	req := publishRequest{body: []byte("hello")}
	pub := req.buildPublishing("msg-1")

	if pub.MessageId != "msg-1" {
		t.Fatalf("MessageId = %q, want msg-1", pub.MessageId)
	}
	if string(pub.Body) != "hello" {
		t.Fatalf("Body = %q, want hello", string(pub.Body))
	}
	if pub.ContentType != "application/octet-stream" {
		t.Fatalf("ContentType = %q, want application/octet-stream", pub.ContentType)
	}
	if pub.DeliveryMode != amqp.Persistent {
		t.Fatalf("DeliveryMode = %d, want %d", pub.DeliveryMode, amqp.Persistent)
	}
}

func TestBuildPublishingCustomFields(t *testing.T) {
	t.Parallel()

	req := publishRequest{
		body:         []byte("data"),
		contentType:  "application/json",
		deliveryMode: amqp.Transient,
		priority:     5,
		expiration:   "5000",
		headers:      amqp.Table{"x-retry": int32(0)},
		timestamp:    time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC),
	}
	pub := req.buildPublishing("msg-2")

	if pub.ContentType != "application/json" {
		t.Fatalf("ContentType = %q", pub.ContentType)
	}
	if pub.DeliveryMode != amqp.Transient {
		t.Fatalf("DeliveryMode = %d", pub.DeliveryMode)
	}
	if pub.Priority != 5 {
		t.Fatalf("Priority = %d", pub.Priority)
	}
	if pub.Expiration != "5000" {
		t.Fatalf("Expiration = %q", pub.Expiration)
	}
	if pub.Headers["x-retry"] != int32(0) {
		t.Fatal("Headers mismatch")
	}
	if pub.Timestamp.IsZero() {
		t.Fatal("Timestamp should be set")
	}
}

// --------------------------------------------------------------------------
// bindingArgs (headers.go)
// --------------------------------------------------------------------------

func TestHeaderBindingArgs(t *testing.T) {
	t.Parallel()

	t.Run("match all", func(t *testing.T) {
		b := HeaderBinding{
			MatchAll: true,
			Headers:  map[string]any{"key1": "val1", "key2": "val2"},
		}
		args := b.bindingArgs()
		if args["x-match"] != "all" {
			t.Fatalf("x-match = %v, want all", args["x-match"])
		}
		if args["key1"] != "val1" || args["key2"] != "val2" {
			t.Fatal("header binding args missing keys")
		}
	})

	t.Run("match any", func(t *testing.T) {
		b := HeaderBinding{
			MatchAll: false,
			Headers:  map[string]any{"format": "json"},
		}
		args := b.bindingArgs()
		if args["x-match"] != "any" {
			t.Fatalf("x-match = %v, want any", args["x-match"])
		}
		if args["format"] != "json" {
			t.Fatal("header binding args missing format")
		}
	})

	t.Run("empty headers", func(t *testing.T) {
		b := HeaderBinding{MatchAll: true}
		args := b.bindingArgs()
		if args["x-match"] != "all" {
			t.Fatalf("x-match = %v", args["x-match"])
		}
	})
}

// --------------------------------------------------------------------------
// retryCount additional types
// --------------------------------------------------------------------------

func TestRetryCountAdditionalTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		headers amqp.Table
		want    int32
	}{
		{name: "nil headers", headers: nil, want: 0},
		{name: "int8", headers: amqp.Table{"x-retry": int8(3)}, want: 3},
		{name: "int16", headers: amqp.Table{"x-retry": int16(7)}, want: 7},
		{name: "uint16", headers: amqp.Table{"x-retry": uint16(9)}, want: 9},
		{name: "uint32", headers: amqp.Table{"x-retry": uint32(11)}, want: 11},
		{name: "uint64", headers: amqp.Table{"x-retry": uint64(1 << 50)}, want: math.MaxInt32},
		{name: "overflow int64", headers: amqp.Table{"x-retry": int64(math.MaxInt32) + 1}, want: math.MaxInt32},
		{name: "string (not numeric)", headers: amqp.Table{"x-retry": "hello"}, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := retryCount(tt.headers); got != tt.want {
				t.Fatalf("retryCount = %d, want %d", got, tt.want)
			}
		})
	}
}

// --------------------------------------------------------------------------
// normalizeOption edge cases
// --------------------------------------------------------------------------

func TestNormalizeOptionEmptyMQURL(t *testing.T) {
	t.Parallel()

	_, err := normalizeOption(MQOption{QueueName: "q"})
	if err == nil {
		t.Fatal("empty MQURL should return error")
	}
}

func TestNormalizeOptionConnName(t *testing.T) {
	t.Parallel()

	opt, err := normalizeOption(MQOption{
		QueueName: "q",
		MQURL:     "amqp://localhost",
		ConnName:  "my-conn",
	})
	if err != nil {
		t.Fatalf("normalizeOption err = %v", err)
	}
	if opt.ConnName != "my-conn" {
		t.Fatalf("ConnName = %q, want my-conn", opt.ConnName)
	}
}

func TestNormalizeOptionHeartbeat(t *testing.T) {
	t.Parallel()

	opt, err := normalizeOption(MQOption{
		QueueName: "q",
		MQURL:     "amqp://localhost",
		Heartbeat: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("normalizeOption err = %v", err)
	}
	if opt.Heartbeat != 5*time.Second {
		t.Fatalf("Heartbeat = %v, want 5s", opt.Heartbeat)
	}
}

func TestNormalizeOptionNegativeHeartbeatDefaults(t *testing.T) {
	t.Parallel()

	opt, err := normalizeOption(MQOption{
		QueueName: "q",
		MQURL:     "amqp://localhost",
		Heartbeat: -1,
	})
	if err != nil {
		t.Fatalf("normalizeOption err = %v", err)
	}
	if opt.Heartbeat != defaultHeartbeat {
		t.Fatalf("negative Heartbeat = %v, want %v", opt.Heartbeat, defaultHeartbeat)
	}
}

func TestNormalizeOptionWhitespaceTrimmed(t *testing.T) {
	t.Parallel()

	opt, err := normalizeOption(MQOption{
		QueueName: "  q  ",
		MQURL:     " amqp://localhost ",
		Vhost:     " / ",
	})
	if err != nil {
		t.Fatalf("normalizeOption err = %v", err)
	}
	if opt.QueueName != "q" {
		t.Fatalf("QueueName trimmed to %q", opt.QueueName)
	}
	if opt.MQURL != "amqp://localhost" {
		t.Fatalf("MQURL trimmed to %q", opt.MQURL)
	}
	if opt.Vhost != "/" {
		t.Fatalf("Vhost trimmed to %q", opt.Vhost)
	}
}

// --------------------------------------------------------------------------
// adaptLogger edge cases
// --------------------------------------------------------------------------

func TestAdaptLoggerNilReturnsFalse(t *testing.T) {
	t.Parallel()

	_, ok := adaptLogger(nil)
	if ok {
		t.Fatal("adaptLogger(nil) should be false")
	}
}

func TestAdaptLoggerLoggerInference(t *testing.T) {
	t.Parallel()

	// adaptedLogger with infoLogger interface
	type bothImpl struct{ formattedOnlyLogger }
	_, ok := adaptLogger(&bothImpl{})
	if !ok {
		t.Fatal("adaptLogger bothImpl should succeed")
	}
}

// --------------------------------------------------------------------------
// isPermanent / permanentError edge cases
// --------------------------------------------------------------------------

func TestIsPermanentNonAMQPError(t *testing.T) {
	t.Parallel()

	// network errors are temporary
	netErr := &netOpError{op: "dial", msg: "connection refused"}
	if isPermanent(netErr) {
		t.Fatal("network error should not be permanent")
	}
}

type netOpError struct {
	op  string
	msg string
}

func (e *netOpError) Error() string   { return e.op + ": " + e.msg }
func (e *netOpError) Temporary() bool { return true }
func (e *netOpError) Timeout() bool   { return false }

func TestPermanentErrorNilReturnsNil(t *testing.T) {
	t.Parallel()

	if permanentError(nil) != nil {
		t.Fatal("permanentError(nil) should return nil")
	}
}

// --------------------------------------------------------------------------
// maybeLogTLSWarning
// --------------------------------------------------------------------------

func TestMaybeLogTLSWarningAmqps(t *testing.T) {
	// note: not parallel due to SetLogger global state
	original := currentLogger()
	defer SetLogger(original)

	captured := &formattedOnlyLogger{}
	SetLogger(captured)

	maybeLogTLSWarning("amqps://host:5671/")
	if captured.infofCalls != 0 {
		t.Fatal("amqps:// should not trigger TLS warning")
	}
}

func TestMaybeLogTLSWarningAmqp(t *testing.T) {
	// note: not parallel due to SetLogger global state
	original := currentLogger()
	defer SetLogger(original)

	captured := &formattedOnlyLogger{}
	SetLogger(captured)

	maybeLogTLSWarning("amqp://host:5672/")
	if captured.infofCalls != 1 {
		t.Fatal("amqp:// with TLS should trigger warning")
	}
}
