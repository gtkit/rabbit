package rabbit

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// 这些 benchmark 测试库内部纯本地热路径，不依赖 broker。
// 运行：go test -bench=. -benchmem -run='^$' ./rabbit/...

func BenchmarkRetryCountInt32(b *testing.B) {
	headers := amqp.Table{"x-retry": int32(5)}
	for b.Loop() {
		_ = retryCount(headers)
	}
}

func BenchmarkRetryCountMissing(b *testing.B) {
	headers := amqp.Table{}
	for b.Loop() {
		_ = retryCount(headers)
	}
}

func BenchmarkCopyHeadersSmall(b *testing.B) {
	headers := amqp.Table{
		"x-retry":   int32(0),
		"x-trace":   "abc-def",
		"x-source":  "producer-a",
		"x-version": int32(1),
	}
	for b.Loop() {
		_ = copyHeaders(headers)
	}
}

func BenchmarkCopyHeadersEmpty(b *testing.B) {
	var headers amqp.Table
	for b.Loop() {
		_ = copyHeaders(headers)
	}
}

func BenchmarkTTLToString(b *testing.B) {
	d := 2500 * time.Millisecond
	for b.Loop() {
		_ = ttlToString(d)
	}
}

func BenchmarkRetryBackoffDelay(b *testing.B) {
	i := 0
	for b.Loop() {
		_ = retryBackoffDelay(i % 8)
		i++
	}
}

func BenchmarkSafeNamePart(b *testing.B) {
	const name = "order/created.queue:test"
	for b.Loop() {
		_ = safeNamePart(name)
	}
}

func BenchmarkAdaptLogger(b *testing.B) {
	external := &formattedOnlyLogger{}
	for b.Loop() {
		_, _ = adaptLogger(external)
	}
}

func BenchmarkLoggerErrorf(b *testing.B) {
	mq := &MQ{}
	for b.Loop() {
		_ = mq.logger()
	}
}

type noopHandler struct{}

func (noopHandler) Process([]byte, string) error { return nil }
func (noopHandler) Failed(FailedMsg)             {}

type errHandler struct{}

func (errHandler) Process([]byte, string) error { return errors.New("boom") }
func (errHandler) Failed(FailedMsg)             {}

func BenchmarkSafeProcessSuccess(b *testing.B) {
	mq := &MQ{}
	body := []byte("payload")
	for b.Loop() {
		_ = mq.safeProcess(noopHandler{}, body, "m1")
	}
}

func BenchmarkSafeProcessError(b *testing.B) {
	mq := &MQ{}
	body := []byte("payload")
	for b.Loop() {
		_ = mq.safeProcess(errHandler{}, body, "m1")
	}
}

func BenchmarkFailedMessage(b *testing.B) {
	mq := &MQ{
		opt: MQOption{
			ExchangeName: "ex",
			QueueName:    "q",
			RoutingKey:   "rk",
		},
	}
	body := []byte("payload")
	for b.Loop() {
		_ = mq.failedMessage(body, "msg-1")
	}
}

func BenchmarkContextOrBackground(b *testing.B) {
	mq := &MQ{
		opt: MQOption{Ctx: context.Background()},
	}
	for b.Loop() {
		_ = mq.contextOrBackground()
	}
}
