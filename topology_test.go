package rabbit

import (
	"reflect"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

// newTestMQ 用归一化后的 option 构造一个仅用于参数计算的 MQ（不连接 broker）。
func newTestMQ(t *testing.T, opts ...Option) *MQ {
	t.Helper()
	opt, err := newOption("amqp://guest:guest@localhost:5672/", opts...)
	if err != nil {
		t.Fatalf("newOption() error = %v", err)
	}
	return &MQ{opt: opt}
}

func TestMainQueueArgsClassicDefaultMatchesLegacy(t *testing.T) {
	t.Parallel()

	m := newTestMQ(t)

	// 主队列无额外参数：与 v1.1.0 一致，仅含 x-max-priority: 10(int)。
	got := m.mainQueueArgs(nil)
	want := amqp.Table{"x-max-priority": 10}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mainQueueArgs(nil) = %#v, want %#v", got, want)
	}

	// 带 DLX 额外参数：与 v1.1.0 死信主队列声明一致。
	gotDLX := m.mainQueueArgs(amqp.Table{"x-dead-letter-exchange": "dlx-jobs"})
	wantDLX := amqp.Table{
		"x-max-priority":         10,
		"x-dead-letter-exchange": "dlx-jobs",
	}
	if !reflect.DeepEqual(gotDLX, wantDLX) {
		t.Fatalf("mainQueueArgs(DLX) = %#v, want %#v", gotDLX, wantDLX)
	}
}

func TestMainQueueArgsClassicMergesUserQueueArgs(t *testing.T) {
	t.Parallel()

	m := newTestMQ(t, WithQueueArgs(map[string]any{"x-message-ttl": 60000}))

	got := m.mainQueueArgs(nil)
	want := amqp.Table{
		"x-message-ttl":  60000,
		"x-max-priority": 10,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mainQueueArgs() = %#v, want %#v", got, want)
	}
}

func TestMainQueueArgsPriorityDisabled(t *testing.T) {
	t.Parallel()

	m := newTestMQ(t, WithPriority(0))

	got := m.mainQueueArgs(nil)
	if _, ok := got["x-max-priority"]; ok {
		t.Fatalf("mainQueueArgs() with WithPriority(0) should not contain x-max-priority, got %#v", got)
	}
}

func TestMainQueueArgsQuorum(t *testing.T) {
	t.Parallel()

	// 即便显式 WithPriority(10)，quorum 也绝不写 x-max-priority。
	m := newTestMQ(t, WithQueueType(QueueTypeQuorum), WithPriority(10))

	got := m.mainQueueArgs(nil)
	if got["x-queue-type"] != "quorum" {
		t.Fatalf("mainQueueArgs() x-queue-type = %v, want quorum", got["x-queue-type"])
	}
	if _, ok := got["x-max-priority"]; ok {
		t.Fatalf("quorum queue must not contain x-max-priority, got %#v", got)
	}
}

func TestMainQueueArgsQuorumDeliveryLimit(t *testing.T) {
	t.Parallel()

	m := newTestMQ(t, WithQueueType(QueueTypeQuorum), WithDeliveryLimit(5))

	got := m.mainQueueArgs(nil)
	want := amqp.Table{
		"x-queue-type":     "quorum",
		"x-delivery-limit": 5,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mainQueueArgs() = %#v, want %#v", got, want)
	}
}

func TestMainQueueArgsStream(t *testing.T) {
	t.Parallel()

	m := newTestMQ(t, WithQueueType(QueueTypeStream))

	got := m.mainQueueArgs(nil)
	if got["x-queue-type"] != "stream" {
		t.Fatalf("mainQueueArgs() x-queue-type = %v, want stream", got["x-queue-type"])
	}
	if _, ok := got["x-max-priority"]; ok {
		t.Fatalf("stream queue must not contain x-max-priority, got %#v", got)
	}
}

func TestDerivedQueueArgsClassicDefault(t *testing.T) {
	t.Parallel()

	m := newTestMQ(t)

	// retry/delay 派生队列默认携带 x-max-priority: 10(int)，与 v1.1.0 一致。
	got := m.derivedQueueArgs(amqp.Table{
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": "jobs",
	})
	want := amqp.Table{
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": "jobs",
		"x-max-priority":            10,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("derivedQueueArgs() = %#v, want %#v", got, want)
	}
}

func TestDerivedQueueArgsAlwaysClassicForQuorumMain(t *testing.T) {
	t.Parallel()

	// 主队列 quorum 时，派生队列仍为 classic，不含 x-queue-type。
	m := newTestMQ(t, WithQueueType(QueueTypeQuorum))

	got := m.derivedQueueArgs(amqp.Table{"x-dead-letter-exchange": "ex"})
	if _, ok := got["x-queue-type"]; ok {
		t.Fatalf("derived queue must stay classic, got %#v", got)
	}
}

func TestDerivedQueueArgsPriorityDisabled(t *testing.T) {
	t.Parallel()

	m := newTestMQ(t, WithPriority(0))

	got := m.derivedQueueArgs(amqp.Table{"x-dead-letter-exchange": "ex"})
	if _, ok := got["x-max-priority"]; ok {
		t.Fatalf("WithPriority(0) derived queue must not contain x-max-priority, got %#v", got)
	}
}
