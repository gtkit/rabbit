// v2_e2e_test.go: v1.2.0 新增能力的端到端集成测试，依赖真实 RabbitMQ broker。
//
//	MQ_INTEGRATION=1 go test ./test -run V2 -count=1 -timeout=5m -v
package test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gtkit/rabbit"
)

// queueType 通过 Management API 读取队列的 type（classic/quorum/stream）。
func queueType(t *testing.T, name string) string {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet,
		fmt.Sprintf("%s/api/queues/%%2F/%s", mgmtURL, url.PathEscape(name)), nil)
	req.SetBasicAuth(mgmtUser, mgmtPass)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("query queue %q: %v", name, err)
	}
	defer resp.Body.Close()

	var info struct {
		Type string `json:"type"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		t.Fatalf("decode queue %q: %v", name, err)
	}
	return info.Type
}

// countAllConnections 返回 broker 上当前连接总数。
func countAllConnections(t *testing.T) int {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, mgmtURL+"/api/connections", nil)
	req.SetBasicAuth(mgmtUser, mgmtPass)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("list connections: %v", err)
	}
	defer resp.Body.Close()

	var conns []json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&conns); err != nil {
		t.Fatalf("decode connections: %v", err)
	}
	return len(conns)
}

// stableConnCount 轮询连接总数，直到连续两次读数相同（说明此刻无连接正在建立/关闭），
// 返回稳定基线，避免上一个测试的连接尚未完全关闭造成干扰。
func stableConnCount(t *testing.T) int {
	t.Helper()
	prev := -1
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		cur := countAllConnections(t)
		if cur == prev {
			return cur
		}
		prev = cur
		time.Sleep(300 * time.Millisecond)
	}
	return prev
}

// failCapture 收集 PubFailNotify 回调。
type failCapture struct {
	mu   sync.Mutex
	msgs []rabbit.FailedMsg
}

func (c *failCapture) notify(m rabbit.FailedMsg) {
	c.mu.Lock()
	c.msgs = append(c.msgs, m)
	c.mu.Unlock()
}

func (c *failCapture) ids() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.msgs))
	for i, m := range c.msgs {
		out[i] = m.MessageID
	}
	return out
}

// ----------------------------------------------------------------------------
// P0-1: mandatory 不可路由消息必须被感知为失败
// ----------------------------------------------------------------------------

func TestV2UnroutablePublishReturnsError(t *testing.T) {
	requireRabbitMQ(t)

	exchange := uniqueName(t, "ex-unroutable")
	queue := uniqueName(t, "q-bound")
	t.Cleanup(func() {
		deleteQueue(t, queue)
		deleteExchange(t, exchange)
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// 消费端绑定 routing key "rk.bound"，声明 exchange + 队列。
	cons, err := rabbit.NewConsumeDirect(exchange, "rk.bound", mqURL,
		rabbit.WithContext(ctx), rabbit.WithQueueName(queue))
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	t.Cleanup(cons.Destroy)
	go func() { _ = cons.Consume(newRecorder(1)) }()
	waitQueueReady(t, queue)

	// 发布端用一个没有任何队列绑定的 routing key。
	fc := &failCapture{}
	pub, err := rabbit.NewPubDirect(exchange, "rk.unbound", mqURL,
		rabbit.WithContext(ctx), rabbit.WithPubFailNotify(fc.notify))
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	t.Cleanup(pub.Destroy)

	msgID, err := pub.Publish([]byte("nobody-listens"))
	if !errors.Is(err, rabbit.ErrPublishReturned) {
		t.Fatalf("Publish() error = %v, want ErrPublishReturned", err)
	}

	// PubFailNotify 应收到该消息。
	time.Sleep(100 * time.Millisecond)
	ids := fc.ids()
	if len(ids) != 1 || ids[0] != msgID {
		t.Fatalf("PubFailNotify ids = %v, want [%s]", ids, msgID)
	}
}

func TestV2RoutablePublishStillSucceeds(t *testing.T) {
	requireRabbitMQ(t)

	queue := uniqueName(t, "q-routable")
	t.Cleanup(func() { deleteQueue(t, queue) })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	fc := &failCapture{}
	pub, err := rabbit.NewPubSimple(queue, mqURL,
		rabbit.WithContext(ctx), rabbit.WithPubFailNotify(fc.notify))
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	t.Cleanup(pub.Destroy)

	if _, err := pub.Publish([]byte("ok")); err != nil {
		t.Fatalf("Publish() error = %v, want nil", err)
	}
	if ids := fc.ids(); len(ids) != 0 {
		t.Fatalf("PubFailNotify should not fire on routable publish, got %v", ids)
	}
}

func TestV2BatchPublishReportsReturnedMessages(t *testing.T) {
	requireRabbitMQ(t)

	exchange := uniqueName(t, "ex-batch-unroutable")
	queue := uniqueName(t, "q-batch-bound")
	t.Cleanup(func() {
		deleteQueue(t, queue)
		deleteExchange(t, exchange)
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// 先用消费端声明 exchange（绑定 rk.bound）。
	cons, err := rabbit.NewConsumeDirect(exchange, "rk.bound", mqURL,
		rabbit.WithContext(ctx), rabbit.WithQueueName(queue))
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	t.Cleanup(cons.Destroy)
	go func() { _ = cons.Consume(newRecorder(1)) }()
	waitQueueReady(t, queue)

	fc := &failCapture{}
	pub, err := rabbit.NewPubDirect(exchange, "rk.unbound", mqURL,
		rabbit.WithContext(ctx), rabbit.WithPubFailNotify(fc.notify))
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	t.Cleanup(pub.Destroy)

	ids, err := pub.BatchPublish([][]byte{[]byte("a"), []byte("b"), []byte("c")})
	if !errors.Is(err, rabbit.ErrPublishReturned) {
		t.Fatalf("BatchPublish() error = %v, want ErrPublishReturned", err)
	}

	time.Sleep(150 * time.Millisecond)
	if got := len(fc.ids()); got != len(ids) {
		t.Fatalf("PubFailNotify fired %d times, want %d (all returned)", got, len(ids))
	}
}

// ----------------------------------------------------------------------------
// P1: 延迟消息 - 分桶消除队头阻塞
// ----------------------------------------------------------------------------

func TestV2DelayNoHeadOfLineBlocking(t *testing.T) {
	requireRabbitMQ(t)

	queue := uniqueName(t, "q-delay-hol")
	t.Cleanup(func() {
		deleteQueue(t, queue)
		deleteQueue(t, queue+"-delay-4000")
		deleteQueue(t, queue+"-delay-500")
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	rec := newRecorder(2)
	cons, err := rabbit.NewConsumeSimple(queue, mqURL, rabbit.WithContext(ctx))
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	t.Cleanup(cons.Destroy)
	go func() { _ = cons.Consume(rec) }()
	waitQueueReady(t, queue)

	pub, err := rabbit.NewPubSimple(queue, mqURL, rabbit.WithContext(ctx))
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	t.Cleanup(pub.Destroy)

	// 先发长 TTL（4s），再发短 TTL（500ms）。分桶后短 TTL 不被长 TTL 阻塞。
	if _, err := pub.PublishDelay([]byte("slow"), 4*time.Second); err != nil {
		t.Fatalf("publish slow: %v", err)
	}
	if _, err := pub.PublishDelay([]byte("fast"), 500*time.Millisecond); err != nil {
		t.Fatalf("publish fast: %v", err)
	}

	// 等第一条到达：应为 fast（约 500ms），远早于 slow（4s）。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(rec.snapshotReceived()) >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	got := rec.snapshotReceived()
	if len(got) < 1 || string(got[0]) != "fast" {
		t.Fatalf("first message = %v, want [fast] first (no head-of-line blocking)", bytesSlice(got))
	}
}

// ----------------------------------------------------------------------------
// P1: Stream 队列
// ----------------------------------------------------------------------------

func TestV2StreamPublishConsumeFromFirst(t *testing.T) {
	requireRabbitMQ(t)

	queue := uniqueName(t, "q-stream")
	t.Cleanup(func() { deleteQueue(t, queue) })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	const total = 3

	// 先发布 3 条到 stream。
	pub, err := rabbit.NewPubStream(queue, mqURL, rabbit.WithContext(ctx))
	if err != nil {
		t.Fatalf("new stream publisher: %v", err)
	}
	t.Cleanup(pub.Destroy)
	for i := range total {
		if _, err = pub.Publish([]byte("stream-" + strconv.Itoa(i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// 验证 broker 上确为 stream 队列。
	if qt := queueType(t, queue); qt != "stream" {
		t.Fatalf("queue %q type = %q, want stream", queue, qt)
	}

	// 从最早的 offset 消费，应重放全部 3 条。
	rec := newRecorder(total)
	cons, err := rabbit.NewConsumeStream(queue, mqURL, rabbit.WithContext(ctx))
	if err != nil {
		t.Fatalf("new stream consumer: %v", err)
	}
	t.Cleanup(cons.Destroy)
	go func() { _ = cons.Consume(rec, rabbit.OffsetFirst) }()

	rec.waitReceived(t, 15*time.Second)
	if got := len(rec.snapshotReceived()); got != total {
		t.Fatalf("stream received = %d, want %d", got, total)
	}
}

// ----------------------------------------------------------------------------
// P1: 连接隔离
// ----------------------------------------------------------------------------

func TestV2PublisherConnectionIsolation(t *testing.T) {
	requireRabbitMQ(t)

	queue := uniqueName(t, "q-iso")
	t.Cleanup(func() { deleteQueue(t, queue) })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// 先取稳定基线（确保上个测试的连接已关闭）。
	before := stableConnCount(t)

	rec := newRecorder(1)
	cfg, err := rabbit.NewConfig(mqURL,
		rabbit.WithContext(ctx),
		rabbit.WithPublisherConnection(),
	)
	if err != nil {
		t.Fatalf("new config: %v", err)
	}
	pub, err := rabbit.NewSimple(queue, cfg)
	if err != nil {
		t.Fatalf("new instance: %v", err)
	}
	t.Cleanup(pub.Destroy)
	go func() { _ = pub.Consume(rec) }()
	waitQueueReady(t, queue)

	if _, err := pub.Publish([]byte("isolated")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	rec.waitReceived(t, 10*time.Second)

	// 隔离模式下该实例应新增 2 条连接（消费共享连接 + 发布专用连接）。
	var delta int
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if delta = countAllConnections(t) - before; delta == 2 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if delta != 2 {
		t.Fatalf("connection delta = %d, want 2 (isolated publisher adds consume+publish connections)", delta)
	}
}

// ----------------------------------------------------------------------------
// P1: 延迟消息 - x-delayed-message 插件路径
// ----------------------------------------------------------------------------

func TestV2DelayedExchangePlugin(t *testing.T) {
	requireRabbitMQ(t)

	exchange := uniqueName(t, "ex-delayed")
	queue := uniqueName(t, "q-delayed")
	t.Cleanup(func() {
		deleteQueue(t, queue)
		deleteExchange(t, exchange+".delayed")
		deleteExchange(t, exchange)
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	rec := newRecorder(1)
	cons, err := rabbit.NewConsumeDirect(exchange, "rk", mqURL,
		rabbit.WithContext(ctx),
		rabbit.WithQueueName(queue),
		rabbit.WithDelayedExchange(),
	)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	t.Cleanup(cons.Destroy)
	go func() { _ = cons.Consume(rec) }()

	pub, err := rabbit.NewPubDirect(exchange, "rk", mqURL,
		rabbit.WithContext(ctx),
		rabbit.WithDelayedExchange(),
	)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	t.Cleanup(pub.Destroy)

	start := time.Now()
	_, err = pub.PublishDelay([]byte("delayed-plugin"), 600*time.Millisecond)
	if err != nil {
		// 插件未安装时 broker 拒绝 x-delayed-message exchange 类型，跳过。
		t.Skipf("rabbitmq_delayed_message_exchange plugin not available: %v", err)
	}

	rec.waitReceived(t, 10*time.Second)
	if elapsed := time.Since(start); elapsed < 500*time.Millisecond {
		t.Fatalf("delayed message arrived too early: %v", elapsed)
	}
}

func TestV2SimpleDelayedExchangePlugin(t *testing.T) {
	requireRabbitMQ(t)

	queue := uniqueName(t, "q-simple-delayed")
	t.Cleanup(func() {
		deleteQueue(t, queue)
		deleteExchange(t, queue+".delayed")
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	rec := newRecorder(1)
	cons, err := rabbit.NewConsumeSimple(queue, mqURL,
		rabbit.WithContext(ctx), rabbit.WithDelayedExchange())
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	t.Cleanup(cons.Destroy)
	go func() { _ = cons.Consume(rec) }()
	waitQueueReady(t, queue)

	pub, err := rabbit.NewPubSimple(queue, mqURL,
		rabbit.WithContext(ctx), rabbit.WithDelayedExchange())
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	t.Cleanup(pub.Destroy)

	start := time.Now()
	if _, err = pub.PublishDelay([]byte("simple-delayed"), 600*time.Millisecond); err != nil {
		t.Skipf("rabbitmq_delayed_message_exchange plugin not available: %v", err)
	}

	rec.waitReceived(t, 10*time.Second)
	if elapsed := time.Since(start); elapsed < 500*time.Millisecond {
		t.Fatalf("delayed message arrived too early: %v", elapsed)
	}
}

// ----------------------------------------------------------------------------
// P0-2: Quorum 队列
// ----------------------------------------------------------------------------

func TestV2QuorumPublishConsume(t *testing.T) {
	requireRabbitMQ(t)

	queue := uniqueName(t, "q-quorum")
	t.Cleanup(func() { deleteQueue(t, queue) })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	const total = 3
	rec := newRecorder(total)

	cons, err := rabbit.NewConsumeSimple(queue, mqURL,
		rabbit.WithContext(ctx), rabbit.WithQueueType(rabbit.QueueTypeQuorum))
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	t.Cleanup(cons.Destroy)
	go func() { _ = cons.Consume(rec) }()
	waitQueueReady(t, queue)

	pub, err := rabbit.NewPubSimple(queue, mqURL,
		rabbit.WithContext(ctx), rabbit.WithQueueType(rabbit.QueueTypeQuorum))
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	t.Cleanup(pub.Destroy)

	for i := range total {
		if _, err := pub.Publish([]byte("quorum-" + strconv.Itoa(i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	rec.waitReceived(t, 10*time.Second)

	// 验证 broker 上确为 quorum 队列。
	if qt := queueType(t, queue); qt != "quorum" {
		t.Fatalf("queue %q type = %q, want quorum", queue, qt)
	}
}

func TestV2QuorumOverClassicFailsPermanent(t *testing.T) {
	requireRabbitMQ(t)

	queue := uniqueName(t, "q-type-conflict")
	t.Cleanup(func() { deleteQueue(t, queue) })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// 先声明 classic 队列。
	classic, err := rabbit.NewPubSimple(queue, mqURL, rabbit.WithContext(ctx))
	if err != nil {
		t.Fatalf("new classic: %v", err)
	}
	t.Cleanup(classic.Destroy)
	if _, err = classic.Publish([]byte("seed")); err != nil {
		t.Fatalf("seed publish: %v", err)
	}
	waitQueueReady(t, queue)

	// 用 quorum 在同名队列上消费 → 声明冲突 → fast-fail 永久错误。
	quorum, err := rabbit.NewConsumeSimple(queue, mqURL,
		rabbit.WithContext(ctx), rabbit.WithQueueType(rabbit.QueueTypeQuorum))
	if err != nil {
		t.Fatalf("new quorum: %v", err)
	}
	t.Cleanup(quorum.Destroy)

	errCh := make(chan error, 1)
	go func() { errCh <- quorum.Consume(newRecorder(1)) }()

	select {
	case got := <-errCh:
		if !errors.Is(got, rabbit.ErrPermanent) {
			t.Fatalf("Consume() error = %v, want ErrPermanent", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Consume did not fast-fail on queue type conflict")
	}
}
