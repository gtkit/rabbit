// e2e_test.go: 端到端集成测试，依赖真实 RabbitMQ broker。
//
// 用 MQ_INTEGRATION=1 启用：
//
//	MQ_INTEGRATION=1 go test ./test -run E2E -count=1 -timeout=5m -v
//
// 所有用例都使用 `t.Name() + nanos` 生成唯一队列名，避免互相干扰；
// 用 t.Cleanup 通过 Management API 清理拓扑；
// 用 ctx.WithTimeout 保证不会永阻塞。
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
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/gtkit/rabbit"
)

const (
	mgmtURL  = "http://127.0.0.1:15672"
	mgmtUser = "guest"
	mgmtPass = "guest"
)

// uniqueName 生成本次测试用的唯一名字。
func uniqueName(t *testing.T, prefix string) string {
	t.Helper()
	return fmt.Sprintf("%s-%s-%d", prefix, t.Name(), time.Now().UnixNano())
}

// deleteQueue 通过 Management API 删除队列（清理）。
func deleteQueue(t testing.TB, name string) {
	t.Helper()
	if name == "" {
		return
	}
	req, _ := http.NewRequest(http.MethodDelete,
		fmt.Sprintf("%s/api/queues/%%2F/%s", mgmtURL, url.PathEscape(name)), nil)
	req.SetBasicAuth(mgmtUser, mgmtPass)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Logf("cleanup delete queue %q: %v", name, err)
		return
	}
	resp.Body.Close()
}

// waitQueueReadyTimeout 是测试中等队列就绪的统一超时上限。
const waitQueueReadyTimeout = 5 * time.Second

// waitQueueReady 轮询 Management API，直到 queue 存在或超时。
func waitQueueReady(t *testing.T, name string) {
	t.Helper()
	deadline := time.Now().Add(waitQueueReadyTimeout)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest(http.MethodGet,
			fmt.Sprintf("%s/api/queues/%%2F/%s", mgmtURL, url.PathEscape(name)), nil)
		req.SetBasicAuth(mgmtUser, mgmtPass)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				// 再多等一拍让 binding 在 broker 端落定
				time.Sleep(100 * time.Millisecond)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("queue %q not ready within %v", name, waitQueueReadyTimeout)
}

// deleteExchange 通过 Management API 删除 exchange。
func deleteExchange(t testing.TB, name string) {
	t.Helper()
	if name == "" {
		return
	}
	req, _ := http.NewRequest(http.MethodDelete,
		fmt.Sprintf("%s/api/exchanges/%%2F/%s", mgmtURL, url.PathEscape(name)), nil)
	req.SetBasicAuth(mgmtUser, mgmtPass)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Logf("cleanup delete exchange %q: %v", name, err)
		return
	}
	resp.Body.Close()
}

// recordHandler 收集已消费消息体，达到 want 后通过 done channel 通知。
type recordHandler struct {
	mu       sync.Mutex
	received [][]byte
	want     int
	failNext int32 // 大于 0 时本次 Process 返回 error，并递减
	done     chan struct{}
	failed   []rabbit.FailedMsg
}

func newRecorder(want int) *recordHandler {
	return &recordHandler{
		want: want,
		done: make(chan struct{}),
	}
}

func (h *recordHandler) Process(body []byte, _ string) error {
	if atomic.LoadInt32(&h.failNext) > 0 {
		atomic.AddInt32(&h.failNext, -1)
		return errors.New("forced fail")
	}

	h.mu.Lock()
	h.received = append(h.received, append([]byte(nil), body...))
	n := len(h.received)
	h.mu.Unlock()

	if n == h.want {
		select {
		case <-h.done:
		default:
			close(h.done)
		}
	}
	return nil
}

func (h *recordHandler) Failed(msg rabbit.FailedMsg) {
	h.mu.Lock()
	h.failed = append(h.failed, msg)
	h.mu.Unlock()
}

// snapshotReceived 返回已消费消息体（snapshot）。
func (h *recordHandler) snapshotReceived() [][]byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([][]byte, len(h.received))
	copy(out, h.received)
	return out
}

// waitReceived 等待 want 条消息到达，或超时。
func (h *recordHandler) waitReceived(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case <-h.done:
	case <-time.After(timeout):
		got := h.snapshotReceived()
		t.Fatalf("timeout waiting for %d messages, got %d: %v", h.want, len(got), bytesSlice(got))
	}
}

func bytesSlice(b [][]byte) []string {
	out := make([]string, len(b))
	for i, x := range b {
		out[i] = string(x)
	}
	return out
}

// e2eObserver 在 E2E 测试中收集事件用于断言。
type e2eObserver struct {
	mu        sync.Mutex
	publishes []rabbit.PublishEvent
	consumes  []rabbit.ConsumeEvent
}

func (o *e2eObserver) OnPublish(e rabbit.PublishEvent) {
	o.mu.Lock()
	o.publishes = append(o.publishes, e)
	o.mu.Unlock()
}

func (o *e2eObserver) OnConsume(e rabbit.ConsumeEvent) {
	o.mu.Lock()
	o.consumes = append(o.consumes, e)
	o.mu.Unlock()
}

func (o *e2eObserver) OnReconnect(rabbit.ReconnectEvent) {}

func (o *e2eObserver) counts() (int, int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.publishes), len(o.consumes)
}

func TestE2EObserverReceivesPublishAndConsumeEvents(t *testing.T) {
	requireRabbitMQ(t)

	queue := uniqueName(t, "q-observer")
	t.Cleanup(func() { deleteQueue(t, queue) })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	obs := &e2eObserver{}
	rec := newRecorder(3)

	cons, err := rabbit.NewConsumeSimple(queue, mqURL,
		rabbit.WithContext(ctx),
		rabbit.WithObserver(obs),
	)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	t.Cleanup(cons.Destroy)
	go func() { _ = cons.Consume(rec) }()

	pub, err := rabbit.NewPubSimple(queue, mqURL,
		rabbit.WithContext(ctx),
		rabbit.WithObserver(obs),
	)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	t.Cleanup(pub.Destroy)

	for i := range 3 {
		if _, err := pub.Publish([]byte("obs-" + strconv.Itoa(i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	rec.waitReceived(t, 10*time.Second)

	// 给 emitConsume 一点点时间落入 observer
	time.Sleep(200 * time.Millisecond)
	pubCount, conCount := obs.counts()
	if pubCount != 3 {
		t.Fatalf("OnPublish calls = %d, want 3", pubCount)
	}
	if conCount != 3 {
		t.Fatalf("OnConsume calls = %d, want 3", conCount)
	}
}

// ----------------------------------------------------------------------------
// simple 模式
// ----------------------------------------------------------------------------

func TestE2ESimplePublishConsume(t *testing.T) {
	requireRabbitMQ(t)

	queue := uniqueName(t, "q-simple")
	t.Cleanup(func() { deleteQueue(t, queue) })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	const total = 5
	rec := newRecorder(total)

	cons, err := rabbit.NewConsumeSimple(queue, mqURL, rabbit.WithContext(ctx))
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	t.Cleanup(cons.Destroy)

	consDone := make(chan struct{})
	go func() {
		_ = cons.Consume(rec)
		close(consDone)
	}()

	pub, err := rabbit.NewPubSimple(queue, mqURL, rabbit.WithContext(ctx))
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	t.Cleanup(pub.Destroy)

	for i := range total {
		if _, err := pub.Publish([]byte("msg-" + strconv.Itoa(i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	rec.waitReceived(t, 10*time.Second)

	if got := len(rec.snapshotReceived()); got != total {
		t.Fatalf("received = %d, want %d", got, total)
	}

	cancel()
	select {
	case <-consDone:
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not exit after ctx cancel")
	}
}

func TestE2ESimpleRetry(t *testing.T) {
	requireRabbitMQ(t)

	queue := uniqueName(t, "q-simple-retry")
	t.Cleanup(func() {
		deleteQueue(t, queue)
		deleteQueue(t, queue+"-retry")
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	rec := newRecorder(1)
	atomic.StoreInt32(&rec.failNext, 2) // 头两次失败 → 进 retry → 第三次成功

	cons, err := rabbit.NewConsumeSimple(queue, mqURL,
		rabbit.WithContext(ctx),
		rabbit.WithMaxRetry(5),
		rabbit.WithRetryTTL(500*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	t.Cleanup(cons.Destroy)

	go func() { _ = cons.Consume(rec) }()

	pub, err := rabbit.NewPubSimple(queue, mqURL, rabbit.WithContext(ctx))
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	t.Cleanup(pub.Destroy)

	if _, err := pub.Publish([]byte("retry-me")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	rec.waitReceived(t, 10*time.Second)

	got := rec.snapshotReceived()
	if len(got) != 1 || string(got[0]) != "retry-me" {
		t.Fatalf("received = %v, want [retry-me]", bytesSlice(got))
	}

	if remaining := atomic.LoadInt32(&rec.failNext); remaining != 0 {
		t.Fatalf("failNext counter = %d, want 0 (means we did not retry enough)", remaining)
	}
}

func TestE2ESimpleConsumeFailToDlx(t *testing.T) {
	requireRabbitMQ(t)

	queue := uniqueName(t, "q-simple-dlx")
	t.Cleanup(func() {
		deleteQueue(t, queue)
		deleteQueue(t, "dlq-"+queue)
		deleteExchange(t, "dlx-"+queue)
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	mainRec := newRecorder(1)
	atomic.StoreInt32(&mainRec.failNext, 1) // 主队列消费失败 → 进 DLX
	dlqRec := newRecorder(1)

	main, err := rabbit.NewConsumeSimple(queue, mqURL, rabbit.WithContext(ctx))
	if err != nil {
		t.Fatalf("new main consumer: %v", err)
	}
	t.Cleanup(main.Destroy)

	dlqConsumer, err := rabbit.NewConsumeSimple(queue, mqURL, rabbit.WithContext(ctx))
	if err != nil {
		t.Fatalf("new dlq consumer: %v", err)
	}
	t.Cleanup(dlqConsumer.Destroy)

	go func() { _ = main.ConsumeFailToDlx(mainRec) }()
	go func() { _ = dlqConsumer.ConsumeDlx(dlqRec) }()

	pub, err := rabbit.NewPubSimple(queue, mqURL, rabbit.WithContext(ctx))
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	t.Cleanup(pub.Destroy)

	if _, err := pub.PublishWithDlx([]byte("to-dlx")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	dlqRec.waitReceived(t, 15*time.Second)

	got := dlqRec.snapshotReceived()
	if len(got) != 1 || string(got[0]) != "to-dlx" {
		t.Fatalf("dlq received = %v, want [to-dlx]", bytesSlice(got))
	}
}

func TestE2ESimpleDelay(t *testing.T) {
	requireRabbitMQ(t)

	queue := uniqueName(t, "q-simple-delay")
	t.Cleanup(func() {
		deleteQueue(t, queue)
		deleteQueue(t, queue+"-delay-800")
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	rec := newRecorder(1)

	cons, err := rabbit.NewConsumeSimple(queue, mqURL, rabbit.WithContext(ctx))
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	t.Cleanup(cons.Destroy)
	go func() { _ = cons.Consume(rec) }()

	pub, err := rabbit.NewPubSimple(queue, mqURL, rabbit.WithContext(ctx))
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	t.Cleanup(pub.Destroy)

	start := time.Now()
	if _, err := pub.PublishDelay([]byte("delayed"), 800*time.Millisecond); err != nil {
		t.Fatalf("publish delay: %v", err)
	}

	rec.waitReceived(t, 10*time.Second)
	elapsed := time.Since(start)

	if elapsed < 700*time.Millisecond {
		t.Fatalf("message arrived too early: elapsed=%v, want >= ~800ms", elapsed)
	}
}

// ----------------------------------------------------------------------------
// direct 模式
// ----------------------------------------------------------------------------

func TestE2EDirectPublishConsume(t *testing.T) {
	requireRabbitMQ(t)

	exchange := uniqueName(t, "ex-direct")
	routing := "rk." + t.Name()
	queue := uniqueName(t, "q-direct")
	t.Cleanup(func() {
		deleteQueue(t, queue)
		deleteExchange(t, exchange)
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	const total = 3
	rec := newRecorder(total)

	cons, err := rabbit.NewConsumeDirect(exchange, routing, mqURL,
		rabbit.WithContext(ctx),
		rabbit.WithQueueName(queue),
	)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	t.Cleanup(cons.Destroy)
	go func() { _ = cons.Consume(rec) }()

	// 等 consumer 声明并绑定 queue
	waitQueueReady(t, queue)

	pub, err := rabbit.NewPubDirect(exchange, routing, mqURL, rabbit.WithContext(ctx))
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	t.Cleanup(pub.Destroy)

	for i := range total {
		if _, err := pub.Publish(fmt.Appendf(nil, "d-%d", i)); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	rec.waitReceived(t, 10*time.Second)
}

// ----------------------------------------------------------------------------
// fanout 模式
// ----------------------------------------------------------------------------

func TestE2EFanoutBroadcast(t *testing.T) {
	requireRabbitMQ(t)

	exchange := uniqueName(t, "ex-fanout")
	queueA := uniqueName(t, "q-fanout-a")
	queueB := uniqueName(t, "q-fanout-b")
	t.Cleanup(func() {
		deleteQueue(t, queueA)
		deleteQueue(t, queueB)
		deleteExchange(t, exchange)
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	const total = 2
	recA := newRecorder(total)
	recB := newRecorder(total)

	consA, err := rabbit.NewConsumeFanout(exchange, mqURL,
		rabbit.WithContext(ctx), rabbit.WithQueueName(queueA))
	if err != nil {
		t.Fatalf("new consumer A: %v", err)
	}
	t.Cleanup(consA.Destroy)
	go func() { _ = consA.Consume(recA) }()

	consB, err := rabbit.NewConsumeFanout(exchange, mqURL,
		rabbit.WithContext(ctx), rabbit.WithQueueName(queueB))
	if err != nil {
		t.Fatalf("new consumer B: %v", err)
	}
	t.Cleanup(consB.Destroy)
	go func() { _ = consB.Consume(recB) }()

	waitQueueReady(t, queueA)
	waitQueueReady(t, queueB)

	pub, err := rabbit.NewPubFanout(exchange, mqURL, rabbit.WithContext(ctx))
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	t.Cleanup(pub.Destroy)

	for i := range total {
		if _, err := pub.Publish(fmt.Appendf(nil, "fan-%d", i)); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	recA.waitReceived(t, 10*time.Second)
	recB.waitReceived(t, 10*time.Second)
}

// ----------------------------------------------------------------------------
// topic 模式
// ----------------------------------------------------------------------------

func TestE2ETopicRoutingKey(t *testing.T) {
	requireRabbitMQ(t)

	exchange := uniqueName(t, "ex-topic")
	queueA := uniqueName(t, "q-topic-a")
	queueB := uniqueName(t, "q-topic-b")
	t.Cleanup(func() {
		deleteQueue(t, queueA)
		deleteQueue(t, queueB)
		deleteExchange(t, exchange)
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	recA := newRecorder(2) // 接收所有 order.*
	recB := newRecorder(1) // 仅接收 order.paid

	consA, err := rabbit.NewConsumeTopic(exchange, "order.*", mqURL,
		rabbit.WithContext(ctx), rabbit.WithQueueName(queueA))
	if err != nil {
		t.Fatalf("new consumer A: %v", err)
	}
	t.Cleanup(consA.Destroy)
	go func() { _ = consA.Consume(recA) }()

	consB, err := rabbit.NewConsumeTopic(exchange, "order.paid", mqURL,
		rabbit.WithContext(ctx), rabbit.WithQueueName(queueB))
	if err != nil {
		t.Fatalf("new consumer B: %v", err)
	}
	t.Cleanup(consB.Destroy)
	go func() { _ = consB.Consume(recB) }()

	waitQueueReady(t, queueA)
	waitQueueReady(t, queueB)

	pubA, err := rabbit.NewPubTopic(exchange, "order.created", mqURL, rabbit.WithContext(ctx))
	if err != nil {
		t.Fatalf("new publisher A: %v", err)
	}
	t.Cleanup(pubA.Destroy)

	pubB, err := rabbit.NewPubTopic(exchange, "order.paid", mqURL, rabbit.WithContext(ctx))
	if err != nil {
		t.Fatalf("new publisher B: %v", err)
	}
	t.Cleanup(pubB.Destroy)

	if _, err := pubA.Publish([]byte("created")); err != nil {
		t.Fatalf("publish created: %v", err)
	}
	if _, err := pubB.Publish([]byte("paid")); err != nil {
		t.Fatalf("publish paid: %v", err)
	}

	recA.waitReceived(t, 10*time.Second)
	recB.waitReceived(t, 10*time.Second)

	gotB := recB.snapshotReceived()
	if len(gotB) != 1 || string(gotB[0]) != "paid" {
		t.Fatalf("consumer B received %v, want [paid]", bytesSlice(gotB))
	}
}

// ----------------------------------------------------------------------------
// 重连
// ----------------------------------------------------------------------------

// closeAllConnections 通过 Management API 关闭所有当前 connection，模拟网络中断。
func closeAllConnections(t *testing.T) int {
	t.Helper()

	req, _ := http.NewRequest(http.MethodGet, mgmtURL+"/api/connections", nil)
	req.SetBasicAuth(mgmtUser, mgmtPass)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("list connections: %v", err)
	}
	defer resp.Body.Close()

	var conns []struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&conns); err != nil {
		t.Fatalf("decode connections: %v", err)
	}

	for _, c := range conns {
		req, _ := http.NewRequest(http.MethodDelete,
			fmt.Sprintf("%s/api/connections/%s", mgmtURL, url.PathEscape(c.Name)), nil)
		req.SetBasicAuth(mgmtUser, mgmtPass)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Logf("close conn %q: %v", c.Name, err)
			continue
		}
		resp.Body.Close()
	}
	return len(conns)
}

func TestE2EReconnectAfterConnectionClosed(t *testing.T) {
	requireRabbitMQ(t)

	queue := uniqueName(t, "q-reconnect")
	t.Cleanup(func() { deleteQueue(t, queue) })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	rec := newRecorder(2)

	cons, err := rabbit.NewConsumeSimple(queue, mqURL, rabbit.WithContext(ctx))
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	t.Cleanup(cons.Destroy)
	go func() { _ = cons.Consume(rec) }()

	pub, err := rabbit.NewPubSimple(queue, mqURL, rabbit.WithContext(ctx))
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	t.Cleanup(pub.Destroy)

	// 第一条消息
	if _, err := pub.Publish([]byte("before-kill")); err != nil {
		t.Fatalf("publish before: %v", err)
	}

	// 等第一条收到
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(rec.snapshotReceived()) >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := rec.snapshotReceived(); len(got) < 1 {
		t.Fatalf("first message not received: %v", bytesSlice(got))
	}

	// 关闭所有 connection
	n := closeAllConnections(t)
	t.Logf("closed %d connections", n)

	// 等库自己重连（指数退避，第一次重试 1s）
	time.Sleep(3 * time.Second)

	// 第二条消息：依赖 publisher 自动重连
	if _, err := pub.Publish([]byte("after-reconnect")); err != nil {
		t.Fatalf("publish after reconnect: %v", err)
	}

	rec.waitReceived(t, 15*time.Second)

	got := rec.snapshotReceived()
	if len(got) != 2 {
		t.Fatalf("received = %d, want 2: %v", len(got), bytesSlice(got))
	}

	bodies := bytesSlice(got)
	if bodies[0] != "before-kill" || bodies[1] != "after-reconnect" {
		t.Fatalf("received order = %v, want [before-kill after-reconnect]", bodies)
	}
}

// ----------------------------------------------------------------------------
// direct 模式 - retry
// ----------------------------------------------------------------------------

func TestE2EDirectRetry(t *testing.T) {
	requireRabbitMQ(t)

	exchange := uniqueName(t, "ex-direct-retry")
	routing := "rk." + t.Name()
	queue := uniqueName(t, "q-direct-retry")
	t.Cleanup(func() {
		deleteQueue(t, queue)
		deleteQueue(t, queue+".retry")
		deleteExchange(t, exchange)
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	rec := newRecorder(1)
	atomic.StoreInt32(&rec.failNext, 2)

	cons, err := rabbit.NewConsumeDirect(exchange, routing, mqURL,
		rabbit.WithContext(ctx),
		rabbit.WithQueueName(queue),
		rabbit.WithMaxRetry(5),
		rabbit.WithRetryTTL(500*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	t.Cleanup(cons.Destroy)
	go func() { _ = cons.Consume(rec) }()

	waitQueueReady(t, queue)

	pub, err := rabbit.NewPubDirect(exchange, routing, mqURL, rabbit.WithContext(ctx))
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	t.Cleanup(pub.Destroy)

	if _, err := pub.Publish([]byte("direct-retry")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	rec.waitReceived(t, 15*time.Second)

	got := rec.snapshotReceived()
	if len(got) != 1 || string(got[0]) != "direct-retry" {
		t.Fatalf("received = %v, want [direct-retry]", bytesSlice(got))
	}
	if remaining := atomic.LoadInt32(&rec.failNext); remaining != 0 {
		t.Fatalf("failNext = %d, want 0", remaining)
	}
}

func TestE2EDirectDelay(t *testing.T) {
	requireRabbitMQ(t)

	exchange := uniqueName(t, "ex-direct-delay")
	routing := "rk." + t.Name()
	queue := uniqueName(t, "q-direct-delay")
	t.Cleanup(func() {
		deleteQueue(t, queue)
		deleteQueue(t, queue+".delay.800")
		deleteExchange(t, exchange)
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	rec := newRecorder(1)

	cons, err := rabbit.NewConsumeDirect(exchange, routing, mqURL,
		rabbit.WithContext(ctx),
		rabbit.WithQueueName(queue),
	)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	t.Cleanup(cons.Destroy)
	go func() { _ = cons.Consume(rec) }()

	waitQueueReady(t, queue)

	pub, err := rabbit.NewPubDirect(exchange, routing, mqURL, rabbit.WithContext(ctx))
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	t.Cleanup(pub.Destroy)

	start := time.Now()
	if _, err := pub.PublishDelay([]byte("direct-delayed"), 800*time.Millisecond); err != nil {
		t.Fatalf("publish delay: %v", err)
	}

	rec.waitReceived(t, 10*time.Second)
	if elapsed := time.Since(start); elapsed < 700*time.Millisecond {
		t.Fatalf("message arrived too early: elapsed=%v", elapsed)
	}
}

func TestE2EDirectFailToDlx(t *testing.T) {
	requireRabbitMQ(t)

	exchange := uniqueName(t, "ex-direct-dlx")
	routing := "rk." + t.Name()
	queue := uniqueName(t, "q-direct-dlx")
	t.Cleanup(func() {
		deleteQueue(t, queue)
		deleteExchange(t, exchange)
		deleteExchange(t, exchange+".dlx")
		deleteQueue(t, queue+".dlq")
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	mainRec := newRecorder(1)
	atomic.StoreInt32(&mainRec.failNext, 1)
	dlqRec := newRecorder(1)

	main, err := rabbit.NewConsumeDirect(exchange, routing, mqURL,
		rabbit.WithContext(ctx),
		rabbit.WithQueueName(queue),
	)
	if err != nil {
		t.Fatalf("new main consumer: %v", err)
	}
	t.Cleanup(main.Destroy)
	go func() { _ = main.ConsumeFailToDlx(mainRec) }()

	dlqConsumer, err := rabbit.NewConsumeDirect(exchange, routing, mqURL,
		rabbit.WithContext(ctx),
		rabbit.WithQueueName(queue),
	)
	if err != nil {
		t.Fatalf("new dlq consumer: %v", err)
	}
	t.Cleanup(dlqConsumer.Destroy)
	go func() { _ = dlqConsumer.ConsumeDlx(dlqRec) }()

	waitQueueReady(t, queue)

	pub, err := rabbit.NewPubDirect(exchange, routing, mqURL, rabbit.WithContext(ctx))
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	t.Cleanup(pub.Destroy)

	if _, err := pub.PublishWithDlx([]byte("direct-to-dlx")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	dlqRec.waitReceived(t, 15*time.Second)

	got := dlqRec.snapshotReceived()
	if len(got) != 1 || string(got[0]) != "direct-to-dlx" {
		t.Fatalf("dlq received = %v, want [direct-to-dlx]", bytesSlice(got))
	}
}

// ----------------------------------------------------------------------------
// topic 模式 - retry & delay
// ----------------------------------------------------------------------------

func TestE2ETopicRetry(t *testing.T) {
	requireRabbitMQ(t)

	exchange := uniqueName(t, "ex-topic-retry")
	routing := "rk." + t.Name()
	queue := uniqueName(t, "q-topic-retry")
	t.Cleanup(func() {
		deleteQueue(t, queue)
		deleteQueue(t, queue+".retry")
		deleteExchange(t, exchange)
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	rec := newRecorder(1)
	atomic.StoreInt32(&rec.failNext, 2)

	cons, err := rabbit.NewConsumeTopic(exchange, routing, mqURL,
		rabbit.WithContext(ctx),
		rabbit.WithQueueName(queue),
		rabbit.WithMaxRetry(5),
		rabbit.WithRetryTTL(500*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	t.Cleanup(cons.Destroy)
	go func() { _ = cons.Consume(rec) }()

	waitQueueReady(t, queue)

	pub, err := rabbit.NewPubTopic(exchange, routing, mqURL, rabbit.WithContext(ctx))
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	t.Cleanup(pub.Destroy)

	if _, err := pub.Publish([]byte("topic-retry")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	rec.waitReceived(t, 15*time.Second)

	got := rec.snapshotReceived()
	if len(got) != 1 || string(got[0]) != "topic-retry" {
		t.Fatalf("received = %v, want [topic-retry]", bytesSlice(got))
	}
	if remaining := atomic.LoadInt32(&rec.failNext); remaining != 0 {
		t.Fatalf("failNext = %d, want 0", remaining)
	}
}

func TestE2ETopicDelay(t *testing.T) {
	requireRabbitMQ(t)

	exchange := uniqueName(t, "ex-topic-delay")
	routing := "rk." + t.Name()
	queue := uniqueName(t, "q-topic-delay")
	t.Cleanup(func() {
		deleteQueue(t, queue)
		deleteQueue(t, queue+".delay.800")
		deleteExchange(t, exchange)
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	rec := newRecorder(1)

	cons, err := rabbit.NewConsumeTopic(exchange, routing, mqURL,
		rabbit.WithContext(ctx),
		rabbit.WithQueueName(queue),
	)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	t.Cleanup(cons.Destroy)
	go func() { _ = cons.Consume(rec) }()

	waitQueueReady(t, queue)

	pub, err := rabbit.NewPubTopic(exchange, routing, mqURL, rabbit.WithContext(ctx))
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	t.Cleanup(pub.Destroy)

	start := time.Now()
	if _, err := pub.PublishDelay([]byte("topic-delayed"), 800*time.Millisecond); err != nil {
		t.Fatalf("publish delay: %v", err)
	}

	rec.waitReceived(t, 10*time.Second)
	if elapsed := time.Since(start); elapsed < 700*time.Millisecond {
		t.Fatalf("message arrived too early: elapsed=%v", elapsed)
	}
}

// ----------------------------------------------------------------------------
// fanout 模式 - DLX
// ----------------------------------------------------------------------------

func TestE2EFanoutFailToDlx(t *testing.T) {
	requireRabbitMQ(t)

	exchange := uniqueName(t, "ex-fanout-dlx")
	queue := uniqueName(t, "q-fanout-dlx")
	t.Cleanup(func() {
		deleteQueue(t, queue)
		deleteExchange(t, exchange)
		deleteExchange(t, exchange+".dlx")
		deleteQueue(t, queue+".dlq")
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	mainRec := newRecorder(1)
	atomic.StoreInt32(&mainRec.failNext, 1)
	dlqRec := newRecorder(1)

	main, err := rabbit.NewConsumeFanout(exchange, mqURL,
		rabbit.WithContext(ctx),
		rabbit.WithQueueName(queue),
	)
	if err != nil {
		t.Fatalf("new main consumer: %v", err)
	}
	t.Cleanup(main.Destroy)
	go func() { _ = main.ConsumeFailToDlx(mainRec) }()

	dlqConsumer, err := rabbit.NewConsumeFanout(exchange, mqURL,
		rabbit.WithContext(ctx),
		rabbit.WithQueueName(queue),
	)
	if err != nil {
		t.Fatalf("new dlq consumer: %v", err)
	}
	t.Cleanup(dlqConsumer.Destroy)
	go func() { _ = dlqConsumer.ConsumeDlx(dlqRec) }()

	waitQueueReady(t, queue)

	pub, err := rabbit.NewPubFanout(exchange, mqURL, rabbit.WithContext(ctx))
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	t.Cleanup(pub.Destroy)

	if _, err := pub.PublishWithDlx([]byte("fanout-to-dlx")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	dlqRec.waitReceived(t, 15*time.Second)

	got := dlqRec.snapshotReceived()
	if len(got) != 1 || string(got[0]) != "fanout-to-dlx" {
		t.Fatalf("dlq received = %v, want [fanout-to-dlx]", bytesSlice(got))
	}
}

// ----------------------------------------------------------------------------
// headers 模式
// ----------------------------------------------------------------------------

func TestE2EHeadersPublishConsume(t *testing.T) {
	requireRabbitMQ(t)

	exchange := uniqueName(t, "ex-headers")
	queue := uniqueName(t, "q-headers")
	t.Cleanup(func() {
		deleteQueue(t, queue)
		deleteExchange(t, exchange)
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	binding := rabbit.HeaderBinding{
		MatchAll: false,
		Headers:  map[string]any{"format": "json", "type": "event"},
	}
	rec := newRecorder(2)

	cons, err := rabbit.NewConsumeHeaders(exchange, binding, mqURL,
		rabbit.WithContext(ctx),
		rabbit.WithQueueName(queue),
	)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	t.Cleanup(cons.Destroy)
	go func() { _ = cons.Consume(rec) }()

	waitQueueReady(t, queue)

	pub, err := rabbit.NewPubHeaders(exchange, mqURL, rabbit.WithContext(ctx))
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	t.Cleanup(pub.Destroy)

	if _, err := pub.PublishWithHeaders([]byte("match-json"), amqp.Table{"format": "json", "type": "event"}); err != nil {
		t.Fatalf("publish match: %v", err)
	}
	if _, err := pub.PublishWithHeaders([]byte("match-event"), amqp.Table{"type": "event"}); err != nil {
		t.Fatalf("publish match event: %v", err)
	}

	rec.waitReceived(t, 10*time.Second)

	got := rec.snapshotReceived()
	if len(got) != 2 {
		t.Fatalf("received = %d, want 2: %v", len(got), bytesSlice(got))
	}
}

// ----------------------------------------------------------------------------
// simple RPC
// ----------------------------------------------------------------------------

func TestE2ESimpleRPC(t *testing.T) {
	requireRabbitMQ(t)

	queue := uniqueName(t, "q-rpc")
	t.Cleanup(func() { deleteQueue(t, queue) })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	server, err := rabbit.NewConsumeSimple(queue, mqURL, rabbit.WithContext(ctx))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	t.Cleanup(server.Destroy)

	serverDone := make(chan struct{})
	go func() {
		_ = server.ServeRPC(rabbit.RPCHandlerFunc(func(body []byte) ([]byte, error) {
			return append([]byte("reply:"), body...), nil
		}))
		close(serverDone)
	}()

	client, err := rabbit.NewPubSimple(queue, mqURL, rabbit.WithContext(ctx))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(client.Destroy)

	waitQueueReady(t, queue)

	reply, err := client.Call([]byte("hello"))
	if err != nil {
		t.Fatalf("Call error: %v", err)
	}
	if string(reply) != "reply:hello" {
		t.Fatalf("Call reply = %q, want reply:hello", string(reply))
	}

	cancel()
	select {
	case <-serverDone:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not exit after ctx cancel")
	}
}

func TestE2EDirectRPC(t *testing.T) {
	requireRabbitMQ(t)

	exchange := uniqueName(t, "ex-rpc")
	routing := "rk." + t.Name()
	queue := uniqueName(t, "q-rpc")
	t.Cleanup(func() {
		deleteQueue(t, queue)
		deleteExchange(t, exchange)
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	server, err := rabbit.NewConsumeDirect(exchange, routing, mqURL,
		rabbit.WithContext(ctx),
		rabbit.WithQueueName(queue),
	)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	t.Cleanup(server.Destroy)
	go func() {
		_ = server.ServeRPC(rabbit.RPCHandlerFunc(func(body []byte) ([]byte, error) {
			return append([]byte("echo:"), body...), nil
		}))
	}()

	client, err := rabbit.NewPubDirect(exchange, routing, mqURL, rabbit.WithContext(ctx))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(client.Destroy)

	waitQueueReady(t, queue)

	reply, err := client.Call([]byte("direct-rpc"))
	if err != nil {
		t.Fatalf("Call error: %v", err)
	}
	if string(reply) != "echo:direct-rpc" {
		t.Fatalf("Call reply = %q, want echo:direct-rpc", string(reply))
	}
}
