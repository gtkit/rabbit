package rabbit_test

import (
	"fmt"
	"time"

	"github.com/gtkit/rabbit"
)

// orderHandler 是业务消息处理器示例。
type orderHandler struct{}

func (orderHandler) Process(body []byte, _ string) error {
	fmt.Printf("处理订单: %s\n", body)
	return nil
}

func (orderHandler) Failed(msg rabbit.FailedMsg) {
	fmt.Printf("最终失败: %s\n", msg.MessageID)
}

// ExampleIdempotentHandler 演示给消费者套一层幂等去重。
// 单机 / 测试用内置 MemoryStore；分布式多实例请注入基于 Redis 等的共享实现。
func ExampleIdempotentHandler() {
	// TTL 内重复投递的同一条消息只会被业务处理一次。
	store := rabbit.NewMemoryStore(10 * time.Minute)
	handler := rabbit.IdempotentHandler(orderHandler{}, store)

	// 模拟同一 msgID 的消息被重复投递（如 ACK 丢失后 broker 重投）。
	_ = handler.Process([]byte("order-1001"), "msg-42")
	_ = handler.Process([]byte("order-1001"), "msg-42")

	// 实际使用：把包装后的 handler 传给 Consume。
	//   cons, _ := rabbit.NewConsumeSimple("queue", cfg)
	//   _ = cons.Consume(handler)

	// Output:
	// 处理订单: order-1001
}

// ExampleIdempotentHandler_customKey 演示用业务唯一键（而非 msgID）去重，
// 适用于生产者重试导致同一业务消息携带不同 msgID 的场景。
func ExampleIdempotentHandler_customKey() {
	store := rabbit.NewMemoryStore(10 * time.Minute)
	handler := rabbit.IdempotentHandler(orderHandler{}, store,
		rabbit.WithDedupKey(func(body []byte, _ string) (string, bool) {
			// 真实场景从 body 解析订单号；这里直接以 body 为键。
			return string(body), len(body) > 0
		}),
	)

	// 两条 msgID 不同但业务键相同的消息，只处理一次。
	_ = handler.Process([]byte("order-2002"), "msg-a")
	_ = handler.Process([]byte("order-2002"), "msg-b")

	// Output:
	// 处理订单: order-2002
}

// ExampleIdempotentClaimHandler 演示用原子 claim 状态机做生产级消费幂等。
// 分布式多实例请注入基于 Redis SETNX 或数据库唯一键 / 去重表的共享实现。
func ExampleIdempotentClaimHandler() {
	store := rabbit.NewMemoryClaimStore(time.Minute, 10*time.Minute)
	handler := rabbit.IdempotentClaimHandler(orderHandler{}, store,
		rabbit.WithClaimDedupKey(func(body []byte, _ string) (string, bool) {
			// 真实场景从 body 解析订单号、支付流水号或事件 ID。
			return string(body), len(body) > 0
		}),
	)

	// 第一条消息抢占 key 并处理成功；第二条业务 key 相同，直接跳过。
	_ = handler.Process([]byte("order-3003"), "msg-a")
	_ = handler.Process([]byte("order-3003"), "msg-b")

	// Output:
	// 处理订单: order-3003
}
