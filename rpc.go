package rabbit

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

// resetRPCState 清除 RPC 回复队列状态，使 ensureReplyQueue 下次可重建。
func (m *MQ) resetRPCState() {
	if m == nil {
		return
	}
	m.rpc.mu.Lock()
	m.rpc.initialized = false
	m.rpc.queueName = ""
	m.rpc.mu.Unlock()
}

var (
	// ErrRPCTimeout 在 RPC 调用超时时返回。
	ErrRPCTimeout = errors.New("mq: RPC call timed out")
	// ErrRPCNoReply 在 RPC 回复队列不可用时返回。
	ErrRPCNoReply = errors.New("mq: RPC reply queue not available")
)

// rpcDefaultTimeout 是 RPC 调用的默认超时。
const rpcDefaultTimeout = 30 * time.Second

// RPCHandler 处理 RPC 请求并返回应答。
// Handle 接收请求 body，返回应答 body。返回 error 时消息会被拒绝（不回复）。
type RPCHandler interface {
	Handle(body []byte) ([]byte, error)
}

// RPCHandlerFunc 是 RPCHandler 的函数适配器。
type RPCHandlerFunc func(body []byte) ([]byte, error)

// Handle 实现 RPCHandler 接口。
func (f RPCHandlerFunc) Handle(body []byte) ([]byte, error) {
	return f(body)
}

// RPCCallPublisher 是支持 RPC 调用的发布端接口。
// Call 发送请求并等待应答，详见各模式的 Call 方法 GoDoc。
type RPCCallPublisher interface {
	Call(body []byte) ([]byte, error)
}

// RPCServer 是 RPC 服务端接口。
// ServeRPC 持续消费请求并回复应答，详见各模式的 ServeRPC 方法 GoDoc。
type RPCServer interface {
	ServeRPC(handler RPCHandler) error
}

// rpcState 管理 RPC 调用所需的回复队列和待处理映射。
// 通过 MQ.rpc 字段访问，首次 Call 时懒初始化，连接断开后可自动重建。
type rpcState struct {
	mu          sync.Mutex
	queueName   string // 独占自动删除的回复队列名
	initialized bool   // 回复队列和消费者是否就绪
	pending     sync.Map
}

// pendingReply 是 rpcState.pending 中存储的值类型。
type pendingReply struct {
	ch chan []byte
}

// ensureReplyQueue 创建独占的回复队列并启动消费者。
// 连接断开后回复队列自动消失，下次 Call 会自动重建。
// 内部用互斥锁保护初始化路径，并发安全。
func (m *MQ) ensureReplyQueue() error {
	if m == nil {
		return ErrNotInitialized
	}

	m.rpc.mu.Lock()
	if m.rpc.initialized && m.rpc.queueName != "" {
		m.rpc.mu.Unlock()
		return nil
	}
	m.rpc.mu.Unlock()

	ch, err := m.openConsumerChannel()
	if err != nil {
		return err
	}

	queue, err := ch.QueueDeclare("", false, false, true, false, nil)
	if err != nil {
		_ = ch.Close()
		return err
	}

	deliveries, err := ch.Consume(queue.Name, "", true, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		return err
	}

	// 二次检查：避免两个并发 Call 都走到创建资源
	m.rpc.mu.Lock()
	if m.rpc.initialized && m.rpc.queueName != "" {
		m.rpc.mu.Unlock()
		_ = ch.Close()
		return nil
	}
	m.rpc.queueName = queue.Name
	m.rpc.initialized = true
	m.rpc.mu.Unlock()

	go m.rpcReplyLoop(deliveries, ch)
	return nil
}

// rpcReplyLoop 在后台 goroutine 中消费回复队列的消息，按 correlation-id 分发到对应等待者。
// 通道关闭时（连接断开 / channel 关闭）自动重置 initialized，下次 Call 重建。
func (m *MQ) rpcReplyLoop(deliveries <-chan amqp.Delivery, ch *amqp.Channel) {
	defer closeAMQPChannel(ch)
	defer m.resetRPCState()

	for d := range deliveries {
		corrID := d.CorrelationId
		if corrID == "" {
			continue
		}
		if val, loaded := m.rpc.pending.Load(corrID); loaded {
			pr, ok := val.(*pendingReply)
			if !ok {
				continue
			}
			select {
			case pr.ch <- d.Body:
			default:
			}
		}
	}
}

// callGeneric 执行 RPC 调用的核心逻辑：发布请求 + 等待应答。
// exchange 和 routingKey 由具体模式的 Call 方法传入。
func (m *MQ) callGeneric(exchange, routingKey string, body []byte) ([]byte, error) {
	if err := m.ensureReplyQueue(); err != nil {
		return nil, err
	}

	done, ok := m.trackPublish()
	if !ok {
		return nil, ErrDestroyed
	}
	defer done()

	ctx := m.contextOrBackground()

	select {
	case <-ctx.Done():
		return nil, m.canceledError("rpc call")
	default:
	}

	callCtx := ctx
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		callCtx, cancel = context.WithTimeout(ctx, rpcDefaultTimeout)
		defer cancel()
	}

	corrID := uuid.NewString()
	replyCh := make(chan []byte, 1)
	m.rpc.pending.Store(corrID, &pendingReply{ch: replyCh})
	defer m.rpc.pending.Delete(corrID)

	err := m.publishWithChannel(func(ch *amqp.Channel) error {
		pub := amqp.Publishing{
			DeliveryMode:  m.resolveDeliveryMode(),
			ContentType:   "application/octet-stream",
			MessageId:     corrID,
			CorrelationId: corrID,
			ReplyTo:       m.rpc.queueName,
			Body:          body,
			Headers:       amqp.Table{"x-retry": int32(0)},
		}
		_, pubErr := ch.PublishWithDeferredConfirmWithContext(callCtx, exchange, routingKey, true, false, pub)
		return pubErr
	})
	if err != nil {
		return nil, err
	}

	select {
	case reply := <-replyCh:
		return reply, nil
	case <-callCtx.Done():
		return nil, ErrRPCTimeout
	}
}

// serveRPCLoop 是 RPC 服务端的消费循环：消费请求 → 处理 → 回复。
// 不依赖 MsgHandler，直接处理 amqp.Delivery 并发布回复。
func (m *MQ) serveRPCLoop(handler RPCHandler, cfg consumerConfig) error {
	if handler == nil {
		return ErrHandlerRequired
	}

	ctx := m.contextOrBackground()
	retryAttempt := 0

	for {
		if err := ctx.Err(); err != nil {
			return m.canceledError(cfg.operation)
		}

		ch, queue, err := m.prepareConsume(cfg, &retryAttempt)
		if err != nil {
			if errors.Is(err, errAwaitRetry) {
				continue
			}
			return err
		}

		retryAttempt = 0

		deliveries, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
		if err != nil {
			closeAMQPChannel(ch)
			m.logger().Infof("%s start consume failed: %v, will reconnect", cfg.logTag, err)
			m.emitReconnect(ReconnectEvent{
				Operation: cfg.operation,
				Err:       err,
			})
			select {
			case <-ctx.Done():
				return m.canceledError(cfg.operation)
			case <-time.After(idleAfterClose):
			}
			continue
		}

		notifyClose := ch.NotifyClose(make(chan *amqp.Error, 1))

	loop:
		for {
			select {
			case <-ctx.Done():
				_ = ch.Cancel("", false)
				closeAMQPChannel(ch)
				return m.canceledError(cfg.operation)
			case closeErr, ok := <-notifyClose:
				_ = ch.Cancel("", false)
				closeAMQPChannel(ch)
				if ok && closeErr != nil {
					m.logger().Infof("%s channel closed: %v", cfg.logTag, closeErr)
					m.emitReconnect(ReconnectEvent{
						Operation: cfg.operation,
						Err:       closeErr,
					})
				}
				break loop
			case msg, ok := <-deliveries:
				if !ok {
					_ = ch.Cancel("", false)
					closeAMQPChannel(ch)
					break loop
				}

				m.handleRPCDelivery(ch, msg, handler, cfg)
			}
		}
	}
}

// handleRPCDelivery 处理单条 RPC 请求消息：调用 handler → 发布回复。
func (m *MQ) handleRPCDelivery(ch *amqp.Channel, msg amqp.Delivery, handler RPCHandler, cfg consumerConfig) {
	reply, err := handler.Handle(msg.Body)
	if err != nil {
		m.logger().Errorf("%s handler error (msgID=%s): %v", cfg.logTag, msg.MessageId, err)
		_ = msg.Reject(false)
		return
	}

	if msg.ReplyTo == "" {
		_ = msg.Ack(false)
		return
	}

	pubErr := ch.Publish("", msg.ReplyTo, false, false, amqp.Publishing{
		DeliveryMode:  amqp.Transient,
		ContentType:   msg.ContentType,
		CorrelationId: msg.CorrelationId,
		MessageId:     msg.MessageId,
		Body:          reply,
	})
	if pubErr != nil {
		m.logger().Errorf("%s publish reply failed (msgID=%s): %v", cfg.logTag, msg.MessageId, pubErr)
		_ = msg.Nack(false, true)
		return
	}

	_ = msg.Ack(false)
}
