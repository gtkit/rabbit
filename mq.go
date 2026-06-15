// Package rabbit 是基于 amqp091-go 的 RabbitMQ 封装。
//
// 包内分层：
//
//   - mq.go        — 核心类型 MQ、配置 MQOption、公共接口（MQInterface / Retrier）
//     错误哨兵、Observer 钩子、recover 保护的回调入口。
//   - runtime.go   — 连接 / 通道 / 重连 / 退避 / 公共消费循环（consumerConfig）。
//   - simple.go    — simple 模式（默认 exchange + queue 直发）。
//   - routed.go    — direct / topic 共用实现（routedMQ）。
//   - direct.go    — direct 模式的薄壳（仅指定 exchange kind）。
//   - topic.go     — topic 模式的薄壳。
//   - fanout.go    — fanout 模式（广播，无重试）。
//   - config.go    — Option 函数 + 配置归一化。
//   - log.go       — Logger 接口 + 全局 / 实例双层注入 + 自适应适配器。
package rabbit

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// defaultMaxRetry 是消费失败的默认最大重试次数。
	defaultMaxRetry = int32(3)
	// defaultRetryTTL 是 retry queue 中消息的默认停留时长。
	defaultRetryTTL = 2 * time.Second
	// defaultVhost 是默认 AMQP vhost。
	defaultVhost = "/"
	// defaultHeartbeat 是默认 AMQP 心跳间隔。
	defaultHeartbeat = 10 * time.Second
	// defaultPrefetchCount 是默认的消费端预取数。
	defaultPrefetchCount = 1
)

// 错误哨兵，使用方可用 errors.Is 判定。
var (
	// ErrDestroyed 在实例已 Destroy 后再次发起 Publish 时返回。
	ErrDestroyed = errors.New("mq: instance is destroyed")

	// ErrPublishNotAcknowledged 在 broker 回复 nack 时返回（confirm 通道收到 false）。
	ErrPublishNotAcknowledged = errors.New("mq: publish not acknowledged by broker")

	// ErrConnectionNotInitialized 在尝试使用尚未建立的 connection 时返回。
	ErrConnectionNotInitialized = errors.New("mq: connection is not initialized")

	// ErrHandlerRequired 在 Consume* 系列传入 nil handler 时返回。
	ErrHandlerRequired = errors.New("mq: handler is required")

	// ErrNotInitialized 在对零值或 nil 实例调用方法时返回。
	ErrNotInitialized = errors.New("mq: instance is not initialized")

	// ErrPermanent 标识协议层永久不可恢复的错误（凭证错误 / queue type 冲突 /
	// access-refused / not-found 等），重试无效，需要外部干预。
	// 调用方可用 errors.Is(err, ErrPermanent) 判定。
	// 内部以 amqp.Error.Temporary() == false 判定（amqp091 v1.11+ 提供）。
	ErrPermanent = errors.New("mq: permanent broker error (requires external action)")
)

// isPermanent 判定一个错误是否在协议层永久不可恢复。
// 仅识别 *amqp.Error 类型；网络层错误（io / net）按可重试处理。
func isPermanent(err error) bool {
	if err == nil {
		return false
	}

	var amqpErr *amqp.Error
	if !errors.As(err, &amqpErr) {
		return false
	}

	return !amqpErr.Temporary()
}

// permanentError 把原 err 包成 ErrPermanent + 原因，调用方 errors.Is(err, ErrPermanent)。
func permanentError(err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("%w: %w", ErrPermanent, err)
}

// MQOption 描述构造 MQ 实例时的配置。
// 所有字段建议通过 With* Option 函数设置。
// 注意：MQOption 一旦传给 newMQ 即视为不可变；不要在运行期改写其字段。
type MQOption struct {
	// 业务标识。
	ExchangeName string
	QueueName    string
	RoutingKey   string

	// 连接参数。
	MQURL     string
	ConnName  string
	Vhost     string        // 默认 "/"。
	Heartbeat time.Duration // 默认 10s。
	TLSConfig *tls.Config   // 非空时启用 amqps:// TLS 连接。

	// 生命周期 ctx。被 newMQ 内部 WithCancel 后用于全局取消。
	Ctx context.Context

	// 失败重试相关。
	MaxRetry int32         // 默认 3。
	RetryTTL time.Duration // 默认 2s。

	// 消费端预取数。默认 1。
	PrefetchCount int

	// DeliveryMode 是发布消息的投递模式。
	// 0 = 使用默认值（amqp.Persistent，即持久化），1 = 非持久化（amqp.Transient）。
	// 高吞吐、可容忍消息丢失的场景可设为 1 以减少写入确认开销。
	DeliveryMode uint8

	// QueueArgs 是队列声明时的额外参数，如 x-message-ttl、x-max-length 等。
	// 通过 WithQueueArgs 设置；若为 nil 则不附加额外参数。
	QueueArgs amqp.Table
	// ExchangeArgs 是交换机声明时的额外参数，通过 WithExchangeArg 设置。
	ExchangeArgs amqp.Table

	// 日志与回调注入。
	Logger        Logger
	PubFailNotify func(FailedMsg)
	Observer      Observer
}

// MQ 封装了共享的 connection、publish channel 和生命周期控制逻辑。
// 业务侧不直接构造 MQ；通过 New{Pub,Consume}{Simple,Direct,Fanout,Topic} 拿到具体模式实例。
type MQ struct {
	opt MQOption
	// mode 用于事件标签，例如 "simple" / "direct" / "fanout" / "topic"。
	mode string

	// 连接互斥与状态。
	connMu sync.Mutex
	conn   *amqp.Connection

	// publish channel 与拓扑声明缓存（同一个 channel 内只声明一次）。
	pubMu    sync.Mutex
	pubCh    *amqp.Channel
	pubDecls map[string]struct{}

	// 生命周期：destroyed 在 Destroy 中置位，publishWG 跟踪 in-flight publish。
	lifeMu    sync.Mutex
	destroyed bool
	publishWG sync.WaitGroup
	closeOnce sync.Once
	cancel    context.CancelFunc

	// RPC 状态：reply queue + pending 映射，通过 EnsureReplyQueue 懒初始化。
	rpc rpcState
}

// MsgHandler 定义了消费端的消息处理器。
// Process 负责处理消息，返回 error 触发 retry 或 reject。
// Failed 在最终失败（重试耗尽 / DLX 处理失败 / 上下文取消）时收到通知。
type MsgHandler interface {
	Process(body []byte, msgID string) error
	Failed(FailedMsg)
}

// FailedMsg 表示发布失败或消费最终失败时传递给业务方的消息上下文。
type FailedMsg struct {
	ExchangeName string
	QueueName    string
	RoutingKey   string
	MessageID    string
	Message      []byte
}

// MQInterface 定义了所有模式真正都支持的最小公共行为。
// Fanout 不支持自动 retry，故 RetryMsg 不在此接口；
// 需要手动重试能力的请用 Retrier。
//
// 注意：该接口同时包含了发布和消费方法。如果只需要发布或消费单一能力，
// 建议使用更细粒度的 Publisher / Consumer 接口。
type MQInterface interface {
	Publish(body []byte) (string, error)
	Consume(handler MsgHandler) error
	PublishDelay(body []byte, ttl time.Duration) (string, error)
	ConsumeFailToDlx(handler MsgHandler) error
	ConsumeDlx(handler MsgHandler) error
}

// Publisher 定义了所有模式的发布能力。适用于不需要消费能力的场景。
type Publisher interface {
	Publish(body []byte) (string, error)
	PublishDelay(body []byte, ttl time.Duration) (string, error)
	PublishString(msg string) (string, error)
}

// Consumer 定义了所有模式的消费能力。适用于不需要发布能力的场景。
type Consumer interface {
	Consume(handler MsgHandler) error
	ConsumeFailToDlx(handler MsgHandler) error
	ConsumeDlx(handler MsgHandler) error
}

// Retrier 是支持手动把当前消息投入 retry queue 的能力。
// MQSimple / MQDirect / MQTopic 实现此接口；MQFanout 不实现。
type Retrier interface {
	RetryMsg(msg amqp.Delivery, ttl time.Duration) error
}

// PublishEvent 是一次发布操作的可观测信息。
// 通过 Observer.OnPublish 暴露给监控系统（Prometheus / OpenTelemetry 等）。
type PublishEvent struct {
	Mode         string // "simple" / "direct" / "fanout" / "topic"
	Operation    string // "publish" / "publish delay" / "publish with dlx" / "publish retry"
	ExchangeName string
	RoutingKey   string
	QueueName    string
	MessageID    string
	BodySize     int
	Duration     time.Duration
	Err          error // 成功为 nil
}

// ConsumeEvent 是一次消费投递的可观测信息。
type ConsumeEvent struct {
	Mode         string
	Operation    string // "consume" / "consume fail-to-dlx" / "consume dlx"
	ExchangeName string
	RoutingKey   string
	QueueName    string
	MessageID    string
	BodySize     int
	Duration     time.Duration
	Retry        int32 // 进入本次 Process 前消息已重试的次数（来自 x-retry header）
	Err          error // Process 返回的 error；成功为 nil
}

// ReconnectEvent 描述一次重连尝试触发的原因。
type ReconnectEvent struct {
	Mode      string
	Operation string // 触发重连的操作
	Attempt   int    // 当前是第几次退避（0 起步）
	Err       error  // 触发原因，可能是 channel 关闭、connection 关闭或拓扑声明失败
	// Permanent 为 true 时表示这是一个永久错误（凭证 / queue type 冲突等），
	// 消费循环已经 fast-fail 退出，不会再有后续重连。
	Permanent bool
}

// Observer 是埋点钩子。所有方法都在调用方 goroutine 同步执行，
// 由库内部 recover 保护，业务回调不应阻塞。
type Observer interface {
	OnPublish(event PublishEvent)
	OnConsume(event ConsumeEvent)
	OnReconnect(event ReconnectEvent)
}

// newMQ 是内部构造函数。各模式构造完 MQOption 后调用此函数获得 *MQ。
// 内部会派生 cancellable ctx 用于全局生命周期。
func newMQ(option MQOption, mode string) (*MQ, error) {
	ctx, cancel := context.WithCancel(option.Ctx)
	option.Ctx = ctx

	mq := &MQ{
		opt:    option,
		mode:   mode,
		cancel: cancel,
	}

	conn, err := mq.dial()
	if err != nil {
		cancel()
		return nil, err
	}

	mq.conn = conn

	return mq, nil
}

// Destroy 关闭 connection、publish channel，等待 in-flight publish 完成，
// 并取消内部 context。可重复调用。
func (m *MQ) Destroy() {
	if m == nil {
		return
	}

	m.closeOnce.Do(func() {
		// 1. 拒绝新的 publish 进入。
		m.lifeMu.Lock()
		m.destroyed = true
		m.lifeMu.Unlock()

		// 2. 通知所有正在阻塞的 publish 立刻返回。
		if m.cancel != nil {
			m.cancel()
		}

		// 3. 等待已 Add 的 publish 走完（无论成功或被 ctx 短路）。
		m.publishWG.Wait()

		// 4. 关闭 publish channel + 重置声明缓存。
		m.closePublishChannel()

		// 5. 关闭 connection。
		m.connMu.Lock()
		conn := m.conn
		m.conn = nil
		m.connMu.Unlock()

		if conn != nil {
			if err := conn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
				m.logger().Infof("close connection error: %v", err)
			}
		}
	})
}

// IsReady 检查当前实例的连接和发布通道是否处于可用状态。
// 适用于 Kubernetes 存活/就绪探针、监控巡检等场景。
func (m *MQ) IsReady() bool {
	if m == nil {
		return false
	}

	m.connMu.Lock()
	conn := m.conn
	m.connMu.Unlock()

	if conn == nil || conn.IsClosed() {
		return false
	}

	m.pubMu.Lock()
	ch := m.pubCh
	m.pubMu.Unlock()

	return ch != nil && !ch.IsClosed()
}

// trackPublish 由业务发起的 publish 入口调用，注册到 publishWG。
// 已 Destroy 时返回 ok=false 且不增加计数；调用方应直接返回 ErrDestroyed。
func (m *MQ) trackPublish() (done func(), ok bool) {
	if m == nil {
		return func() {}, false
	}

	m.lifeMu.Lock()
	defer m.lifeMu.Unlock()

	if m.destroyed {
		return func() {}, false
	}

	m.publishWG.Add(1)
	return m.publishWG.Done, true
}

// logger 返回实例 logger，未设置时回退到全局。
func (m *MQ) logger() Logger {
	if m != nil && m.opt.Logger != nil {
		return m.opt.Logger
	}

	return currentLogger()
}

// contextOrBackground 返回实例 ctx，nil 时回退到 context.Background()。
// 保证库内部任何 select case 都不会 panic on nil channel。
func (m *MQ) contextOrBackground() context.Context {
	if m == nil || m.opt.Ctx == nil {
		return context.Background()
	}

	return m.opt.Ctx
}

// canceledError 在 ctx 已取消时，返回带 operation 前缀的 wrapped error；否则返回 nil。
// 调用方可用 errors.Is(err, context.Canceled) 判定。
func (m *MQ) canceledError(operation string) error {
	if err := m.contextOrBackground().Err(); err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}

	return nil
}

// failedMessage 构造 FailedMsg，把当前实例的拓扑信息一并填入。
// message 会做深拷贝，避免业务方持有 FailedMsg 后因切片底层数组复用导致数据错乱。
func (m *MQ) failedMessage(message []byte, msgID string) FailedMsg {
	body := make([]byte, len(message))
	copy(body, message)

	if m == nil {
		return FailedMsg{
			MessageID: msgID,
			Message:   body,
		}
	}

	return FailedMsg{
		ExchangeName: m.opt.ExchangeName,
		QueueName:    m.opt.QueueName,
		RoutingKey:   m.opt.RoutingKey,
		MessageID:    msgID,
		Message:      body,
	}
}

// notifyConsumeFailed 通知消费端最终失败，自动 recover 业务回调中的 panic。
func (m *MQ) notifyConsumeFailed(handler MsgHandler, msg FailedMsg) {
	if handler == nil {
		return
	}

	defer func() {
		if rec := recover(); rec != nil {
			m.logger().Errorf("MsgHandler.Failed panic recovered: %v\n%s", rec, debug.Stack())
		}
	}()

	handler.Failed(msg)
}

// notifyPubFailed 触发实例上配置的 publish 失败回调，自动 recover 业务回调中的 panic。
func (m *MQ) notifyPubFailed(msg FailedMsg) {
	if m == nil || m.opt.PubFailNotify == nil {
		return
	}

	defer func() {
		if rec := recover(); rec != nil {
			m.logger().Errorf("PubFailNotify panic recovered: %v\n%s", rec, debug.Stack())
		}
	}()

	m.opt.PubFailNotify(msg)
}

// safeProcess 在 recover 保护下调用 handler.Process，panic 转换为 error 并记录 stack。
func (m *MQ) safeProcess(handler MsgHandler, body []byte, msgID string) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("handler panic: %v", rec)
			m.logger().Errorf("handler.Process panic recovered: %v (msgID=%s)\n%s",
				rec, msgID, debug.Stack())
		}
	}()

	return handler.Process(body, msgID)
}

// emitPublish 在配置了 Observer 时投递一次发布事件，panic recover。
func (m *MQ) emitPublish(event PublishEvent) {
	if m == nil || m.opt.Observer == nil {
		return
	}

	event.Mode = m.mode
	if event.ExchangeName == "" {
		event.ExchangeName = m.opt.ExchangeName
	}
	if event.RoutingKey == "" {
		event.RoutingKey = m.opt.RoutingKey
	}
	if event.QueueName == "" {
		event.QueueName = m.opt.QueueName
	}

	defer func() {
		if rec := recover(); rec != nil {
			m.logger().Errorf("Observer.OnPublish panic recovered: %v\n%s", rec, debug.Stack())
		}
	}()

	m.opt.Observer.OnPublish(event)
}

// emitConsume 在配置了 Observer 时投递一次消费事件，panic recover。
func (m *MQ) emitConsume(event ConsumeEvent) {
	if m == nil || m.opt.Observer == nil {
		return
	}

	event.Mode = m.mode
	if event.ExchangeName == "" {
		event.ExchangeName = m.opt.ExchangeName
	}
	if event.RoutingKey == "" {
		event.RoutingKey = m.opt.RoutingKey
	}
	if event.QueueName == "" {
		event.QueueName = m.opt.QueueName
	}

	defer func() {
		if rec := recover(); rec != nil {
			m.logger().Errorf("Observer.OnConsume panic recovered: %v\n%s", rec, debug.Stack())
		}
	}()

	m.opt.Observer.OnConsume(event)
}

// emitReconnect 在配置了 Observer 时投递一次重连事件，panic recover。
func (m *MQ) emitReconnect(event ReconnectEvent) {
	if m == nil || m.opt.Observer == nil {
		return
	}

	event.Mode = m.mode

	defer func() {
		if rec := recover(); rec != nil {
			m.logger().Errorf("Observer.OnReconnect panic recovered: %v\n%s", rec, debug.Stack())
		}
	}()

	m.opt.Observer.OnReconnect(event)
}

// declareStep 描述一次拓扑声明。cacheKey 用于 ensurePublishDeclared 去重。
type declareStep struct {
	cacheKey string
	declare  func(*amqp.Channel) error
}

// publishRequest 描述一次发布操作的参数，由 publishGeneric 统一执行。
// 调用方只需填充差异部分（目标、声明、发布参数），公共流程由库保证。
type publishRequest struct {
	// 操作名称，用于事件日志与错误上下文，例如 "publish" / "publish delay"。
	operation string

	// 消息体与元数据。
	body       []byte
	msgID      string // 空串时自动生成 UUID
	exchange   string // 要发布到的 exchange（空串 = default exchange）
	routingKey string // simple 模式填 queue name；routed 模式填 routing key
	mandatory  bool   // 是否要求消息必须路由到至少一个队列
	immediate  bool   // AMQP 已废弃，保留 false 即可

	// 发布参数。
	contentType  string // 空串默认 "application/octet-stream"
	deliveryMode uint8  // 0 默认 amqp.Persistent (2)
	headers      amqp.Table
	expiration   string // TTL 毫秒字符串；空串不设置
	priority     uint8
	timestamp    time.Time // zero 时不设置

	// 发布前需要执行的拓扑声明。按序执行，每个声明通过 cacheKey 去重。
	declares []declareStep
}

// buildPublishing 从 publishRequest 构建 amqp.Publishing 结构体。
func (req publishRequest) buildPublishing(msgID string) amqp.Publishing {
	contentType := req.contentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	deliveryMode := req.deliveryMode
	if deliveryMode == 0 {
		deliveryMode = amqp.Persistent
	}

	pub := amqp.Publishing{
		DeliveryMode: deliveryMode,
		ContentType:  contentType,
		MessageId:    msgID,
		Body:         req.body,
		Headers:      req.headers,
	}
	if req.expiration != "" {
		pub.Expiration = req.expiration
	}
	if req.priority != 0 {
		pub.Priority = req.priority
	}
	if !req.timestamp.IsZero() {
		pub.Timestamp = req.timestamp
	}
	return pub
}

// publishGeneric 是所有 Publish / PublishDelay / PublishWithDlx / publishRetryMessage
// 的公共实现骨架。
func (m *MQ) publishGeneric(req publishRequest) (msgID string, err error) {
	done, ok := m.trackPublish()
	if !ok {
		return "", ErrDestroyed
	}
	defer done()

	ctx := m.contextOrBackground()

	// 仅当配置了 Observer 时需要计时，减少无 Observer 场景的开销。
	var start time.Time
	if m.opt.Observer != nil {
		start = time.Now()
	}

	select {
	case <-ctx.Done():
		m.notifyPubFailed(m.failedMessage(req.body, ""))
		canceled := m.canceledError(req.operation)
		m.emitPublish(PublishEvent{
			Operation: req.operation,
			BodySize:  len(req.body),
			Duration:  time.Since(start),
			Err:       canceled,
		})
		return "", canceled
	default:
	}

	msgID = req.msgID
	if msgID == "" {
		msgID = uuid.NewString()
	}

	// 如果 publishRequest 未显式设置 deliveryMode，则从 MQOption 读取。
	if req.deliveryMode == 0 && m.opt.DeliveryMode != 0 {
		req.deliveryMode = m.opt.DeliveryMode
	}

	err = m.publishWithChannel(func(ch *amqp.Channel) error {
		for _, ds := range req.declares {
			if dErr := m.ensurePublishDeclared(ds.cacheKey, ch, ds.declare); dErr != nil {
				return dErr
			}
		}

		pub := req.buildPublishing(msgID)
		confirmation, pubErr := ch.PublishWithDeferredConfirmWithContext(
			ctx, req.exchange, req.routingKey, req.mandatory, req.immediate, pub,
		)
		if pubErr != nil {
			return pubErr
		}

		return waitForDeferredConfirm(ctx, confirmation)
	})

	m.emitPublish(PublishEvent{
		Operation: req.operation,
		MessageID: msgID,
		BodySize:  len(req.body),
		Duration:  time.Since(start),
		Err:       err,
	})
	if err != nil {
		m.notifyPubFailed(m.failedMessage(req.body, msgID))
		return msgID, err
	}

	return msgID, nil
}

// batchPublishGeneric 是所有 BatchPublish 的公共实现。
// 在一个 channel 上快速发送多条消息，然后一次性等待所有确认。
func (m *MQ) batchPublishGeneric(exchange, routingKey string, bodies [][]byte) ([]string, error) {
	if len(bodies) == 0 {
		return nil, nil
	}

	done, ok := m.trackPublish()
	if !ok {
		return nil, ErrDestroyed
	}
	defer done()

	ctx := m.contextOrBackground()

	var start time.Time
	if m.opt.Observer != nil {
		start = time.Now()
	}

	select {
	case <-ctx.Done():
		m.notifyPubFailed(m.failedMessage(nil, ""))
		return nil, m.canceledError("publish batch")
	default:
	}

	msgIDs := make([]string, len(bodies))
	err := m.publishWithChannel(func(ch *amqp.Channel) error {
		confs := make([]*amqp.DeferredConfirmation, len(bodies))

		for i, body := range bodies {
			msgID := uuid.NewString()
			msgIDs[i] = msgID

			pub := amqp.Publishing{
				DeliveryMode: m.resolveDeliveryMode(),
				ContentType:  "application/octet-stream",
				MessageId:    msgID,
				Body:         body,
				Headers:      amqp.Table{"x-retry": int32(0)},
			}

			conf, pubErr := ch.PublishWithDeferredConfirmWithContext(
				ctx, exchange, routingKey, true, false, pub,
			)
			if pubErr != nil {
				return pubErr
			}
			confs[i] = conf
		}

		for i, conf := range confs {
			if conf == nil {
				continue
			}
			acked, cErr := conf.WaitContext(ctx)
			if cErr != nil {
				m.notifyPubFailed(m.failedMessage(bodies[i], msgIDs[i]))
				return cErr
			}
			if !acked {
				m.notifyPubFailed(m.failedMessage(bodies[i], msgIDs[i]))
				return fmt.Errorf("msg %q: %w", msgIDs[i], ErrPublishNotAcknowledged)
			}
		}
		return nil
	})

	if err != nil {
		m.emitPublish(PublishEvent{
			Operation: "publish batch",
			BodySize:  totalSizeBytes(bodies),
			Duration:  time.Since(start),
			Err:       err,
		})
		return msgIDs, err
	}

	m.emitPublish(PublishEvent{
		Operation: "publish batch",
		BodySize:  totalSizeBytes(bodies),
		Duration:  time.Since(start),
	})
	return msgIDs, nil
}

// totalSizeBytes 计算 [][]byte 的总字节数。
func totalSizeBytes(bodies [][]byte) int {
	total := 0
	for _, b := range bodies {
		total += len(b)
	}
	return total
}

// resolveDeliveryMode 返回当前实例的有效投递模式。
func (m *MQ) resolveDeliveryMode() uint8 {
	if m == nil {
		return amqp.Persistent
	}
	if m.opt.DeliveryMode != 0 {
		return m.opt.DeliveryMode
	}
	return amqp.Persistent
}

// BatchPublish 批量发布多条消息到一个目标（队列或 exchange）。
func (m *MQ) BatchPublish(bodies [][]byte) ([]string, error) {
	return m.batchPublishGeneric(m.opt.ExchangeName, m.opt.RoutingKey, bodies)
}

// ParseURI 解析 RabbitMQ 连接串。
func ParseURI(uri string) (amqp.URI, error) {
	return amqp.ParseURI(uri)
}
