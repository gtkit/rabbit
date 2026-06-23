package rabbit

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// maxBackoffDelay 是消费循环重连退避的上限。
	maxBackoffDelay = 30 * time.Second
	// idleAfterClose 是 channel 关闭后等待重连的间隔。
	idleAfterClose = 1 * time.Second
	// returnNotifyBuffer 是 publish channel 上 NotifyReturn 通道的缓冲大小。
	// 发布在 pubMu 下串行，且每次发布后立即排空，缓冲只需吸收瞬时退回，取小值即可。
	returnNotifyBuffer = 16
)

// nameSanitizer 把 AMQP 路由 / 队列名里不友好的字符替换掉，
// 用于派生 retry / delay / dlq 队列名。
var nameSanitizer = strings.NewReplacer(
	" ", "_",
	"/", "_",
	"\\", "_",
	":", "_",
	"*", "star",
	"#", "hash",
)

// ttlToString 把 time.Duration 转换为 amqp 期望的毫秒字符串。
// 非正值返回空串（表示禁用 TTL）。
func ttlToString(d time.Duration) string {
	if d <= 0 {
		return ""
	}

	return strconv.FormatInt(d.Milliseconds(), 10)
}

// delayMillis 把延迟时长转换为毫秒整数（用于 x-message-ttl / x-delay）。
// 非正值返回 0，表示立即投递。
func delayMillis(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	return int(d.Milliseconds())
}

// clampInt32 把任意 int64 收敛到 int32 范围。
func clampInt32(v int64) int32 {
	switch {
	case v > math.MaxInt32:
		return math.MaxInt32
	case v < math.MinInt32:
		return math.MinInt32
	default:
		return int32(v)
	}
}

// retryCount 从 AMQP headers 中读出 x-retry 值（兼容 broker 可能用的多种数值类型）。
func retryCount(headers amqp.Table) int32 {
	if headers == nil {
		return 0
	}

	value, ok := headers["x-retry"]
	if !ok {
		return 0
	}

	switch typed := value.(type) {
	case int:
		return clampInt32(int64(typed))
	case int8:
		return int32(typed)
	case int16:
		return int32(typed)
	case int32:
		return typed
	case int64:
		return clampInt32(typed)
	case uint8:
		return int32(typed)
	case uint16:
		return int32(typed)
	case uint32:
		return clampInt32(int64(typed))
	case uint64:
		if typed > math.MaxInt32 {
			return math.MaxInt32
		}
		return int32(typed)
	default:
		return 0
	}
}

// dial 用当前 opt 中的 vhost / heartbeat / TLS 建立 AMQP connection。
func (m *MQ) dial() (*amqp.Connection, error) {
	if m == nil {
		return nil, ErrNotInitialized
	}

	if m.opt.TLSConfig != nil {
		maybeLogTLSWarning(m.opt.MQURL)
	}

	config := amqp.Config{
		Vhost:           m.opt.Vhost,
		Properties:      amqp.NewConnectionProperties(),
		Heartbeat:       m.opt.Heartbeat,
		Locale:          "en_US",
		TLSClientConfig: m.opt.TLSConfig,
	}
	config.Properties.SetClientConnectionName(m.opt.ConnName)

	return amqp.DialConfig(m.opt.MQURL, config)
}

// watchBlocked 在配置了 BlockObserver 时，注册 conn 的 NotifyBlocked，
// 并启动 goroutine 把 broker 的阻塞 / 恢复事件投递给 Observer。
// goroutine 随连接关闭（NotifyBlocked 通道关闭）自动退出，不会泄漏。
func (m *MQ) watchBlocked(conn *amqp.Connection) {
	if conn == nil || m.opt.Observer == nil {
		return
	}
	if _, ok := m.opt.Observer.(BlockObserver); !ok {
		return
	}

	blockings := conn.NotifyBlocked(make(chan amqp.Blocking, 1))
	go func() {
		for b := range blockings {
			m.emitBlocked(BlockedEvent{Blocked: b.Active, Reason: b.Reason})
		}
	}()
}

// publisherConn 返回发布所用连接：隔离模式（WithPublisherConnection）下使用独立
// pubConn，否则复用共享 conn。返回前确保连接已建立。
func (m *MQ) publisherConn() (*amqp.Connection, error) {
	if !m.opt.isolatePublisher {
		if err := m.reconnect(); err != nil {
			return nil, err
		}
		return m.getConn(), nil
	}
	return m.reconnectPublisher()
}

// reconnectPublisher 在隔离模式下按需建立 / 重建发布专用连接。
func (m *MQ) reconnectPublisher() (*amqp.Connection, error) {
	if m == nil {
		return nil, ErrNotInitialized
	}

	m.pubConnMu.Lock()
	defer m.pubConnMu.Unlock()

	if m.pubConn != nil && !m.pubConn.IsClosed() {
		return m.pubConn, nil
	}

	conn, err := m.dial()
	if err != nil {
		return nil, err
	}

	stale := m.pubConn
	m.pubConn = conn

	// 旧连接上的 publish channel 失效，清理待重建。
	m.pubMu.Lock()
	if m.pubCh != nil {
		_ = m.pubCh.Close()
		m.pubCh = nil
	}
	m.pubDecls = nil
	m.pubReturns = nil
	m.pubMu.Unlock()

	if stale != nil && !stale.IsClosed() {
		_ = stale.Close()
	}

	m.watchBlocked(conn)
	return conn, nil
}

// reconnect 在 connection 已断开时重新拨号。
// 同时把旧的 publish channel 和声明缓存清掉，让下一次 publishWithChannel 重新初始化。
func (m *MQ) reconnect() error {
	if m == nil {
		return ErrNotInitialized
	}

	m.connMu.Lock()
	defer m.connMu.Unlock()

	if m.conn != nil && !m.conn.IsClosed() {
		return nil
	}

	conn, err := m.dial()
	if err != nil {
		return err
	}

	staleConn := m.conn
	m.conn = conn

	// 非隔离模式下 publish channel 建在共享连接上，重连需一并清理重建；
	// 隔离模式下 publish channel 属于 pubConn，由 reconnectPublisher 负责，不在此处动。
	if !m.opt.isolatePublisher {
		m.pubMu.Lock()
		if m.pubCh != nil {
			_ = m.pubCh.Close()
			m.pubCh = nil
		}
		m.pubDecls = nil
		m.pubReturns = nil
		m.pubMu.Unlock()
	}

	if staleConn != nil && !staleConn.IsClosed() {
		_ = staleConn.Close()
	}

	m.watchBlocked(conn)
	return nil
}

// getConn 在 connMu 保护下获取当前 connection 引用。
func (m *MQ) getConn() *amqp.Connection {
	m.connMu.Lock()
	defer m.connMu.Unlock()

	return m.conn
}

// openConsumerChannel 为消费循环打开一个新 channel，并按 opt.PrefetchCount 设置 Qos。
func (m *MQ) openConsumerChannel() (*amqp.Channel, error) {
	if err := m.reconnect(); err != nil {
		return nil, err
	}

	conn := m.getConn()
	if conn == nil {
		return nil, ErrConnectionNotInitialized
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	prefetch := m.opt.PrefetchCount
	if prefetch <= 0 {
		prefetch = defaultPrefetchCount
	}
	if qosErr := ch.Qos(prefetch, 0, false); qosErr != nil {
		_ = ch.Close()
		return nil, qosErr
	}

	return ch, nil
}

// closePublishChannel 关闭复用的 publish channel 并清空声明缓存。
func (m *MQ) closePublishChannel() {
	m.pubMu.Lock()
	defer m.pubMu.Unlock()

	if m.pubCh == nil {
		m.pubDecls = nil
		m.pubReturns = nil
		return
	}

	_ = m.pubCh.Close()
	m.pubCh = nil
	m.pubDecls = nil
	m.pubReturns = nil
}

// drainReturns 非阻塞地排空 pubReturns 中已到达的退回消息。
// 调用方必须持有 pubMu。返回本次排空到的所有 basic.return。
func (m *MQ) drainReturns() []amqp.Return {
	if m.pubReturns == nil {
		return nil
	}

	var out []amqp.Return
	for {
		select {
		case r, ok := <-m.pubReturns:
			if !ok {
				return out
			}
			out = append(out, r)
		default:
			return out
		}
	}
}

// closeAMQPChannel 是 nil 安全的 channel.Close 包装。
func closeAMQPChannel(ch *amqp.Channel) {
	if ch == nil {
		return
	}

	_ = ch.Close()
}

// waitForDeferredConfirm 阻塞等待 confirm；broker 回复 nack 时返回 ErrPublishNotAcknowledged。
func waitForDeferredConfirm(ctx context.Context, confirmation *amqp.DeferredConfirmation) error {
	if confirmation == nil {
		return nil
	}

	acked, err := confirmation.WaitContext(ctx)
	if err != nil {
		return err
	}

	if !acked {
		return ErrPublishNotAcknowledged
	}

	return nil
}

// copyHeaders 深拷一份 headers，避免修改时影响原 delivery。
func copyHeaders(headers amqp.Table) amqp.Table {
	if len(headers) == 0 {
		return amqp.Table{}
	}

	cloned := make(amqp.Table, len(headers))
	maps.Copy(cloned, headers)

	return cloned
}

// maybeLogTLSWarning 在提供了 TLS 配置但连接串未使用 amqps:// 时输出警告。
func maybeLogTLSWarning(mqURL string) {
	if strings.HasPrefix(mqURL, "amqps://") {
		return
	}

	scheme := extractURLScheme(mqURL)
	currentLogger().Infof("TLSConfig provided but connection URL uses %q (not amqps://); "+
		"TLS may not be active. Use amqps:// scheme for encrypted connections.", scheme)
}

// extractURLScheme 提取 URL 中的 scheme 部分，便于日志输出。
func extractURLScheme(rawURL string) string {
	idx := strings.Index(rawURL, "://")
	if idx <= 0 {
		return rawURL
	}
	return rawURL[:idx+3]
}

// declareExchangeWithArgs 声明 exchange，支持传递额外参数。
func declareExchangeWithArgs(ch *amqp.Channel, name, kind string, args amqp.Table) error {
	return ch.ExchangeDeclare(name, kind, true, false, false, false, args)
}

// safeNamePart 把 exchange / routing key 等转换为可用作派生队列名的字符串。
// 空值统一返回 "default" 避免拼接出 ".dlq" / ".retry" 这种异常名字。
func safeNamePart(value string) string {
	if strings.TrimSpace(value) == "" {
		return "default"
	}

	return nameSanitizer.Replace(value)
}

// publishWithChannel 在 pubMu 保护下复用 confirm channel 执行一次 publish。
// channel 不可用时会自动重建。
func (m *MQ) publishWithChannel(fn func(*amqp.Channel) error) error {
	conn, err := m.publisherConn()
	if err != nil {
		return err
	}
	if conn == nil {
		return ErrConnectionNotInitialized
	}

	m.pubMu.Lock()
	defer m.pubMu.Unlock()

	if m.pubCh == nil || m.pubCh.IsClosed() {
		ch, err := conn.Channel()
		if err != nil {
			return err
		}

		if confirmErr := ch.Confirm(false); confirmErr != nil {
			_ = ch.Close()
			return confirmErr
		}

		m.pubCh = ch
		m.pubDecls = make(map[string]struct{})
		m.pubReturns = ch.NotifyReturn(make(chan amqp.Return, returnNotifyBuffer))
	}

	if err := fn(m.pubCh); err != nil {
		if m.pubCh != nil && m.pubCh.IsClosed() {
			m.pubCh = nil
			m.pubDecls = nil
			m.pubReturns = nil
		}
		return err
	}

	return nil
}

// retryBackoffDelay 给消费循环重连提供指数退避，封顶 maxBackoffDelay。
func retryBackoffDelay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}

	delay := time.Second * time.Duration(1<<attempt)
	if delay > maxBackoffDelay {
		return maxBackoffDelay
	}

	return delay
}

// waitRetry 打印一条 Info 日志并阻塞直到退避结束或 ctx 取消。
// 同时投递 ReconnectEvent 给 Observer。
func (m *MQ) waitRetry(operation string, attempt *int, cause error, format string, args ...any) error {
	delay := retryBackoffDelay(0)
	current := 0
	if attempt != nil {
		current = *attempt
		delay = retryBackoffDelay(current)
		*attempt++
	}

	m.logger().Infof(format, args...)
	m.emitReconnect(ReconnectEvent{
		Operation: operation,
		Attempt:   current,
		Err:       cause,
	})

	select {
	case <-m.contextOrBackground().Done():
		return m.canceledError(operation)
	case <-time.After(delay):
		return nil
	}
}

// maxRetry 返回当前实例的最大重试次数（默认 3）。
func (m *MQ) maxRetry() int32 {
	if m == nil || m.opt.MaxRetry <= 0 {
		return defaultMaxRetry
	}

	return m.opt.MaxRetry
}

// ensurePublishDeclared 在同一个 publish channel 上对 key 做一次拓扑声明。
// 后续同 channel 内不会重复声明（直到 channel 被重建）。
func (m *MQ) ensurePublishDeclared(key string, ch *amqp.Channel, declare func(*amqp.Channel) error) error {
	if key == "" {
		return declare(ch)
	}

	if _, ok := m.pubDecls[key]; ok {
		return nil
	}

	if err := declare(ch); err != nil {
		return err
	}

	if m.pubDecls == nil {
		m.pubDecls = make(map[string]struct{})
	}
	m.pubDecls[key] = struct{}{}

	return nil
}

// retryTTL 返回当前实例的 retry queue 停留时长（默认 2s）。
func (m *MQ) retryTTL() time.Duration {
	if m == nil || m.opt.RetryTTL <= 0 {
		return defaultRetryTTL
	}

	return m.opt.RetryTTL
}

// retryPublisher 是把消息重新投递到 retry queue 的策略函数。
type retryPublisher func(msg amqp.Delivery, headers amqp.Table, ttl time.Duration) error

// handleDeliveryWithRetry 处理消费失败时走 retry queue 的逻辑（simple / direct / topic）。
// 会向 Observer 投递 ConsumeEvent（含本次 retry 计数）。
func (m *MQ) handleDeliveryWithRetry(msg amqp.Delivery, handler MsgHandler, pub retryPublisher) error {
	start := time.Now()
	retry := retryCount(msg.Headers)

	select {
	case <-m.contextOrBackground().Done():
		m.notifyConsumeFailed(handler, m.failedMessage(msg.Body, msg.MessageId))
		err := msg.Reject(false)
		canceled := m.canceledError("consume")
		m.emitConsume(ConsumeEvent{
			Operation: "consume",
			MessageID: msg.MessageId,
			BodySize:  len(msg.Body),
			Duration:  time.Since(start),
			Retry:     retry,
			Err:       canceled,
		})
		if err != nil {
			return err
		}
		return canceled
	default:
	}

	processErr := m.safeProcess(handler, msg.Body, msg.MessageId)
	m.emitConsume(ConsumeEvent{
		Operation: "consume",
		MessageID: msg.MessageId,
		BodySize:  len(msg.Body),
		Duration:  time.Since(start),
		Retry:     retry,
		Err:       processErr,
	})
	if processErr != nil {
		if retry >= m.maxRetry() {
			m.notifyConsumeFailed(handler, m.failedMessage(msg.Body, msg.MessageId))
			return msg.Reject(false)
		}

		headers := copyHeaders(msg.Headers)
		headers["x-retry"] = retry + 1

		if pubErr := pub(msg, headers, m.retryTTL()); pubErr != nil {
			if nackErr := msg.Nack(false, true); nackErr != nil {
				return fmt.Errorf("publish retry message: %w", errors.Join(pubErr, nackErr))
			}
			return pubErr
		}

		return m.ackAfterRetryPublish(msg)
	}

	return msg.Ack(false)
}

// handleDeliveryNoRetry 处理失败时直接拒绝的逻辑（fanout 的普通 Consume）。
func (m *MQ) handleDeliveryNoRetry(msg amqp.Delivery, handler MsgHandler) error {
	start := time.Now()

	select {
	case <-m.contextOrBackground().Done():
		m.notifyConsumeFailed(handler, m.failedMessage(msg.Body, msg.MessageId))
		err := msg.Reject(false)
		canceled := m.canceledError("consume")
		m.emitConsume(ConsumeEvent{
			Operation: "consume",
			MessageID: msg.MessageId,
			BodySize:  len(msg.Body),
			Duration:  time.Since(start),
			Err:       canceled,
		})
		if err != nil {
			return err
		}
		return canceled
	default:
	}

	processErr := m.safeProcess(handler, msg.Body, msg.MessageId)
	m.emitConsume(ConsumeEvent{
		Operation: "consume",
		MessageID: msg.MessageId,
		BodySize:  len(msg.Body),
		Duration:  time.Since(start),
		Err:       processErr,
	})
	if processErr != nil {
		m.notifyConsumeFailed(handler, m.failedMessage(msg.Body, msg.MessageId))
		return msg.Reject(false)
	}

	return msg.Ack(false)
}

// handleDeliveryFailToDLX 处理 ConsumeFailToDlx 模式：失败 → reject 进 DLX。
func (m *MQ) handleDeliveryFailToDLX(msg amqp.Delivery, handler MsgHandler) error {
	start := time.Now()

	select {
	case <-m.contextOrBackground().Done():
		err := msg.Reject(false)
		canceled := m.canceledError("consume fail-to-dlx")
		m.emitConsume(ConsumeEvent{
			Operation: "consume fail-to-dlx",
			MessageID: msg.MessageId,
			BodySize:  len(msg.Body),
			Duration:  time.Since(start),
			Err:       canceled,
		})
		if err != nil {
			return err
		}
		return canceled
	default:
	}

	processErr := m.safeProcess(handler, msg.Body, msg.MessageId)
	m.emitConsume(ConsumeEvent{
		Operation: "consume fail-to-dlx",
		MessageID: msg.MessageId,
		BodySize:  len(msg.Body),
		Duration:  time.Since(start),
		Err:       processErr,
	})
	if processErr != nil {
		m.notifyConsumeFailed(handler, m.failedMessage(msg.Body, msg.MessageId))
		return msg.Reject(false)
	}

	return msg.Ack(false)
}

// handleDeliveryDLQ 处理 ConsumeDlx 模式：失败 → nack 重新入队 DLQ。
func (m *MQ) handleDeliveryDLQ(msg amqp.Delivery, handler MsgHandler) error {
	start := time.Now()

	select {
	case <-m.contextOrBackground().Done():
		err := msg.Reject(false)
		canceled := m.canceledError("consume dlx")
		m.emitConsume(ConsumeEvent{
			Operation: "consume dlx",
			MessageID: msg.MessageId,
			BodySize:  len(msg.Body),
			Duration:  time.Since(start),
			Err:       canceled,
		})
		if err != nil {
			return err
		}
		return canceled
	default:
	}

	processErr := m.safeProcess(handler, msg.Body, msg.MessageId)
	m.emitConsume(ConsumeEvent{
		Operation: "consume dlx",
		MessageID: msg.MessageId,
		BodySize:  len(msg.Body),
		Duration:  time.Since(start),
		Err:       processErr,
	})
	if processErr != nil {
		m.notifyConsumeFailed(handler, m.failedMessage(msg.Body, msg.MessageId))
		return msg.Nack(false, true)
	}

	return msg.Ack(false)
}

// ackAfterRetryPublish 在 retry message 发布成功后 ack 原消息。
// ack 失败仅记录日志，不退出消费循环（仍是 at-least-once 语义）。
func (m *MQ) ackAfterRetryPublish(msg amqp.Delivery) error {
	if err := msg.Ack(false); err != nil {
		m.logger().Errorf("ack after retry publish failed: %v (message may be redelivered)", err)
	}

	return nil
}

// consumerConfig 描述运行一个消费循环所需的最小信息。
// 不同模式仅通过 declare + onDelivery 注入差异。
type consumerConfig struct {
	// operation 用于日志与 cancel 错误，例如 "consume" / "consume fail-to-dlx" / "consume dlx"。
	operation string
	// logTag 用于日志前缀，例如 "simple consumer" / "direct fail-to-dlx"。
	logTag string
	// declare 在每个新 channel 上声明拓扑并返回要消费的队列。
	declare func(ch *amqp.Channel) (amqp.Queue, error)
	// onDelivery 处理单条消息。
	onDelivery func(msg amqp.Delivery, handler MsgHandler) error
	// consumeArgs 是 basic.consume 的额外参数（如 stream 的 x-stream-offset）；nil 表示无。
	consumeArgs amqp.Table
}

// runConsumer 是公共消费循环：连接 / 通道 / 声明 / 消费 / 重连。
// 出错时按指数退避重试，ctx 取消时返回。
func (m *MQ) runConsumer(handler MsgHandler, cfg consumerConfig) error {
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

		if loopErr := m.consumeLoop(ctx, ch, queue, handler, cfg); loopErr != nil {
			return loopErr
		}

		select {
		case <-ctx.Done():
			return m.canceledError(cfg.operation)
		case <-time.After(idleAfterClose):
		}
	}
}

// errAwaitRetry 是 prepareConsume 内部错误，表示已通过 waitRetry 退避，调用方应 continue。
var errAwaitRetry = errors.New("await retry")

// prepareConsume 打开 channel、声明拓扑、启动消费。
// 任一步骤失败：
//   - 永久错误（凭证 / queue type 冲突 / 权限 / access-refused 等）→ fast-fail 返回 ErrPermanent
//   - 临时错误（网络抖动 / 通道关闭等）→ channel 关闭后 continue 等下一轮重试
func (m *MQ) prepareConsume(cfg consumerConfig, attempt *int) (*amqp.Channel, amqp.Queue, error) {
	ch, err := m.openConsumerChannel()
	if err != nil {
		if isPermanent(err) {
			m.logger().Errorf("%s open channel permanent failure: %v", cfg.logTag, err)
			m.emitReconnect(ReconnectEvent{
				Operation: cfg.operation,
				Attempt:   *attempt,
				Err:       err,
				Permanent: true,
			})
			return nil, amqp.Queue{}, permanentError(err)
		}
		if waitErr := m.waitRetry(cfg.operation, attempt, err,
			"%s open channel failed: %v, reconnecting...", cfg.logTag, err); waitErr != nil {
			return nil, amqp.Queue{}, waitErr
		}
		return nil, amqp.Queue{}, errAwaitRetry
	}

	queue, declareErr := cfg.declare(ch)
	if declareErr != nil {
		closeAMQPChannel(ch)
		if isPermanent(declareErr) {
			m.logger().Errorf("%s declare topology permanent failure: %v", cfg.logTag, declareErr)
			m.emitReconnect(ReconnectEvent{
				Operation: cfg.operation,
				Attempt:   *attempt,
				Err:       declareErr,
				Permanent: true,
			})
			return nil, amqp.Queue{}, permanentError(declareErr)
		}
		if waitErr := m.waitRetry(cfg.operation, attempt, declareErr,
			"%s declare topology failed: %v, reconnecting...", cfg.logTag, declareErr); waitErr != nil {
			return nil, amqp.Queue{}, waitErr
		}
		return nil, amqp.Queue{}, errAwaitRetry
	}

	return ch, queue, nil
}

// consumeLoop 在单个 channel 上持续消费，直到 channel 关闭、ctx 取消或 deliveries 通道关闭。
// 返回 nil 表示需要外层重连；返回非 nil 表示需要外层退出。
func (m *MQ) consumeLoop(
	ctx context.Context,
	ch *amqp.Channel,
	queue amqp.Queue,
	handler MsgHandler,
	cfg consumerConfig,
) error {
	deliveries, err := ch.Consume(queue.Name, "", false, false, false, false, cfg.consumeArgs)
	if err != nil {
		closeAMQPChannel(ch)
		m.logger().Infof("%s start consume failed: %v, will reconnect", cfg.logTag, err)
		m.emitReconnect(ReconnectEvent{
			Operation: cfg.operation,
			Err:       err,
		})
		return nil
	}

	notifyClose := ch.NotifyClose(make(chan *amqp.Error, 1))

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
			return nil
		case msg, ok := <-deliveries:
			if !ok {
				_ = ch.Cancel("", false)
				closeAMQPChannel(ch)
				return nil
			}

			if deliverErr := cfg.onDelivery(msg, handler); deliverErr != nil {
				_ = ch.Cancel("", false)
				closeAMQPChannel(ch)
				return deliverErr
			}
		}
	}
}
