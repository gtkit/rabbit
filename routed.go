package rabbit

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

// routedMQ 是 direct 和 topic 模式共用的实现。
// direct/topic 仅在 exchange 类型上不同，业务方法完全一致。
type routedMQ struct {
	*MQ
	// exchangeKind 是 amqp.ExchangeDirect 或 amqp.ExchangeTopic。
	exchangeKind string
}

// newRoutedFromConfig 用预构建的 option 创建 routedMQ。
func newRoutedFromConfig(exchangeKind, exchangeName, routingKey string, option MQOption) (*routedMQ, error) {
	exchangeName = strings.TrimSpace(exchangeName)
	routingKey = strings.TrimSpace(routingKey)
	if exchangeName == "" || routingKey == "" {
		return nil, errors.New("exchange name and routing key are required")
	}

	option.ExchangeName = exchangeName
	option.RoutingKey = routingKey
	var err error
	option, err = normalizeOption(option)
	if err != nil {
		return nil, err
	}

	core, err := newMQ(option, exchangeKind)
	if err != nil {
		return nil, err
	}

	return &routedMQ{MQ: core, exchangeKind: exchangeKind}, nil
}

// newRouted 构造 direct / topic 模式共用的 routedMQ。
// exchangeKind 应为 amqp.ExchangeDirect 或 amqp.ExchangeTopic。
func newRouted(exchangeKind, exchangeName, routingKey, mqURL string, opts ...Option) (*routedMQ, error) {
	option, err := newOption(mqURL, opts...)
	if err != nil {
		return nil, err
	}
	return newRoutedFromConfig(exchangeKind, exchangeName, routingKey, option)
}

// PublishString 是 Publish 的便捷包装，自动将 string 转为 []byte。
func (r *routedMQ) PublishString(msg string) (string, error) {
	return r.Publish([]byte(msg))
}

// PublishDelayString 是 PublishDelay 的便捷包装，自动将 string 转为 []byte。
func (r *routedMQ) PublishDelayString(msg string, ttl time.Duration) (string, error) {
	return r.PublishDelay([]byte(msg), ttl)
}

// Publish 向 exchange 发布一条持久化消息，返回 messageID。
func (r *routedMQ) Publish(body []byte) (string, error) {
	return r.publishGeneric(publishRequest{
		operation:  "publish",
		body:       body,
		exchange:   r.opt.ExchangeName,
		routingKey: r.opt.RoutingKey,
		mandatory:  true,
		headers:    amqp.Table{"x-retry": int32(0)},
		declares: []declareStep{
			{cacheKey: "exchange", declare: r.declareExchange},
		},
	})
}

// Consume 持续消费 exchange 模式消息，失败自动走 retry queue。
func (r *routedMQ) Consume(handler MsgHandler) error {
	return r.runConsumer(handler, consumerConfig{
		operation: "consume",
		logTag:    r.exchangeKind + " consumer",
		declare: func(ch *amqp.Channel) (amqp.Queue, error) {
			return r.declareBoundQueue(ch, nil)
		},
		onDelivery: func(msg amqp.Delivery, h MsgHandler) error {
			return r.handleDeliveryWithRetry(msg, h, r.publishRetryMessage)
		},
	})
}

// PublishWithDlxString 是 PublishWithDlx 的字符串便捷包装。
func (r *routedMQ) PublishWithDlxString(msg string) (string, error) {
	return r.PublishWithDlx([]byte(msg))
}

// PublishWithDlx 在声明死信拓扑后向主 exchange 发布消息。
func (r *routedMQ) PublishWithDlx(body []byte) (string, error) {
	headers := amqp.Table{"x-retry": int32(0)}

	return r.publishGeneric(publishRequest{
		operation:  "publish with dlx",
		body:       body,
		exchange:   r.opt.ExchangeName,
		routingKey: r.opt.RoutingKey,
		mandatory:  true,
		headers:    headers,
		declares: []declareStep{
			{cacheKey: "exchange", declare: r.declareExchange},
			{cacheKey: "dlx-topology", declare: func(ch *amqp.Channel) error {
				_, _, err := r.declareDLXTopology(ch)
				return err
			}},
		},
	})
}

// BatchPublish 批量发布多条消息到 exchange。
func (r *routedMQ) BatchPublish(bodies [][]byte) ([]string, error) {
	return r.batchPublishGeneric(r.opt.ExchangeName, r.opt.RoutingKey, bodies)
}

// Call 发送 RPC 请求到 exchange 并等待应答。
// 请求通过 exchange + routingKey 路由到服务端；服务端将回复发回内部独占回复队列。
// 默认超时 30 秒，可通过实例的 context 设置 deadline 控制。
func (r *routedMQ) Call(body []byte) ([]byte, error) {
	return r.callGeneric(r.opt.ExchangeName, r.opt.RoutingKey, body)
}

// ServeRPC 作为 RPC 服务端持续消费主队列中的请求。
// handler 处理请求并返回应答 body，返回 error 时消息被拒绝（不回复）。
func (r *routedMQ) ServeRPC(handler RPCHandler) error {
	return r.serveRPCLoop(handler, consumerConfig{
		operation: "rpc serve",
		logTag:    r.exchangeKind + " rpc server",
		declare: func(ch *amqp.Channel) (amqp.Queue, error) {
			return r.declareBoundQueue(ch, nil)
		},
	})
}

// RetryMsg 将当前 delivery 手动发送到 retry queue。
func (r *routedMQ) RetryMsg(msg amqp.Delivery, ttl time.Duration) error {
	done, ok := r.trackPublish()
	if !ok {
		return ErrDestroyed
	}
	defer done()

	headers := copyHeaders(msg.Headers)
	return r.publishRetryMessage(msg, headers, ttl)
}

// PublishDelay 发布一条延迟消息（毫秒精度），返回 messageID。
//
// 默认按 TTL 分桶到独立 delay 队列（队列名含 TTL），不同 TTL 互不阻塞；
// 启用 WithDelayedExchange 时改走 x-delayed-message 插件 exchange。
func (r *routedMQ) PublishDelay(body []byte, ttl time.Duration) (string, error) {
	ms := delayMillis(ttl)

	if r.opt.delayedExchange {
		return r.publishWithDelayHeader(r.opt.ExchangeName, r.opt.RoutingKey, body, ms, r.declareExchange)
	}

	delayQueue := fmt.Sprintf("%s.delay.%d", r.baseName(), ms)
	return r.publishGeneric(publishRequest{
		operation:  "publish delay",
		body:       body,
		routingKey: delayQueue,
		mandatory:  true,
		headers:    amqp.Table{"x-retry": int32(0)},
		declares: []declareStep{
			{cacheKey: "exchange", declare: r.declareExchange},
			{cacheKey: "delay:" + delayQueue, declare: func(ch *amqp.Channel) error {
				_, err := ch.QueueDeclare(
					delayQueue, true, false, false, false,
					r.derivedQueueArgs(amqp.Table{
						"x-message-ttl":             ms,
						"x-dead-letter-exchange":    r.opt.ExchangeName,
						"x-dead-letter-routing-key": r.opt.RoutingKey,
					}),
				)
				return err
			}},
		},
	})
}

// ConsumeDelay 等价于 Consume（延迟消息最终落回正常队列）。
func (r *routedMQ) ConsumeDelay(handler MsgHandler) error {
	return r.Consume(handler)
}

// ConsumeFailToDlx 消费主队列，并在业务处理失败时直接转入死信队列。
func (r *routedMQ) ConsumeFailToDlx(handler MsgHandler) error {
	return r.runConsumer(handler, consumerConfig{
		operation: "consume fail-to-dlx",
		logTag:    r.exchangeKind + " fail-to-dlx",
		declare: func(ch *amqp.Channel) (amqp.Queue, error) {
			q, _, err := r.declareDLXTopology(ch)
			return q, err
		},
		onDelivery: r.handleDeliveryFailToDLX,
	})
}

// ConsumeDlx 持续消费死信队列。
func (r *routedMQ) ConsumeDlx(handler MsgHandler) error {
	return r.runConsumer(handler, consumerConfig{
		operation: "consume dlx",
		logTag:    r.exchangeKind + " dlx",
		declare: func(ch *amqp.Channel) (amqp.Queue, error) {
			_, dlqName, err := r.declareDLXTopology(ch)
			if err != nil {
				return amqp.Queue{}, err
			}
			return amqp.Queue{Name: dlqName}, nil
		},
		onDelivery: r.handleDeliveryDLQ,
	})
}

// declareExchange 声明业务 exchange。
// 启用 WithDelayedExchange 时，业务 exchange 声明为 x-delayed-message 类型
// （底层仍按 exchangeKind 路由），从而 PublishDelay 可通过 x-delay header 实现延迟。
func (r *routedMQ) declareExchange(ch *amqp.Channel) error {
	if r.opt.delayedExchange {
		return declareExchangeWithArgs(ch, r.opt.ExchangeName, delayedMessageExchangeType,
			delayedExchangeArgs(r.opt.ExchangeArgs, r.exchangeKind))
	}
	return declareExchangeWithArgs(ch, r.opt.ExchangeName, r.exchangeKind, r.opt.ExchangeArgs)
}

// declareBoundQueue 声明主队列并把它绑定到业务 exchange + routingKey 上。
// args 用于追加业务自定义参数（例如 x-dead-letter-exchange）。
func (r *routedMQ) declareBoundQueue(ch *amqp.Channel, args amqp.Table) (amqp.Queue, error) {
	if err := r.declareExchange(ch); err != nil {
		return amqp.Queue{}, err
	}

	queue, err := ch.QueueDeclare(r.opt.QueueName, true, false, false, false, r.mainQueueArgs(args))
	if err != nil {
		return amqp.Queue{}, err
	}

	if bindErr := ch.QueueBind(queue.Name, r.opt.RoutingKey, r.opt.ExchangeName, false, nil); bindErr != nil {
		return amqp.Queue{}, bindErr
	}

	return queue, nil
}

// declareDLXTopology 声明 direct / topic 模式的死信拓扑：
// <exchange>.dlx + <base>.dlq；主队列的 x-dead-letter-exchange 指向 dlx。
func (r *routedMQ) declareDLXTopology(ch *amqp.Channel) (amqp.Queue, string, error) {
	dlxExchange := r.opt.ExchangeName + ".dlx"
	dlxRouting := r.opt.RoutingKey + ".dlx"
	dlqName := r.baseName() + ".dlq"

	if err := ch.ExchangeDeclare(dlxExchange, r.exchangeKind, true, false, false, false, nil); err != nil {
		return amqp.Queue{}, "", err
	}

	queue, err := r.declareBoundQueue(ch, amqp.Table{
		"x-dead-letter-exchange":    dlxExchange,
		"x-dead-letter-routing-key": dlxRouting,
	})
	if err != nil {
		return amqp.Queue{}, "", err
	}

	if _, declareErr := ch.QueueDeclare(dlqName, true, false, false, false, nil); declareErr != nil {
		return amqp.Queue{}, "", declareErr
	}

	if bindErr := ch.QueueBind(dlqName, dlxRouting, dlxExchange, false, nil); bindErr != nil {
		return amqp.Queue{}, "", bindErr
	}

	return queue, dlqName, nil
}

// publishRetryMessage 把当前 delivery 投递到 retry queue（带 TTL）。
// retry queue 的 dead-letter 目标是业务 exchange + routingKey，TTL 到期后自动回到主队列。
func (r *routedMQ) publishRetryMessage(msg amqp.Delivery, headers amqp.Table, ttl time.Duration) error {
	retryQueue := r.baseName() + ".retry"
	messageID := msg.MessageId
	if messageID == "" {
		messageID = uuid.NewString()
	}

	_, err := r.publishGeneric(publishRequest{
		operation:   "publish retry",
		body:        msg.Body,
		msgID:       messageID,
		routingKey:  retryQueue,
		mandatory:   true,
		contentType: msg.ContentType,
		headers:     headers,
		expiration:  ttlToString(ttl),
		priority:    msg.Priority,
		timestamp:   time.Now(),
		declares: []declareStep{
			{cacheKey: "exchange", declare: r.declareExchange},
			{cacheKey: "retry:" + retryQueue, declare: func(ch *amqp.Channel) error {
				_, err := ch.QueueDeclare(
					retryQueue, true, false, false, false,
					r.derivedQueueArgs(amqp.Table{
						"x-dead-letter-exchange":    r.opt.ExchangeName,
						"x-dead-letter-routing-key": r.opt.RoutingKey,
					}),
				)
				return err
			}},
		},
	})
	return err
}

// baseName 派生 retry / delay / dlq 队列的命名前缀。
// 优先用 QueueName；未设置时退回 exchange + routingKey 的 sanitized 形式。
func (r *routedMQ) baseName() string {
	if strings.TrimSpace(r.opt.QueueName) != "" {
		return safeNamePart(r.opt.QueueName)
	}

	return safeNamePart(r.opt.ExchangeName) + "." + safeNamePart(r.opt.RoutingKey)
}
