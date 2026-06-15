package rabbit

import (
	"errors"
	"maps"
	"strings"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

var _ MQInterface = (*MQHeaders)(nil)
var _ Publisher = (*MQHeaders)(nil)
var _ Consumer = (*MQHeaders)(nil)
var _ Retrier = (*MQHeaders)(nil)

// HeaderBinding 描述 headers exchange 绑定时需要的匹配条件。
// MatchAll=true 对应 x-match=all（需全部匹配），MatchAll=false 对应 x-match=any（任一匹配）。
type HeaderBinding struct {
	MatchAll bool
	Headers  map[string]any
}

// bindingArgs 将 HeaderBinding 转换为 amqp.Table，包含 x-match 和所有键值对。
func (b HeaderBinding) bindingArgs() amqp.Table {
	args := make(amqp.Table, len(b.Headers)+1)
	for k, v := range b.Headers {
		args[k] = v
	}
	if b.MatchAll {
		args["x-match"] = "all"
	} else {
		args["x-match"] = "any"
	}
	return args
}

// MQHeaders 是 headers exchange 模式的客户端。
// 发布端通过 PublishWithHeaders 指定路由使用的 headers，绑定端通过 HeaderBinding 定义匹配规则。
type MQHeaders struct {
	*MQ
	binding HeaderBinding
}

// newMQHeadersFromConfig 用预构建的 option 创建 MQHeaders。
func newMQHeadersFromConfig(exchangeName string, binding HeaderBinding, option MQOption) (*MQHeaders, error) {
	exchangeName = strings.TrimSpace(exchangeName)
	if exchangeName == "" {
		return nil, errors.New("exchange name is required")
	}

	option.ExchangeName = exchangeName
	var err error
	option, err = normalizeOption(option)
	if err != nil {
		return nil, err
	}

	core, err := newMQ(option, "headers")
	if err != nil {
		return nil, err
	}

	return &MQHeaders{MQ: core, binding: binding}, nil
}

// newMQHeaders 校验 exchangeName + 归一化 Option，构造一个 MQHeaders。
func newMQHeaders(exchangeName string, binding HeaderBinding, mqURL string, opts ...Option) (*MQHeaders, error) {
	option, err := newOption(mqURL, opts...)
	if err != nil {
		return nil, err
	}
	return newMQHeadersFromConfig(exchangeName, binding, option)
}

// NewHeaders 创建 headers 模式实例，可用于发布和消费。
// cfg 应通过 NewConfig 预先构建。消费端通过 binding 指定 headers 匹配规则。
func NewHeaders(exchangeName string, binding HeaderBinding, cfg MQOption) (*MQHeaders, error) {
	return newMQHeadersFromConfig(exchangeName, binding, cfg)
}

// NewPubHeaders 创建 headers 模式发布端实例。
// 发布端不关心 binding，传零值即可。
func NewPubHeaders(exchangeName, mqURL string, opts ...Option) (*MQHeaders, error) {
	return newMQHeaders(exchangeName, HeaderBinding{}, mqURL, opts...)
}

// NewConsumeHeaders 创建 headers 模式消费端实例。
func NewConsumeHeaders(exchangeName string, binding HeaderBinding, mqURL string, opts ...Option) (*MQHeaders, error) {
	return newMQHeaders(exchangeName, binding, mqURL, opts...)
}

// MustNewHeaders 创建 headers 模式实例，失败时 panic。
func MustNewHeaders(exchangeName string, binding HeaderBinding, cfg MQOption) *MQHeaders {
	mq, err := NewHeaders(exchangeName, binding, cfg)
	if err != nil {
		panic(err)
	}
	return mq
}

// PublishString 是 PublishWithHeaders 的便捷包装（不带额外 headers），
// 自动将 string 转为 []byte。
func (h *MQHeaders) PublishString(msg string) (string, error) {
	return h.Publish([]byte(msg))
}

// Publish 向 headers exchange 发布一条消息（不带额外路由 headers）。
func (h *MQHeaders) Publish(body []byte) (string, error) {
	return h.PublishWithHeaders(body, nil)
}

// PublishWithHeaders 向 headers exchange 发布一条消息，并设置路由用的 headers。
// headers 中的键值对会被用于与消费端绑定的 HeaderBinding 进行匹配。
func (h *MQHeaders) PublishWithHeaders(body []byte, headers amqp.Table) (string, error) {
	pubHeaders := make(amqp.Table)
	maps.Copy(pubHeaders, headers)
	pubHeaders["x-retry"] = int32(0)

	return h.publishGeneric(publishRequest{
		operation: "publish",
		body:      body,
		exchange:  h.opt.ExchangeName,
		headers:   pubHeaders,
		declares: []declareStep{
			{cacheKey: "exchange", declare: h.declareExchange},
		},
	})
}

// Consume 持续消费 headers 模式消息，失败自动走 retry queue。
func (h *MQHeaders) Consume(handler MsgHandler) error {
	return h.runConsumer(handler, consumerConfig{
		operation: "consume",
		logTag:    "headers consumer",
		declare: func(ch *amqp.Channel) (amqp.Queue, error) {
			return h.declareBoundQueue(ch, nil)
		},
		onDelivery: func(msg amqp.Delivery, hdlr MsgHandler) error {
			return h.handleDeliveryWithRetry(msg, hdlr, h.publishRetryMessage)
		},
	})
}

// RetryMsg 将当前 delivery 手动发送到 retry queue。
func (h *MQHeaders) RetryMsg(msg amqp.Delivery, ttl time.Duration) error {
	done, ok := h.trackPublish()
	if !ok {
		return ErrDestroyed
	}
	defer done()

	headers := copyHeaders(msg.Headers)
	return h.publishRetryMessage(msg, headers, ttl)
}

// PublishDelayString 是 PublishDelay 的便捷包装。
func (h *MQHeaders) PublishDelayString(msg string, ttl time.Duration) (string, error) {
	return h.PublishDelay([]byte(msg), ttl)
}

// PublishDelay 发布一条延迟消息（毫秒精度）。
func (h *MQHeaders) PublishDelay(body []byte, ttl time.Duration) (string, error) {
	delayQueue := h.baseName() + ".delay"
	return h.publishGeneric(publishRequest{
		operation:  "publish delay",
		body:       body,
		exchange:   h.opt.ExchangeName,
		routingKey: delayQueue,
		mandatory:  true,
		expiration: ttlToString(ttl),
		headers:    amqp.Table{"x-retry": int32(0)},
		declares: []declareStep{
			{cacheKey: "exchange", declare: h.declareExchange},
			{cacheKey: "delay:" + delayQueue, declare: func(ch *amqp.Channel) error {
				_, err := ch.QueueDeclare(
					delayQueue, true, false, false, false,
					amqp.Table{
						"x-dead-letter-exchange": h.opt.ExchangeName,
						"x-max-priority":         simpleQueueMaxPriority,
					},
				)
				return err
			}},
		},
	})
}

// ConsumeDelay 等价于 Consume（延迟消息最终落回正常队列）。
func (h *MQHeaders) ConsumeDelay(handler MsgHandler) error {
	return h.Consume(handler)
}

// PublishWithDlxString 是 PublishWithDlx 的字符串便捷包装。
func (h *MQHeaders) PublishWithDlxString(msg string) (string, error) {
	return h.PublishWithDlx([]byte(msg))
}

// PublishWithDlx 在声明死信拓扑后向主 exchange 发布消息。
func (h *MQHeaders) PublishWithDlx(body []byte) (string, error) {
	headers := amqp.Table{"x-retry": int32(0)}

	return h.publishGeneric(publishRequest{
		operation: "publish with dlx",
		body:      body,
		exchange:  h.opt.ExchangeName,
		headers:   headers,
		declares: []declareStep{
			{cacheKey: "exchange", declare: h.declareExchange},
			{cacheKey: "dlx-topology", declare: func(ch *amqp.Channel) error {
				_, _, err := h.declareDLXTopology(ch)
				return err
			}},
		},
	})
}

// BatchPublish 批量发布消息到 headers exchange。
func (h *MQHeaders) BatchPublish(bodies [][]byte) ([]string, error) {
	return h.batchPublishGeneric(h.opt.ExchangeName, "", bodies)
}

// ConsumeFailToDlx 消费主队列，并在业务处理失败时直接转入死信队列。
func (h *MQHeaders) ConsumeFailToDlx(handler MsgHandler) error {
	return h.runConsumer(handler, consumerConfig{
		operation: "consume fail-to-dlx",
		logTag:    "headers fail-to-dlx",
		declare: func(ch *amqp.Channel) (amqp.Queue, error) {
			q, _, err := h.declareDLXTopology(ch)
			return q, err
		},
		onDelivery: h.handleDeliveryFailToDLX,
	})
}

// ConsumeDlx 持续消费 headers 模式的死信队列。
func (h *MQHeaders) ConsumeDlx(handler MsgHandler) error {
	return h.runConsumer(handler, consumerConfig{
		operation: "consume dlx",
		logTag:    "headers dlx",
		declare: func(ch *amqp.Channel) (amqp.Queue, error) {
			_, dlqName, err := h.declareDLXTopology(ch)
			if err != nil {
				return amqp.Queue{}, err
			}
			return amqp.Queue{Name: dlqName}, nil
		},
		onDelivery: h.handleDeliveryDLQ,
	})
}

// declareExchange 声明 headers 类型的业务 exchange。
func (h *MQHeaders) declareExchange(ch *amqp.Channel) error {
	return declareExchangeWithArgs(ch, h.opt.ExchangeName, amqp.ExchangeHeaders, h.opt.ExchangeArgs)
}

// declareBoundQueue 声明主队列并把它绑定到业务 exchange。
// binding 决定了哪些 headers 能路由到此队列。
func (h *MQHeaders) declareBoundQueue(ch *amqp.Channel, args amqp.Table) (amqp.Queue, error) {
	if err := h.declareExchange(ch); err != nil {
		return amqp.Queue{}, err
	}

	queueArgs := h.opt.QueueArgs
	if queueArgs == nil {
		queueArgs = make(amqp.Table)
	}
	queueArgs["x-max-priority"] = simpleQueueMaxPriority
	maps.Copy(queueArgs, args)

	queue, err := ch.QueueDeclare(h.opt.QueueName, true, false, false, false, queueArgs)
	if err != nil {
		return amqp.Queue{}, err
	}

	bindingArgs := h.binding.bindingArgs()
	if bindErr := ch.QueueBind(queue.Name, "", h.opt.ExchangeName, false, bindingArgs); bindErr != nil {
		return amqp.Queue{}, bindErr
	}

	return queue, nil
}

// declareDLXTopology 声明 headers 模式的死信拓扑：
// <exchange>.dlx exchange（与主 exchange 同类型）+ <base>.dlq 死信队列。
func (h *MQHeaders) declareDLXTopology(ch *amqp.Channel) (amqp.Queue, string, error) {
	dlxExchange := h.opt.ExchangeName + ".dlx"

	if err := ch.ExchangeDeclare(dlxExchange, amqp.ExchangeHeaders, true, false, false, false, nil); err != nil {
		return amqp.Queue{}, "", err
	}

	queue, err := h.declareBoundQueue(ch, amqp.Table{
		"x-dead-letter-exchange": dlxExchange,
	})
	if err != nil {
		return amqp.Queue{}, "", err
	}

	dlqName := h.baseName() + ".dlq"
	if _, declareErr := ch.QueueDeclare(dlqName, true, false, false, false, nil); declareErr != nil {
		return amqp.Queue{}, "", declareErr
	}

	if bindErr := ch.QueueBind(dlqName, "", dlxExchange, false, nil); bindErr != nil {
		return amqp.Queue{}, "", bindErr
	}

	return queue, dlqName, nil
}

// publishRetryMessage 把当前 delivery 投递到 retry queue（带 TTL）。
func (h *MQHeaders) publishRetryMessage(msg amqp.Delivery, headers amqp.Table, ttl time.Duration) error {
	retryQueue := h.baseName() + ".retry"
	messageID := msg.MessageId
	if messageID == "" {
		messageID = uuid.NewString()
	}

	// 保留原始 headers 用于路由，追加 x-retry
	routingHeaders := copyHeaders(msg.Headers)
	maps.Copy(routingHeaders, headers)

	_, err := h.publishGeneric(publishRequest{
		operation:   "publish retry",
		body:        msg.Body,
		msgID:       messageID,
		exchange:    h.opt.ExchangeName,
		routingKey:  retryQueue,
		mandatory:   true,
		contentType: msg.ContentType,
		headers:     routingHeaders,
		expiration:  ttlToString(ttl),
		priority:    msg.Priority,
		timestamp:   time.Now(),
		declares: []declareStep{
			{cacheKey: "exchange", declare: h.declareExchange},
			{cacheKey: "retry:" + retryQueue, declare: func(ch *amqp.Channel) error {
				_, err := ch.QueueDeclare(
					retryQueue, true, false, false, false,
					amqp.Table{
						"x-dead-letter-exchange": h.opt.ExchangeName,
						"x-max-priority":         simpleQueueMaxPriority,
					},
				)
				return err
			}},
		},
	})
	return err
}

// baseName 派生 retry / delay / dlq 队列的命名前缀。
func (h *MQHeaders) baseName() string {
	if strings.TrimSpace(h.opt.QueueName) != "" {
		return safeNamePart(h.opt.QueueName)
	}
	return safeNamePart(h.opt.ExchangeName)
}
