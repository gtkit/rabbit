package rabbit

import (
	"errors"
	"maps"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var _ MQInterface = (*MQFanout)(nil)
var _ Publisher = (*MQFanout)(nil)
var _ Consumer = (*MQFanout)(nil)

// MQFanout 是 fanout exchange 模式的客户端。
// 注意：fanout 不支持自动重试，消费失败直接 Reject；fanout 也不实现 Retrier。
type MQFanout struct {
	*MQ
}

// newMQFanoutFromConfig 用预构建的 option 创建 MQFanout。
func newMQFanoutFromConfig(exchangeName string, option MQOption) (*MQFanout, error) {
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

	core, err := newMQ(option, "fanout")
	if err != nil {
		return nil, err
	}

	return &MQFanout{MQ: core}, nil
}

// newMQFanout 校验 exchangeName + 归一化 Option，构造一个 MQFanout。
func newMQFanout(exchangeName, mqURL string, opts ...Option) (*MQFanout, error) {
	option, err := newOption(mqURL, opts...)
	if err != nil {
		return nil, err
	}
	return newMQFanoutFromConfig(exchangeName, option)
}

// NewFanout 创建 fanout 模式实例，可用于发布和消费。
// cfg 应通过 NewConfig 预先构建。
func NewFanout(exchangeName string, cfg MQOption) (*MQFanout, error) {
	return newMQFanoutFromConfig(exchangeName, cfg)
}

// NewPubFanout 创建 fanout 模式发布端实例。
func NewPubFanout(exchangeName, mqURL string, opts ...Option) (*MQFanout, error) {
	return newMQFanout(exchangeName, mqURL, opts...)
}

// NewConsumeFanout 创建 fanout 模式消费端实例。
func NewConsumeFanout(exchangeName, mqURL string, opts ...Option) (*MQFanout, error) {
	return newMQFanout(exchangeName, mqURL, opts...)
}

// MustNewFanout 创建 fanout 模式实例，失败时 panic。
// 适用于初始化不可失败的场景（如测试）。
func MustNewFanout(exchangeName string, cfg MQOption) *MQFanout {
	mq, err := NewFanout(exchangeName, cfg)
	if err != nil {
		panic(err)
	}
	return mq
}

// PublishString 是 Publish 的便捷包装，自动将 string 转为 []byte。
func (f *MQFanout) PublishString(msg string) (string, error) {
	return f.Publish([]byte(msg))
}

// PublishDelayString 是 PublishDelay 的便捷包装，自动将 string 转为 []byte。
func (f *MQFanout) PublishDelayString(msg string, ttl time.Duration) (string, error) {
	return f.PublishDelay([]byte(msg), ttl)
}

// Publish 向 fanout exchange 广播一条消息，返回 messageID。
func (f *MQFanout) Publish(body []byte) (string, error) {
	return f.publishGeneric(publishRequest{
		operation: "publish",
		body:      body,
		exchange:  f.opt.ExchangeName,
		mandatory: true,
		headers:   amqp.Table{"x-retry": int32(0)},
		declares: []declareStep{
			{cacheKey: "exchange", declare: f.declareExchange},
		},
	})
}

// Consume 持续消费 fanout 模式消息。失败直接 Reject，不重试。
func (f *MQFanout) Consume(handler MsgHandler) error {
	return f.runConsumer(handler, consumerConfig{
		operation: "consume",
		logTag:    "fanout consumer",
		declare: func(ch *amqp.Channel) (amqp.Queue, error) {
			return f.declareBoundQueue(ch, nil)
		},
		onDelivery: f.handleDeliveryNoRetry,
	})
}

// PublishDelay 发布一条延迟广播消息，返回 messageID。
func (f *MQFanout) PublishDelay(body []byte, ttl time.Duration) (string, error) {
	delayQueue := f.baseName() + ".delay"
	return f.publishGeneric(publishRequest{
		operation:  "publish delay",
		body:       body,
		routingKey: delayQueue,
		mandatory:  true,
		expiration: ttlToString(ttl),
		headers:    amqp.Table{"x-retry": int32(0)},
		declares: []declareStep{
			{cacheKey: "exchange", declare: f.declareExchange},
			{cacheKey: "delay:" + delayQueue, declare: func(ch *amqp.Channel) error {
				_, err := ch.QueueDeclare(
					delayQueue, true, false, false, false,
					amqp.Table{
						"x-dead-letter-exchange": f.opt.ExchangeName,
						"x-max-priority":         simpleQueueMaxPriority,
					},
				)
				return err
			}},
		},
	})
}

// ConsumeDelay 在 fanout 模式下等价于 Consume。
func (f *MQFanout) ConsumeDelay(handler MsgHandler) error {
	return f.Consume(handler)
}

// ConsumeFailToDlx 消费主队列，并在业务处理失败时直接转入死信队列。
func (f *MQFanout) ConsumeFailToDlx(handler MsgHandler) error {
	return f.runConsumer(handler, consumerConfig{
		operation: "consume fail-to-dlx",
		logTag:    "fanout fail-to-dlx",
		declare: func(ch *amqp.Channel) (amqp.Queue, error) {
			q, _, err := f.declareDLXTopology(ch)
			return q, err
		},
		onDelivery: f.handleDeliveryFailToDLX,
	})
}

// ConsumeDlx 持续消费 fanout 模式的死信队列。
func (f *MQFanout) ConsumeDlx(handler MsgHandler) error {
	return f.runConsumer(handler, consumerConfig{
		operation: "consume dlx",
		logTag:    "fanout dlx",
		declare: func(ch *amqp.Channel) (amqp.Queue, error) {
			_, dlqName, err := f.declareDLXTopology(ch)
			if err != nil {
				return amqp.Queue{}, err
			}
			return amqp.Queue{Name: dlqName}, nil
		},
		onDelivery: f.handleDeliveryDLQ,
	})
}

// declareExchange 声明 fanout 类型的业务 exchange。
func (f *MQFanout) declareExchange(ch *amqp.Channel) error {
	return ch.ExchangeDeclare(f.opt.ExchangeName, amqp.ExchangeFanout, true, false, false, false, nil)
}

// declareBoundQueue 声明主队列并把它（按空 routing key）绑定到 fanout exchange。
// args 用于追加业务自定义参数（例如 x-dead-letter-exchange）。
func (f *MQFanout) declareBoundQueue(ch *amqp.Channel, args amqp.Table) (amqp.Queue, error) {
	if err := f.declareExchange(ch); err != nil {
		return amqp.Queue{}, err
	}

	queueArgs := amqp.Table{
		"x-max-priority": simpleQueueMaxPriority,
	}
	maps.Copy(queueArgs, args)

	queue, err := ch.QueueDeclare(f.opt.QueueName, true, false, false, false, queueArgs)
	if err != nil {
		return amqp.Queue{}, err
	}

	if bindErr := ch.QueueBind(queue.Name, "", f.opt.ExchangeName, false, nil); bindErr != nil {
		return amqp.Queue{}, bindErr
	}

	return queue, nil
}

// declareDLXTopology 声明 fanout 模式的死信拓扑：
// <exchange>.dlx fanout exchange + <base>.dlq；主队列的 x-dead-letter-exchange 指向 dlx。
func (f *MQFanout) declareDLXTopology(ch *amqp.Channel) (amqp.Queue, string, error) {
	dlxExchange := f.opt.ExchangeName + ".dlx"
	dlqName := f.baseName() + ".dlq"

	if err := ch.ExchangeDeclare(dlxExchange, amqp.ExchangeFanout, true, false, false, false, nil); err != nil {
		return amqp.Queue{}, "", err
	}

	queue, err := f.declareBoundQueue(ch, amqp.Table{
		"x-dead-letter-exchange": dlxExchange,
	})
	if err != nil {
		return amqp.Queue{}, "", err
	}

	if _, declareErr := ch.QueueDeclare(dlqName, true, false, false, false, nil); declareErr != nil {
		return amqp.Queue{}, "", declareErr
	}

	if bindErr := ch.QueueBind(dlqName, "", dlxExchange, false, nil); bindErr != nil {
		return amqp.Queue{}, "", bindErr
	}

	return queue, dlqName, nil
}

// baseName 派生 delay / dlq 队列的命名前缀。
// 优先用 QueueName；未设置时退回 sanitized 后的 ExchangeName。
func (f *MQFanout) baseName() string {
	if strings.TrimSpace(f.opt.QueueName) != "" {
		return safeNamePart(f.opt.QueueName)
	}

	return safeNamePart(f.opt.ExchangeName)
}
