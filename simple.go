package rabbit

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

// simpleQueueMaxPriority 是 simple 模式所有队列的 x-max-priority。
const simpleQueueMaxPriority = 10

var _ MQInterface = (*MQSimple)(nil)
var _ Publisher = (*MQSimple)(nil)
var _ Consumer = (*MQSimple)(nil)
var _ Retrier = (*MQSimple)(nil)
var _ RPCCallPublisher = (*MQSimple)(nil)
var _ RPCServer = (*MQSimple)(nil)

// MQSimple 是 simple 模式（无 exchange，直接发送到指定队列）的客户端。
type MQSimple struct {
	*MQ
}

// newMQSimpleFromConfig 用预构建的 option 创建 MQSimple，适用于调用方已持有归一化配置的场景。
func newMQSimpleFromConfig(queueName string, option MQOption) (*MQSimple, error) {
	queueName = strings.TrimSpace(queueName)
	if queueName == "" {
		return nil, errors.New("queue name is required")
	}

	option.QueueName = queueName
	var err error
	option, err = normalizeOption(option)
	if err != nil {
		return nil, err
	}

	core, err := newMQ(option, "simple")
	if err != nil {
		return nil, err
	}

	return &MQSimple{MQ: core}, nil
}

// newMQSimple 校验 queueName + 归一化 Option，构造一个 MQSimple。
func newMQSimple(queueName, mqURL string, opts ...Option) (*MQSimple, error) {
	option, err := newOption(mqURL, opts...)
	if err != nil {
		return nil, err
	}
	return newMQSimpleFromConfig(queueName, option)
}

// NewSimple 创建 simple 模式实例，可用于发布和消费。
// cfg 应通过 NewConfig 预先构建；生产者和消费者可共用同一份 cfg。
func NewSimple(queueName string, cfg MQOption) (*MQSimple, error) {
	return newMQSimpleFromConfig(queueName, cfg)
}

// NewPubSimple 创建 simple 模式发布端实例。
func NewPubSimple(queueName, mqURL string, opts ...Option) (*MQSimple, error) {
	return newMQSimple(queueName, mqURL, opts...)
}

// NewConsumeSimple 创建 simple 模式消费端实例。
func NewConsumeSimple(queueName, mqURL string, opts ...Option) (*MQSimple, error) {
	return newMQSimple(queueName, mqURL, opts...)
}

// MustNewSimple 创建 simple 模式实例，失败时 panic。适用于初始化不可失败的场景（如测试）。
func MustNewSimple(queueName string, cfg MQOption) *MQSimple {
	mq, err := NewSimple(queueName, cfg)
	if err != nil {
		panic(err)
	}
	return mq
}

// PublishString 是 Publish 的便捷包装，自动将 string 转为 []byte。
func (s *MQSimple) PublishString(msg string) (string, error) {
	return s.Publish([]byte(msg))
}

// Call 发送 RPC 请求到简单队列并等待应答。
// 请求被发往主队列（未通过 exchange）；服务端处理完后将回复发回内部独占回复队列。
// 默认超时 30 秒，可通过实例的 context 设置 deadline 控制。
func (s *MQSimple) Call(body []byte) ([]byte, error) {
	return s.callGeneric("", s.opt.QueueName, body)
}

// ServeRPC 作为 RPC 服务端持续消费主队列中的请求。
// handler 处理请求并返回应答 body，返回 error 时消息被拒绝（不回复）。
func (s *MQSimple) ServeRPC(handler RPCHandler) error {
	return s.serveRPCLoop(handler, consumerConfig{
		operation: "rpc serve",
		logTag:    "simple rpc server",
		declare: func(ch *amqp.Channel) (amqp.Queue, error) {
			return s.declareQueue(ch, nil)
		},
	})
}

// BatchPublish 批量发布多条消息到简单队列。
func (s *MQSimple) BatchPublish(bodies [][]byte) ([]string, error) {
	return s.batchPublishGeneric("", s.opt.QueueName, bodies)
}

// PublishDelayString 是 PublishDelay 的便捷包装，自动将 string 转为 []byte。
func (s *MQSimple) PublishDelayString(msg string, ttl time.Duration) (string, error) {
	return s.PublishDelay([]byte(msg), ttl)
}

// PublishWithDlxString 是 PublishWithDlx 的便捷包装，自动将 string 转为 []byte。
func (s *MQSimple) PublishWithDlxString(msg string) (string, error) {
	return s.PublishWithDlx([]byte(msg))
}

// Publish 向普通队列发布一条持久化消息，返回服务端确认后的 messageID。
// 发布失败时若配置了 WithPubFailNotify，回调会收到上下文。
func (s *MQSimple) Publish(body []byte) (string, error) {
	return s.publishGeneric(publishRequest{
		operation:  "publish",
		body:       body,
		routingKey: s.opt.QueueName,
		mandatory:  true,
		headers:    amqp.Table{"x-retry": int32(0)},
		priority:   1,
		declares: []declareStep{
			{cacheKey: "queue", declare: func(ch *amqp.Channel) error {
				_, err := s.declareQueue(ch, nil)
				return err
			}},
		},
	})
}

// Consume 持续消费普通队列中的消息，失败自动走 retry queue。
func (s *MQSimple) Consume(handler MsgHandler) error {
	return s.runConsumer(handler, consumerConfig{
		operation: "consume",
		logTag:    "simple consumer",
		declare: func(ch *amqp.Channel) (amqp.Queue, error) {
			return s.declareQueue(ch, nil)
		},
		onDelivery: func(msg amqp.Delivery, h MsgHandler) error {
			return s.handleDeliveryWithRetry(msg, h, s.publishRetryMessage)
		},
	})
}

// RetryMsg 将当前 delivery 手动发送到 retry queue。
func (s *MQSimple) RetryMsg(msg amqp.Delivery, ttl time.Duration) error {
	done, ok := s.trackPublish()
	if !ok {
		return ErrDestroyed
	}
	defer done()

	headers := copyHeaders(msg.Headers)
	return s.publishRetryMessage(msg, headers, ttl)
}

// PublishDelay 发布一条延迟消息（毫秒精度），返回 messageID。
//
// 默认实现按 TTL 分桶到独立 delay 队列（队列名含 TTL），不同 TTL 互不阻塞；
// 启用 WithDelayedExchange 时改走 x-delayed-message 插件 exchange。
func (s *MQSimple) PublishDelay(body []byte, ttl time.Duration) (string, error) {
	ms := delayMillis(ttl)

	if s.opt.delayedExchange {
		delayedEx := s.opt.QueueName + ".delayed"
		return s.publishDelayedExchange(body, ms, delayedEx, amqp.ExchangeDirect, s.opt.QueueName,
			func(ch *amqp.Channel) error {
				if _, err := s.declareQueue(ch, nil); err != nil {
					return err
				}
				return ch.QueueBind(s.opt.QueueName, s.opt.QueueName, delayedEx, false, nil)
			})
	}

	delayQueue := fmt.Sprintf("%s-delay-%d", s.opt.QueueName, ms)
	return s.publishGeneric(publishRequest{
		operation:  "publish delay",
		body:       body,
		routingKey: delayQueue,
		mandatory:  true,
		headers:    amqp.Table{"x-retry": int32(0)},
		declares: []declareStep{
			{cacheKey: "queue", declare: func(ch *amqp.Channel) error {
				_, err := s.declareQueue(ch, nil)
				return err
			}},
			{cacheKey: "delay:" + delayQueue, declare: func(ch *amqp.Channel) error {
				_, err := ch.QueueDeclare(
					delayQueue, true, false, false, false,
					s.derivedQueueArgs(amqp.Table{
						"x-message-ttl":             ms,
						"x-dead-letter-exchange":    "",
						"x-dead-letter-routing-key": s.opt.QueueName,
					}),
				)
				return err
			}},
		},
	})
}

// ConsumeDelay 在 simple 模式下等价于 Consume。
func (s *MQSimple) ConsumeDelay(handler MsgHandler) error {
	return s.Consume(handler)
}

// PublishWithDlx 在声明死信拓扑后向主队列发布消息，返回 messageID。
func (s *MQSimple) PublishWithDlx(body []byte) (string, error) {
	return s.publishGeneric(publishRequest{
		operation:  "publish with dlx",
		body:       body,
		routingKey: s.opt.QueueName,
		mandatory:  true,
		headers:    amqp.Table{"x-retry": int32(0)},
		declares: []declareStep{
			{cacheKey: "dlx-topology", declare: func(ch *amqp.Channel) error {
				_, _, err := s.declareDLXTopology(ch)
				return err
			}},
		},
	})
}

// ConsumeFailToDlx 消费主队列，并在业务处理失败时直接转入死信队列。
func (s *MQSimple) ConsumeFailToDlx(handler MsgHandler) error {
	return s.runConsumer(handler, consumerConfig{
		operation: "consume fail-to-dlx",
		logTag:    "simple fail-to-dlx",
		declare: func(ch *amqp.Channel) (amqp.Queue, error) {
			q, _, err := s.declareDLXTopology(ch)
			return q, err
		},
		onDelivery: s.handleDeliveryFailToDLX,
	})
}

// ConsumeDlx 持续消费 simple 模式的死信队列。
func (s *MQSimple) ConsumeDlx(handler MsgHandler) error {
	return s.runConsumer(handler, consumerConfig{
		operation: "consume dlx",
		logTag:    "simple dlx",
		declare: func(ch *amqp.Channel) (amqp.Queue, error) {
			_, dlqName, err := s.declareDLXTopology(ch)
			if err != nil {
				return amqp.Queue{}, err
			}
			return amqp.Queue{Name: dlqName}, nil
		},
		onDelivery: s.handleDeliveryDLQ,
	})
}

// declareQueue 在 ch 上声明 simple 主队列（按 simpleQueueMaxPriority 设置优先级）。
// args 用于追加业务自定义 queue arguments（例如 x-dead-letter-exchange）。
func (s *MQSimple) declareQueue(ch *amqp.Channel, args amqp.Table) (amqp.Queue, error) {
	return ch.QueueDeclare(s.opt.QueueName, true, false, false, false, s.mainQueueArgs(args))
}

// declareDLXTopology 声明 simple 模式的死信拓扑：
// dlx-<queue> 是一个 fanout exchange，dlq-<queue> 是其绑定的死信队列。
// 同时把主队列的 x-dead-letter-exchange 指向 dlx，使被 reject 的消息进入 DLQ。
func (s *MQSimple) declareDLXTopology(ch *amqp.Channel) (amqp.Queue, string, error) {
	dlxName := "dlx-" + s.opt.QueueName
	dlqName := "dlq-" + s.opt.QueueName

	if err := ch.ExchangeDeclare(dlxName, amqp.ExchangeFanout, true, false, false, false, nil); err != nil {
		return amqp.Queue{}, "", err
	}

	queue, err := s.declareQueue(ch, amqp.Table{
		"x-dead-letter-exchange": dlxName,
	})
	if err != nil {
		return amqp.Queue{}, "", err
	}

	if _, declareErr := ch.QueueDeclare(dlqName, true, false, false, false, nil); declareErr != nil {
		return amqp.Queue{}, "", declareErr
	}

	if bindErr := ch.QueueBind(dlqName, "#", dlxName, false, nil); bindErr != nil {
		return amqp.Queue{}, "", bindErr
	}

	return queue, dlqName, nil
}

// publishRetryMessage 把当前 delivery 投递到 retry queue（带 TTL）。
// retry queue 的 dead-letter 目标是主队列，TTL 到期后消息自动回到主队列被再次消费。
func (s *MQSimple) publishRetryMessage(msg amqp.Delivery, headers amqp.Table, ttl time.Duration) error {
	retryQueue := s.opt.QueueName + "-retry"
	messageID := msg.MessageId
	if messageID == "" {
		messageID = uuid.NewString()
	}

	_, err := s.publishGeneric(publishRequest{
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
			{cacheKey: "retry:" + retryQueue, declare: func(ch *amqp.Channel) error {
				_, err := ch.QueueDeclare(
					retryQueue, true, false, false, false,
					s.derivedQueueArgs(amqp.Table{
						"x-dead-letter-exchange":    "",
						"x-dead-letter-routing-key": s.opt.QueueName,
					}),
				)
				return err
			}},
		},
	})
	return err
}
