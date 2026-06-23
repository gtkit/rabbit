package rabbit

import (
	"errors"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var _ Publisher = (*MQStream)(nil)

// StreamOffset 控制 stream 消费的起始位置。
//
// 取 OffsetFirst / OffsetLast / OffsetNext，或传入相对时长字符串（如 "1h"）
// 由 broker 解释；空值等价于 OffsetNext。
type StreamOffset string

const (
	// OffsetFirst 从 stream 最早的消息开始消费。
	OffsetFirst StreamOffset = "first"
	// OffsetLast 从最后一个 chunk 开始消费。
	OffsetLast StreamOffset = "last"
	// OffsetNext 只消费连接建立后新写入的消息。
	OffsetNext StreamOffset = "next"
)

// MQStream 是 stream 队列模式的客户端，适合大数据量 / 消息重放场景（broker 3.9+）。
//
// 与 AMQP 队列不同：stream 是追加式日志，消费端按 offset 读取、不支持重新入队；
// 消费失败的消息会被记录并跳过（ack 推进 offset），不会重试或 requeue。
// 发布走默认 exchange 直达 stream 队列。
type MQStream struct {
	*MQ
}

// newMQStreamFromConfig 用预构建的 option 创建 MQStream（强制 stream 队列类型）。
func newMQStreamFromConfig(queueName string, option MQOption) (*MQStream, error) {
	queueName = strings.TrimSpace(queueName)
	if queueName == "" {
		return nil, errors.New("queue name is required")
	}

	option.QueueName = queueName
	option.QueueType = QueueTypeStream
	var err error
	option, err = normalizeOption(option)
	if err != nil {
		return nil, err
	}

	core, err := newMQ(option, "stream")
	if err != nil {
		return nil, err
	}

	return &MQStream{MQ: core}, nil
}

// newMQStream 校验 queueName + 归一化 Option，构造一个 MQStream。
func newMQStream(queueName, mqURL string, opts ...Option) (*MQStream, error) {
	option, err := newOption(mqURL, opts...)
	if err != nil {
		return nil, err
	}
	return newMQStreamFromConfig(queueName, option)
}

// NewStream 创建 stream 模式实例，可用于发布和消费。
// cfg 应通过 NewConfig 预先构建；实例会强制使用 stream 队列类型。
func NewStream(queueName string, cfg MQOption) (*MQStream, error) {
	return newMQStreamFromConfig(queueName, cfg)
}

// NewPubStream 创建 stream 模式发布端实例。
func NewPubStream(queueName, mqURL string, opts ...Option) (*MQStream, error) {
	return newMQStream(queueName, mqURL, opts...)
}

// NewConsumeStream 创建 stream 模式消费端实例。
func NewConsumeStream(queueName, mqURL string, opts ...Option) (*MQStream, error) {
	return newMQStream(queueName, mqURL, opts...)
}

// PublishString 是 Publish 的便捷包装，自动将 string 转为 []byte。
func (s *MQStream) PublishString(msg string) (string, error) {
	return s.Publish([]byte(msg))
}

// Publish 向 stream 队列追加一条持久化消息，返回服务端确认后的 messageID。
func (s *MQStream) Publish(body []byte) (string, error) {
	return s.publishGeneric(publishRequest{
		operation:  "publish",
		body:       body,
		routingKey: s.opt.QueueName,
		mandatory:  true,
		headers:    amqp.Table{"x-retry": int32(0)},
		declares: []declareStep{
			{cacheKey: "stream-queue", declare: func(ch *amqp.Channel) error {
				_, err := s.declareStreamQueue(ch)
				return err
			}},
		},
	})
}

// PublishDelay 在 stream 模式下不被支持：stream 是追加式日志，无 per-message TTL / 延迟语义。
func (s *MQStream) PublishDelay(_ []byte, _ time.Duration) (string, error) {
	return "", errors.New("mq: PublishDelay is not supported for stream queues")
}

// Consume 从 stream 队列按 offset 持续消费。失败的消息记录后跳过（ack 推进 offset），
// 不重试、不 requeue。offset 控制起始位置，空值等价于 OffsetNext。
//
// stream 消费要求设置 prefetch（QoS），库默认 prefetch=1；高吞吐场景建议
// 通过 WithPrefetchCount 调大。
func (s *MQStream) Consume(handler MsgHandler, offset StreamOffset) error {
	if offset == "" {
		offset = OffsetNext
	}

	return s.runConsumer(handler, consumerConfig{
		operation:   "consume stream",
		logTag:      "stream consumer",
		consumeArgs: amqp.Table{"x-stream-offset": string(offset)},
		declare: func(ch *amqp.Channel) (amqp.Queue, error) {
			return s.declareStreamQueue(ch)
		},
		onDelivery: s.handleStreamDelivery,
	})
}

// declareStreamQueue 声明 stream 队列（x-queue-type=stream，由 mainQueueArgs 决定）。
func (s *MQStream) declareStreamQueue(ch *amqp.Channel) (amqp.Queue, error) {
	return ch.QueueDeclare(s.opt.QueueName, true, false, false, false, s.mainQueueArgs(nil))
}

// handleStreamDelivery 处理单条 stream 消息：成功 ack 推进 offset；
// 失败时通知 Failed 后仍 ack（stream 不支持重试 / requeue，避免 poison 消息卡住）。
func (s *MQStream) handleStreamDelivery(msg amqp.Delivery, handler MsgHandler) error {
	start := time.Now()

	select {
	case <-s.contextOrBackground().Done():
		canceled := s.canceledError("consume stream")
		s.emitConsume(ConsumeEvent{
			Operation: "consume stream",
			MessageID: msg.MessageId,
			BodySize:  len(msg.Body),
			Duration:  time.Since(start),
			Err:       canceled,
		})
		_ = msg.Nack(false, false)
		return canceled
	default:
	}

	processErr := s.safeProcess(handler, msg.Body, msg.MessageId)
	s.emitConsume(ConsumeEvent{
		Operation: "consume stream",
		MessageID: msg.MessageId,
		BodySize:  len(msg.Body),
		Duration:  time.Since(start),
		Err:       processErr,
	})
	if processErr != nil {
		s.notifyConsumeFailed(handler, s.failedMessage(msg.Body, msg.MessageId))
	}

	return msg.Ack(false)
}
