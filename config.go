package rabbit

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Option 用于在构造 RabbitMQ 实例时设置可选配置。
type Option func(*MQOption)

// QueueType 是主队列的类型。
//
// classic 是默认值，行为与历史版本完全一致（兼容 RabbitMQ 3.x / 4.x）；
// quorum 提供 RabbitMQ 4.x 下的队列高可用（替代已移除的 classic 镜像队列），需 broker 3.8+；
// stream 适合大数据量 / 消息重放场景，需 broker 3.9+。
//
// 注意：quorum 与 stream 队列不支持 x-max-priority，库会自动跳过该参数。
type QueueType string

const (
	// QueueTypeClassic 是默认队列类型（经典队列）。
	QueueTypeClassic QueueType = "classic"
	// QueueTypeQuorum 是 quorum 队列类型，用于高可用场景（broker 3.8+）。
	QueueTypeQuorum QueueType = "quorum"
	// QueueTypeStream 是 stream 队列类型，用于大数据量 / 重放场景（broker 3.9+）。
	QueueTypeStream QueueType = "stream"
)

// WithQueueType 设置主队列类型，默认 classic。
//
// 仅影响主队列；retry / delay / dlq 等派生队列恒为 classic。
// 选择 quorum / stream 时，库会自动跳过 x-max-priority（这两类队列不兼容优先级）。
//
// 兼容性：对一个已存在的同名队列改变其类型会被 broker 以 PRECONDITION_FAILED 拒绝，
// 需先删除或更换队列名。
func WithQueueType(t QueueType) Option {
	return func(option *MQOption) {
		option.QueueType = t
	}
}

// WithPriority 设置主队列与派生队列的 x-max-priority。
//
// 不调用时，classic 队列默认 10（与历史版本一致）；传入 0 显式关闭优先级。
// quorum / stream 队列始终不写 x-max-priority，此设置对其无效。
func WithPriority(maxPriority uint8) Option {
	return func(option *MQOption) {
		option.maxPriority = maxPriority
		option.prioritySet = true
	}
}

// WithDeliveryLimit 为 quorum 队列设置 x-delivery-limit，
// 使用 quorum 原生的毒消息处理（投递达到上限后被丢弃 / 进入 DLX）。
// 值 <= 0 时不设置；仅对 quorum 队列生效。
func WithDeliveryLimit(limit int) Option {
	return func(option *MQOption) {
		option.deliveryLimit = limit
	}
}

// WithDelayedExchange 启用基于 rabbitmq_delayed_message_exchange 插件的延迟投递。
//
// 启用后，PublishDelay 通过 x-delayed-message 类型 exchange 投递，
// 单个 exchange 即可承载混合 TTL，无队头阻塞；要求 broker 已安装该插件。
// 不启用时（默认），延迟通过按 TTL 分桶的队列实现，不依赖任何插件。
func WithDelayedExchange() Option {
	return func(option *MQOption) {
		option.delayedExchange = true
	}
}

// WithPublisherConnection 让发布与消费使用相互独立的 connection，
// 避免 TCP 背压时消费端的 ack 被发布端阻塞（RabbitMQ 官方最佳实践）。
// 默认不启用，收发共用一条 connection 以保持兼容。
func WithPublisherConnection() Option {
	return func(option *MQOption) {
		option.isolatePublisher = true
	}
}

// WithContext 为当前 mq 实例设置生命周期 context。
// 当 context 被取消后，发布操作会尽快返回，消费循环也会退出。
func WithContext(ctx context.Context) Option {
	return func(option *MQOption) {
		if ctx != nil {
			option.Ctx = ctx
		}
	}
}

// WithConnectionName 设置 RabbitMQ connection name。
// 该名称会显示在 RabbitMQ 管理界面中，便于定位连接来源。
func WithConnectionName(name string) Option {
	return func(option *MQOption) {
		option.ConnName = name
	}
}

// WithQueueName 显式设置队列名称。
// 对 direct、fanout、topic 模式，建议通过该选项固定消费队列名。
func WithQueueName(name string) Option {
	return func(option *MQOption) {
		option.QueueName = name
	}
}

// WithMaxRetry 设置消费失败时的最大重试次数。值 <= 0 时回退到默认值 3。
func WithMaxRetry(maxRetry int32) Option {
	return func(option *MQOption) {
		option.MaxRetry = maxRetry
	}
}

// WithRetryTTL 设置失败重试消息在 retry queue 中的停留时长。
// 值 <= 0 时回退到默认 2 秒。
func WithRetryTTL(ttl time.Duration) Option {
	return func(option *MQOption) {
		option.RetryTTL = ttl
	}
}

// WithLogger 为当前实例注入 logger，覆盖全局 logger。
func WithLogger(l Logger) Option {
	return func(option *MQOption) {
		option.Logger = l
	}
}

// WithPubFailNotify 设置发布失败时的回调，等价于消费端 MsgHandler.Failed 的发布侧版本。
func WithPubFailNotify(fn func(FailedMsg)) Option {
	return func(option *MQOption) {
		option.PubFailNotify = fn
	}
}

// WithObserver 注入观测钩子，用于接入 Prometheus / OpenTelemetry 等。
// 钩子方法在调用方 goroutine 同步执行，回调内不应有阻塞或重操作。
func WithObserver(o Observer) Option {
	return func(option *MQOption) {
		option.Observer = o
	}
}

// WithVhost 设置 AMQP vhost。空串等价于默认 "/"。
func WithVhost(vhost string) Option {
	return func(option *MQOption) {
		option.Vhost = vhost
	}
}

// WithHeartbeat 设置 AMQP 心跳周期。值 <= 0 时回退到默认 10 秒。
func WithHeartbeat(d time.Duration) Option {
	return func(option *MQOption) {
		option.Heartbeat = d
	}
}

// WithPrefetchCount 设置消费端预取消息数。值 <= 0 时回退到默认 1。
// 大值能提升吞吐但可能影响公平消费；高并发场景建议根据业务调优。
func WithPrefetchCount(n int) Option {
	return func(option *MQOption) {
		option.PrefetchCount = n
	}
}

// WithTLSConfig 注入 TLS 配置。
// 注意：mqURL 仍然按业务给定的 scheme 决定（amqp:// 或 amqps://）。
func WithTLSConfig(c *tls.Config) Option {
	return func(option *MQOption) {
		option.TLSConfig = c
	}
}

// WithDeliveryMode 设置发布消息的 AMQP 投递模式。
// 1 = amqp.Transient（非持久化，适合高吞吐、可容忍丢失的场景），
// 2 = amqp.Persistent（持久化，默认值）。传入其他值使用默认值。
func WithDeliveryMode(mode uint8) Option {
	return func(option *MQOption) {
		option.DeliveryMode = mode
	}
}

// WithQueueArgs 设置主队列声明时的额外参数，如 x-message-ttl、x-max-length 等。
//
// 合并规则（见 mainQueueArgs）：先原样并入这里设置的全部键值（包括零值，
// 不做过滤），随后库按所选 QueueType 写入保留键——classic 写 x-max-priority、
// quorum 写 x-queue-type 与可选 x-delivery-limit、stream 写 x-queue-type；
// 对这些保留键，库的值优先于此处同名键。其余键原样传给 broker。
//
// 因此：请勿通过本选项设置 x-queue-type（用 WithQueueType）或 x-max-priority
// （用 WithPriority）；retry/delay/dlq 等派生队列不使用这里的参数。
func WithQueueArgs(args map[string]any) Option {
	return func(option *MQOption) {
		if option.QueueArgs == nil {
			option.QueueArgs = make(amqp.Table, len(args))
		}
		for k, v := range args {
			option.QueueArgs[k] = v
		}
	}
}

// WithExchangeArg 设置交换机声明时的单个额外参数。
// 可多次调用，调用语义为"覆盖或追加"。
func WithExchangeArg(key string, value any) Option {
	return func(option *MQOption) {
		if option.ExchangeArgs == nil {
			option.ExchangeArgs = make(amqp.Table)
		}
		option.ExchangeArgs[key] = value
	}
}

// NewConfig 创建一份可复用的归一化配置。
// 生产者和消费者可共用同一份配置，避免重复调用 With* 选项。
//
// 用法:
//
//	cfg, err := rabbit.NewConfig("amqp://guest:guest@localhost:5672/",
//	    rabbit.WithMaxRetry(5),
//	    rabbit.WithRetryTTL(time.Second),
//	)
//	// 生产者和消费者共用 cfg
//	pub  := rabbit.NewSimple("queue", cfg)
//	cons := rabbit.NewSimple("queue", cfg)
func NewConfig(mqURL string, opts ...Option) (MQOption, error) {
	return newOption(mqURL, opts...)
}

// newOption 把多个 Option 合并为 MQOption，并归一化默认值。
func newOption(mqURL string, opts ...Option) (MQOption, error) {
	option := MQOption{
		MQURL: mqURL,
	}

	for _, apply := range opts {
		if apply == nil {
			continue
		}

		apply(&option)
	}

	return normalizeOption(option)
}

// normalizeOption 校验必填项 + 给可选项填默认值。
// 所有 With* Option 的"值<=0/空串时回退默认"逻辑都集中在这里。
func normalizeOption(option MQOption) (MQOption, error) {
	option.ExchangeName = strings.TrimSpace(option.ExchangeName)
	option.QueueName = strings.TrimSpace(option.QueueName)
	option.RoutingKey = strings.TrimSpace(option.RoutingKey)
	option.MQURL = strings.TrimSpace(option.MQURL)
	option.ConnName = strings.TrimSpace(option.ConnName)
	option.Vhost = strings.TrimSpace(option.Vhost)

	if option.MQURL == "" {
		return MQOption{}, errors.New("mq url is required")
	}

	if option.Ctx == nil {
		option.Ctx = context.Background()
	}

	if option.ConnName == "" {
		option.ConnName = "mq-" + uuid.NewString()
	}

	if option.MaxRetry <= 0 {
		option.MaxRetry = defaultMaxRetry
	}

	if option.RetryTTL <= 0 {
		option.RetryTTL = defaultRetryTTL
	}

	if option.Vhost == "" {
		option.Vhost = defaultVhost
	}

	if option.Heartbeat <= 0 {
		option.Heartbeat = defaultHeartbeat
	}

	if option.PrefetchCount <= 0 {
		option.PrefetchCount = defaultPrefetchCount
	}

	if option.QueueType == "" {
		option.QueueType = QueueTypeClassic
	}
	switch option.QueueType {
	case QueueTypeClassic, QueueTypeQuorum, QueueTypeStream:
	default:
		return MQOption{}, fmt.Errorf("mq: invalid queue type %q (must be one of %q/%q/%q)",
			option.QueueType, QueueTypeClassic, QueueTypeQuorum, QueueTypeStream)
	}

	// 优先级默认值：用户未显式设置时回退到历史默认 10。
	// 归一化幂等：prioritySet 已为 true 时保持原值（含显式 0）。
	if !option.prioritySet {
		option.maxPriority = simpleQueueMaxPriority
		option.prioritySet = true
	}

	return option, nil
}
