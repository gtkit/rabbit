package rabbit

import (
	"context"
	"crypto/tls"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Option 用于在构造 RabbitMQ 实例时设置可选配置。
type Option func(*MQOption)

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

	return option, nil
}
