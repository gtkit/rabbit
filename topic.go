package rabbit //nolint:dupl // direct/topic 是 routedMQ 的薄壳，按设计重复

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

var _ MQInterface = (*MQTopic)(nil)
var _ Publisher = (*MQTopic)(nil)
var _ Consumer = (*MQTopic)(nil)
var _ RPCCallPublisher = (*MQTopic)(nil)
var _ RPCServer = (*MQTopic)(nil)

// MQTopic 是 topic exchange 模式的客户端。
type MQTopic struct {
	*routedMQ
}

// newTopic 创建 topic 模式实例。
func newTopic(exchangeName, routingKey, mqURL string, opts ...Option) (*MQTopic, error) {
	r, err := newRouted(amqp.ExchangeTopic, exchangeName, routingKey, mqURL, opts...)
	if err != nil {
		return nil, err
	}
	return &MQTopic{routedMQ: r}, nil
}

// NewTopic 创建 topic 模式实例，可用于发布和消费。
// cfg 应通过 NewConfig 预先构建。
func NewTopic(exchangeName, routingKey string, cfg MQOption) (*MQTopic, error) {
	r, err := newRoutedFromConfig(amqp.ExchangeTopic, exchangeName, routingKey, cfg)
	if err != nil {
		return nil, err
	}
	return &MQTopic{routedMQ: r}, nil
}

// NewPubTopic 创建 topic 模式发布端实例。
func NewPubTopic(exchangeName, routingKey, mqURL string, opts ...Option) (*MQTopic, error) {
	return newTopic(exchangeName, routingKey, mqURL, opts...)
}

// NewConsumeTopic 创建 topic 模式消费端实例。
func NewConsumeTopic(exchangeName, routingKey, mqURL string, opts ...Option) (*MQTopic, error) {
	return newTopic(exchangeName, routingKey, mqURL, opts...)
}

// MustNewTopic 创建 topic 模式实例，失败时 panic。
// 适用于初始化不可失败的场景（如测试）。
func MustNewTopic(exchangeName, routingKey string, cfg MQOption) *MQTopic {
	mq, err := NewTopic(exchangeName, routingKey, cfg)
	if err != nil {
		panic(err)
	}
	return mq
}
