package rabbit //nolint:dupl // direct/topic 是 routedMQ 的薄壳，按设计重复

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

var _ MQInterface = (*MQDirect)(nil)
var _ Publisher = (*MQDirect)(nil)
var _ Consumer = (*MQDirect)(nil)
var _ RPCCallPublisher = (*MQDirect)(nil)
var _ RPCServer = (*MQDirect)(nil)

// MQDirect 是 direct exchange 模式的客户端。
type MQDirect struct {
	*routedMQ
}

// newDirect 创建 direct 模式实例。
func newDirect(exchangeName, routingKey, mqURL string, opts ...Option) (*MQDirect, error) {
	r, err := newRouted(amqp.ExchangeDirect, exchangeName, routingKey, mqURL, opts...)
	if err != nil {
		return nil, err
	}
	return &MQDirect{routedMQ: r}, nil
}

// NewDirect 创建 direct 模式实例，可用于发布和消费。
// cfg 应通过 NewConfig 预先构建。
func NewDirect(exchangeName, routingKey string, cfg MQOption) (*MQDirect, error) {
	r, err := newRoutedFromConfig(amqp.ExchangeDirect, exchangeName, routingKey, cfg)
	if err != nil {
		return nil, err
	}
	return &MQDirect{routedMQ: r}, nil
}

// NewPubDirect 创建 direct 模式发布端实例。
func NewPubDirect(exchangeName, routingKey, mqURL string, opts ...Option) (*MQDirect, error) {
	return newDirect(exchangeName, routingKey, mqURL, opts...)
}

// NewConsumeDirect 创建 direct 模式消费端实例。
func NewConsumeDirect(exchangeName, routingKey, mqURL string, opts ...Option) (*MQDirect, error) {
	return newDirect(exchangeName, routingKey, mqURL, opts...)
}

// MustNewDirect 创建 direct 模式实例，失败时 panic。
// 适用于初始化不可失败的场景（如测试）。
func MustNewDirect(exchangeName, routingKey string, cfg MQOption) *MQDirect {
	mq, err := NewDirect(exchangeName, routingKey, cfg)
	if err != nil {
		panic(err)
	}
	return mq
}
