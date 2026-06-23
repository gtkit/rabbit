package rabbit

import (
	"maps"

	amqp "github.com/rabbitmq/amqp091-go"
)

// delayedMessageExchangeType 是 rabbitmq_delayed_message_exchange 插件的 exchange 类型。
const delayedMessageExchangeType = "x-delayed-message"

// delayedExchangeArgs 在 base 基础上追加 x-delayed-type，用于把业务 exchange
// 声明为 x-delayed-message 类型（底层仍按 underlyingKind 路由）。
func delayedExchangeArgs(base amqp.Table, underlyingKind string) amqp.Table {
	args := make(amqp.Table)
	maps.Copy(args, base)
	args["x-delayed-type"] = underlyingKind
	return args
}

// publishWithDelayHeader 向业务 exchange 发布一条带 x-delay 的延迟消息（插件模式）。
//
// 适用于 direct/topic/fanout/headers：这些模式的业务 exchange 已被声明为
// x-delayed-message 类型，消费端正常绑定即可，发布端无需知道队列名。
// 不使用 mandatory：x-delayed-message 在发布时延迟路由，此刻无路由会被立即退回。
func (m *MQ) publishWithDelayHeader(
	exchange, routingKey string,
	body []byte,
	ms int,
	declareExchange func(ch *amqp.Channel) error,
) (string, error) {
	return m.publishGeneric(publishRequest{
		operation:  "publish delay",
		body:       body,
		exchange:   exchange,
		routingKey: routingKey,
		mandatory:  false,
		headers: amqp.Table{
			"x-retry": int32(0),
			"x-delay": ms,
		},
		declares: []declareStep{
			{cacheKey: "exchange", declare: declareExchange},
		},
	})
}

// publishDelayedExchange 通过一个独立的 x-delayed-message exchange 发布延迟消息。
//
// 适用于 simple 模式：simple 无业务 exchange，但队列名必填，发布端可自行声明
// 专用延迟 exchange 并把队列绑定上去。bindMainQueue 声明主队列并绑定到 delayedEx。
// 不使用 mandatory，原因同 publishWithDelayHeader。
func (m *MQ) publishDelayedExchange(
	body []byte,
	ms int,
	delayedEx, delayedType, routingKey string,
	bindMainQueue func(ch *amqp.Channel) error,
) (string, error) {
	return m.publishGeneric(publishRequest{
		operation:  "publish delay",
		body:       body,
		exchange:   delayedEx,
		routingKey: routingKey,
		mandatory:  false,
		headers: amqp.Table{
			"x-retry": int32(0),
			"x-delay": ms,
		},
		declares: []declareStep{
			{cacheKey: "delayed-ex:" + delayedEx, declare: func(ch *amqp.Channel) error {
				return ch.ExchangeDeclare(
					delayedEx, delayedMessageExchangeType, true, false, false, false,
					amqp.Table{"x-delayed-type": delayedType},
				)
			}},
			{cacheKey: "delayed-bind:" + delayedEx, declare: bindMainQueue},
		},
	})
}
