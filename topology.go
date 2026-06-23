package rabbit

import (
	"maps"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
)

// declareBusinessExchange 声明业务 exchange。
// delayed 为 true 时声明为 x-delayed-message 类型（底层仍按 kind 路由），
// 供 routed/fanout/headers 的 declareExchange 复用，集中延迟分支逻辑。
func declareBusinessExchange(ch *amqp.Channel, name, kind string, args amqp.Table, delayed bool) error {
	if delayed {
		return declareExchangeWithArgs(ch, name, delayedMessageExchangeType, delayedExchangeArgs(args, kind))
	}
	return declareExchangeWithArgs(ch, name, kind, args)
}

// deriveBaseName 派生 retry / delay / dlq 队列的命名前缀。
// 优先用 queueName；为空时退回 sanitized 的 exchange，routingKey 非空再追加 ".<routingKey>"。
func deriveBaseName(queueName, exchange, routingKey string) string {
	if strings.TrimSpace(queueName) != "" {
		return safeNamePart(queueName)
	}
	if routingKey != "" {
		return safeNamePart(exchange) + "." + safeNamePart(routingKey)
	}
	return safeNamePart(exchange)
}

// dlxSpec 描述一次死信拓扑声明的各模式差异。
type dlxSpec struct {
	dlxExchange string // 死信 exchange 名
	dlxKind     string // 死信 exchange 类型
	dlqName     string // 死信队列名
	dlqBindKey  string // 死信队列绑定到 dlx 时的 binding key
	// declareMain 声明主队列（其 args 已含指向 dlxExchange 的 x-dead-letter-*），返回主队列。
	declareMain func(ch *amqp.Channel) (amqp.Queue, error)
}

// declareDLX 是各模式死信拓扑的公共骨架：
// 声明 dlx exchange → 声明主队列（含死信指向）→ 声明 dlq → 绑定 dlq 到 dlx。
func (m *MQ) declareDLX(ch *amqp.Channel, spec dlxSpec) (amqp.Queue, string, error) {
	if err := ch.ExchangeDeclare(spec.dlxExchange, spec.dlxKind, true, false, false, false, nil); err != nil {
		return amqp.Queue{}, "", err
	}

	queue, err := spec.declareMain(ch)
	if err != nil {
		return amqp.Queue{}, "", err
	}

	if _, declareErr := ch.QueueDeclare(spec.dlqName, true, false, false, false, nil); declareErr != nil {
		return amqp.Queue{}, "", declareErr
	}

	if bindErr := ch.QueueBind(spec.dlqName, spec.dlqBindKey, spec.dlxExchange, false, nil); bindErr != nil {
		return amqp.Queue{}, "", bindErr
	}

	return queue, spec.dlqName, nil
}

// mainQueueArgs 构造主队列声明参数。
//
// 合并顺序：用户 QueueArgs → 按 QueueType 决定的类型参数 → caller 传入的 extra（如 DLX）。
// classic（默认）会在 maxPriority>0 时写 x-max-priority，保持与历史版本一致；
// quorum 写 x-queue-type=quorum（可选 x-delivery-limit），stream 写 x-queue-type=stream，
// 两者均不写 x-max-priority。
func (m *MQ) mainQueueArgs(extra amqp.Table) amqp.Table {
	args := make(amqp.Table)
	maps.Copy(args, m.opt.QueueArgs)

	switch m.opt.QueueType {
	case QueueTypeQuorum:
		args["x-queue-type"] = "quorum"
		if m.opt.deliveryLimit > 0 {
			args["x-delivery-limit"] = m.opt.deliveryLimit
		}
	case QueueTypeStream:
		args["x-queue-type"] = "stream"
	default:
		if m.opt.maxPriority > 0 {
			args["x-max-priority"] = int(m.opt.maxPriority)
		}
	}

	maps.Copy(args, extra)
	return args
}

// derivedQueueArgs 构造 retry / delay 派生队列的声明参数。
//
// 派生队列恒为 classic（不受 WithQueueType 影响），不含用户 QueueArgs；
// 仅在 maxPriority>0 时追加 x-max-priority，保持与历史版本一致。
func (m *MQ) derivedQueueArgs(base amqp.Table) amqp.Table {
	args := make(amqp.Table)
	maps.Copy(args, base)
	if m.opt.maxPriority > 0 {
		args["x-max-priority"] = int(m.opt.maxPriority)
	}
	return args
}
