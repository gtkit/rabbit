package rabbit

import (
	"maps"

	amqp "github.com/rabbitmq/amqp091-go"
)

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
