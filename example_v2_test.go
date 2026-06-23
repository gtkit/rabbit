package rabbit_test

import (
	"errors"
	"log"

	"github.com/gtkit/rabbit"
)

// ExampleWithQueueType 演示用 quorum 队列获得 RabbitMQ 4.x 下的高可用。
func ExampleWithQueueType() {
	cfg, err := rabbit.NewConfig("amqp://guest:guest@127.0.0.1:5672/",
		rabbit.WithQueueType(rabbit.QueueTypeQuorum),
		rabbit.WithDeliveryLimit(5), // quorum 原生毒消息处理
	)
	if err != nil {
		log.Fatal(err)
	}

	mq, err := rabbit.NewSimple("orders.quorum", cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Destroy()

	_, _ = mq.Publish([]byte("order-created"))
}

// ExampleErrPublishReturned 演示感知不可路由的发布。
func ExampleErrPublishReturned() {
	pub, err := rabbit.NewPubDirect("events", "rk.unbound", "amqp://guest:guest@127.0.0.1:5672/")
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Destroy()

	if _, err := pub.Publish([]byte("payload")); errors.Is(err, rabbit.ErrPublishReturned) {
		log.Println("消息未路由到任何队列，已被 broker 退回")
	}
}

// ExampleMQStream_Consume 演示从 stream 最早 offset 重放消费。
func ExampleMQStream_Consume() {
	cons, err := rabbit.NewConsumeStream("events.stream", "amqp://guest:guest@127.0.0.1:5672/")
	if err != nil {
		log.Fatal(err)
	}
	defer cons.Destroy()

	// 从最早的消息开始重放（handler 为业务实现的 rabbit.MsgHandler）。
	var handler rabbit.MsgHandler
	_ = cons.Consume(handler, rabbit.OffsetFirst)
}

// ExampleWithPublisherConnection 演示发布 / 消费连接隔离。
func ExampleWithPublisherConnection() {
	cfg, err := rabbit.NewConfig("amqp://guest:guest@127.0.0.1:5672/",
		rabbit.WithPublisherConnection(),
	)
	if err != nil {
		log.Fatal(err)
	}

	mq, err := rabbit.NewSimple("tasks", cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Destroy()
}
