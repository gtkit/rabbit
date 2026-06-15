package main

import (
	"context"
	"log"

	"github.com/gtkit/rabbit"
)

// myHandler 实现 rabbit.MsgHandler 接口
type myHandler struct{}

func (h *myHandler) Process(body []byte, msgID string) error {
	log.Printf("消费到消息: msgID=%s body=%s", msgID, string(body))
	return nil
}

func (h *myHandler) Failed(msg rabbit.FailedMsg) {
	log.Printf("最终失败: msgID=%s body=%s", msg.MessageID, msg.Message)
}

func main() {
	// 定义 headers 匹配条件：format=json 或 type=event 任一匹配即可
	binding := rabbit.HeaderBinding{
		MatchAll: false, // x-match = any
		Headers: map[string]any{
			"format": "json",
			"type":   "event",
		},
	}

	mq, err := rabbit.NewConsumeHeaders(
		"notify.headers.exchange",
		binding,
		"amqp://guest:guest@127.0.0.1:5672/",
		rabbit.WithContext(context.Background()),
		rabbit.WithConnectionName("headers-consumer"),
		rabbit.WithQueueName("notify.headers.queue"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Destroy()

	log.Println("headers 消费者启动，等待消息...")
	if err := mq.Consume(&myHandler{}); err != nil {
		log.Fatal(err)
	}
}
