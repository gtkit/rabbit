package main

import (
	"context"
	"log"
	"time"

	"github.com/gtkit/rabbit"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	mq, err := rabbit.NewPubHeaders(
		"notify.headers.exchange",
		"amqp://guest:guest@127.0.0.1:5672/",
		rabbit.WithContext(context.Background()),
		rabbit.WithConnectionName("headers-producer"),
		rabbit.WithPubFailNotify(func(msg rabbit.FailedMsg) {
			log.Printf("发布失败: msgID=%s body=%s", msg.MessageID, msg.Message)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Destroy()

	// 发布匹配 format=json 的消息
	msgID, err := mq.PublishWithHeaders(
		[]byte(`{"order_id": 1001}`),
		amqp.Table{"format": "json", "type": "event"},
	)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("headers 消息发布成功: msgID=%s time=%s", msgID, time.Now().Format(time.DateTime))

	// 也可以使用普通的 Publish（不带额外路由 headers）
	msgID, err = mq.PublishString("plain message without routing headers")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("普通消息发布成功: msgID=%s", msgID)
}
