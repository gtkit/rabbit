package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/gtkit/rabbit"
)

const (
	defaultURL = "amqp://guest:guest@127.0.0.1:5672/"
	delayTTL   = 5 * time.Second
)

func onPubFail(msg rabbit.FailedMsg) {
	log.Printf("fanout producer failed: msgID=%s body=%s", msg.MessageID, string(msg.Message))
}

func mqURL() string {
	if v := os.Getenv("MQ_URL"); v != "" {
		return v
	}
	return defaultURL
}

func main() {
	mq, err := rabbit.NewPubFanout(
		"demo.fanout.exchange",
		mqURL(),
		rabbit.WithContext(context.Background()),
		rabbit.WithConnectionName("demo-fanout-producer"),
		rabbit.WithPubFailNotify(onPubFail),
	)
	if err != nil {
		log.Printf("new publisher: %v", err)
		return
	}
	defer mq.Destroy()

	msgID, err := mq.Publish([]byte("hello fanout"))
	if err != nil {
		log.Printf("publish failed: %v", err)
		return
	}
	log.Printf("publish ok: msgID=%s", msgID)

	delayID, err := mq.PublishDelay([]byte("hello fanout delay"), delayTTL)
	if err != nil {
		log.Printf("publish delay failed: %v", err)
		return
	}
	log.Printf("publish delay ok: msgID=%s", delayID)
}
