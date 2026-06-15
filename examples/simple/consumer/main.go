package main

import (
	"context"
	"errors"
	"log"
	"os"
	"time"

	"github.com/gtkit/rabbit"
)

const (
	defaultURL = "amqp://guest:guest@127.0.0.1:5672/"
	maxRetry   = 3
	retryTTL   = 2 * time.Second
)

type handler struct{}

func (h *handler) Process(body []byte, msgID string) error {
	log.Printf("simple consumer recv: msgID=%s body=%s", msgID, string(body))

	if string(body) == "fail" {
		return errors.New("mock simple consume error")
	}

	return nil
}

func (h *handler) Failed(msg rabbit.FailedMsg) {
	log.Printf("simple consumer failed: msgID=%s body=%s", msg.MessageID, string(msg.Message))
}

func mqURL() string {
	if v := os.Getenv("MQ_URL"); v != "" {
		return v
	}
	return defaultURL
}

func main() {
	mq, err := rabbit.NewConsumeSimple(
		"demo.simple.queue",
		mqURL(),
		rabbit.WithContext(context.Background()),
		rabbit.WithConnectionName("demo-simple-consumer"),
		rabbit.WithMaxRetry(maxRetry),
		rabbit.WithRetryTTL(retryTTL),
	)
	if err != nil {
		log.Printf("new consumer: %v", err)
		return
	}
	defer mq.Destroy()

	if err := mq.Consume(&handler{}); err != nil {
		log.Printf("consume: %v", err)
	}
}
