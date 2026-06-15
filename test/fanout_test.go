package test

import (
	"context"
	"errors"
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/gtkit/rabbit"
)

func TestSubMq(t *testing.T) {
	requireManualDemo(t)
	example3()
}

func example3() {
	rabbitmq2, err2 := rabbit.NewConsumeFanout(
		"exchange.example3",
		mqURL,
		rabbit.WithContext(context.Background()),
	)
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println(err2)
	}
	rabbitmq3, err3 := rabbit.NewConsumeFanout(
		"exchange.example3",
		mqURL,
		rabbit.WithContext(context.Background()),
	)
	defer rabbitmq3.Destroy()
	if err3 != nil {
		log.Println(err3)
	}

	go func() {
		for i := range 1000 {
			func(i int) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				rabbitmq1, _ := rabbit.NewPubFanout(
					"exchange.example3",
					mqURL,
					rabbit.WithContext(ctx),
				)
				defer rabbitmq1.Destroy()
				time.Sleep(1 * time.Second)
				if _, err := rabbitmq1.Publish([]byte("消息：" + strconv.Itoa(i))); err != nil {
					log.Println("----example3 Publish error:", err)
				}
			}(i)
		}
	}()

	go func() {
		if err := rabbitmq2.Consume(&Example31{}); err != nil {
			log.Println("----example3 Consume error: ", err)
		}
	}()

	go func() {
		if err := rabbitmq3.Consume(&Example32{}); err != nil {
			log.Println("----example3 Consume error: ", err)
		}
	}()

	forever := make(chan bool)
	<-forever
}

func TestFanoutDlx(t *testing.T) {
	requireManualDemo(t)
	exampleFanoutDlx()
}

func exampleFanoutDlx() {
	rabbitmq2, err2 := rabbit.NewConsumeFanout(
		"exchange.example3",
		mqURL,
		rabbit.WithContext(context.Background()),
	)
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println(err2)
	}
	rabbitmq3, err3 := rabbit.NewConsumeFanout(
		"exchange.example3",
		mqURL,
		rabbit.WithContext(context.Background()),
	)
	defer rabbitmq3.Destroy()
	if err3 != nil {
		log.Println(err3)
	}

	go func() {
		for i := range 10000 {
			time.Sleep(1 * time.Second)
			func(i int) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				rabbitmq1, _ := rabbit.NewPubFanout(
					"exchange.example3",
					mqURL,
					rabbit.WithContext(ctx),
				)
				defer rabbitmq1.Destroy()
				if _, err := rabbitmq1.Publish([]byte("消息：" + strconv.Itoa(i))); err != nil {
					log.Println("----example3 Publish error:", err)
				}
			}(i)
		}
	}()

	go func() {
		log.Println("----ConsumeFailToDlx Consume ------")
		if err := rabbitmq2.ConsumeFailToDlx(&FailToDlx{}); err != nil {
			log.Println("----ConsumeFailToDlx Consume error: ", err)
		}
	}()
	time.Sleep(5 * time.Second)

	go func() {
		log.Println("----ConsumeDlx Consume ------: ")
		if err := rabbitmq3.ConsumeDlx(&ConsumeDlx{}); err != nil {
			log.Println("----ConsumeDlx Consume error: ", err)
		}
	}()

	forever := make(chan bool)
	<-forever
}

type Example31 struct{}

func (m *Example31) Process(msg []byte, msgID string) error {
	log.Println("------------fanout Consume Msg Example31 ----------- : ", string(msg), " -----", msgID)
	return errors.New("test retry error")
}

func (m *Example31) Failed(msg rabbit.FailedMsg) {
	log.Printf("------------failed msg handler Example3:1----------- :  %s\n", string(msg.Message))
}

type Example32 struct{}

func (m *Example32) Process(msg []byte, msgID string) error {
	log.Println("------------fanout Consume Msg Example3:2 ----------- : ", string(msg), " -----", msgID)
	return errors.New("test retry error")
}

func (m *Example32) Failed(msg rabbit.FailedMsg) {
	log.Printf("------------failed msg handler Example32----------- :  %s\n", string(msg.Message))
}

type FailToDlx struct{}

func (m *FailToDlx) Process(msg []byte, msgID string) error {
	log.Println("------------fanout Consume Msg FailToDlx ----------- : ", string(msg), " -----", msgID)
	return errors.New("test retry error")
}

func (m *FailToDlx) Failed(msg rabbit.FailedMsg) {
	log.Printf("------------failed msg handler FailToDlx----------- :  %s\n", string(msg.Message))
}

type ConsumeDlx struct{}

func (m *ConsumeDlx) Process(msg []byte, msgID string) error {
	log.Println("------------fanout Consume Msg ConsumeDlx ----------- : ", string(msg), " -----", msgID)
	return nil
}

func (m *ConsumeDlx) Failed(msg rabbit.FailedMsg) {
	log.Printf("------------failed msg handler ConsumeDlx----------- :  %s\n", string(msg.Message))
}
