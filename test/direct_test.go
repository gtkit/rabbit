package test

import (
	"context"
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/gtkit/rabbit"
)

func TestDirect(t *testing.T) {
	requireManualDemo(t)
	exampleDirect()
}

func TestDirectDelay(t *testing.T) {
	requireManualDemo(t)
	exampleDirectDelay()
}

func TestDirectDlx(t *testing.T) {
	requireManualDemo(t)
	exampleDirectDlx()
}

type DirectFailToDlx struct{}

func (m *DirectFailToDlx) Process(_ []byte, _ string) error {
	return nil
}

func (m *DirectFailToDlx) Failed(_ rabbit.FailedMsg) {}

type DirectDlx struct{}

func (m *DirectDlx) Process(_ []byte, _ string) error {
	return nil
}

func (m *DirectDlx) Failed(_ rabbit.FailedMsg) {}

func exampleDirectDlx() {
	var (
		routingKey = "key.direct.dlx"
		exchange   = "exchange.direct.dlx"
		queueName  = ""
	)
	rabbitmq2, err1 := rabbit.NewConsumeDirect(
		exchange,
		routingKey,
		mqURL,
		rabbit.WithQueueName(queueName),
		rabbit.WithContext(context.Background()),
	)
	defer rabbitmq2.Destroy()
	if err1 != nil {
		log.Println(err1)
	}

	go func() {
		for i := range 100 {
			func(i int) {
				time.Sleep(1 * time.Second)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				rabbitmq1, _ := rabbit.NewPubDirect(
					exchange,
					routingKey,
					mqURL,
					rabbit.WithContext(ctx),
				)
				defer rabbitmq1.Destroy()

				msg := "消息：" + strconv.Itoa(i)
				if _, err := rabbitmq1.Publish([]byte(msg)); err != nil {
					log.Println("----direct.dlx Publish error:", err)
					return
				}
				log.Println("----Publish Dlx success: ", msg, " ----", time.Now().Format(time.DateTime))
			}(i)
		}
	}()

	go func() {
		if err := rabbitmq2.ConsumeFailToDlx(&DirectFailToDlx{}); err != nil {
			log.Println("----ConsumeFailToDlx Consume error: ", err)
		}
	}()

	go func() {
		if err := rabbitmq2.ConsumeDlx(&DirectDlx{}); err != nil {
			log.Println("----ConsumeDlx Consume error: ", err)
		}
	}()

	select {}
}

type DirectDelay struct{}

func (m *DirectDelay) Process(_ []byte, _ string) error {
	return nil
}

func (m *DirectDelay) Failed(_ rabbit.FailedMsg) {}

func exampleDirectDelay() {
	var (
		routingKey = "key.direct.delay"
		exchange   = "exchange.direct.delay"
	)

	rabbitmq2, err2 := rabbit.NewConsumeDirect(
		exchange,
		routingKey,
		mqURL,
		rabbit.WithContext(context.Background()),
	)
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println(err2)
	}
	go func() {
		for i := range 100 {
			time.Sleep(1 * time.Second)
			func(i int) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				rabbitmq1, _ := rabbit.NewPubDirect(
					exchange,
					routingKey,
					mqURL,
					rabbit.WithContext(ctx),
				)
				defer rabbitmq1.Destroy()
				msg := "消息：" + strconv.Itoa(i)
				if _, err := rabbitmq1.PublishDelay([]byte(msg), 2*time.Second); err != nil {
					log.Println("----example3 PublishDelay error:", err)
					return
				}
				log.Println("----PublishDelay success: ", msg, " ----", time.Now().Format(time.DateTime))
			}(i)
		}
	}()

	go func() {
		if err := rabbitmq2.ConsumeDelay(&DirectDelay{}); err != nil {
			log.Println("----ConsumeDelay Consume error: ", err)
		}
	}()

	select {}
}

type Direct struct{}

func (m *Direct) Process(msg []byte, msgID string) error {
	log.Println("------------Direct Consume Msg ----------- : ", string(msg), " -----", msgID)
	return nil
}

func (m *Direct) Failed(msg rabbit.FailedMsg) {
	log.Printf("------------Direct failed msg handler ----------- :  %s\n", string(msg.Message))
}

func exampleDirect() {
	var (
		routingKey   = "my_direct_routingKey"
		queueName    = "my_direct_queue"
		exchangeName = "exchange_direct"
	)

	rabbitmq2, err2 := rabbit.NewConsumeDirect(
		exchangeName,
		routingKey,
		mqURL,
		rabbit.WithQueueName(queueName),
		rabbit.WithContext(context.Background()),
	)
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println("rabbitmq2----", err2)
	}

	go func() {
		for i := range 100 {
			func(i int) {
				time.Sleep(1 * time.Second)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				rabbitmq1, _ := rabbit.NewPubDirect(
					exchangeName,
					routingKey,
					mqURL,
					rabbit.WithContext(ctx),
				)
				defer rabbitmq1.Destroy()

				body := []byte("消息：" + strconv.Itoa(i))
				if _, err := rabbitmq1.Publish(body); err != nil {
					log.Println("-----------------example Publish error:", err)
					log.Println("******** do Publish Direct Msg failed: ", string(body))
				}
			}(i)
		}
	}()

	go func() {
		if err := rabbitmq2.Consume(&Direct{}); err != nil {
			log.Println("----Consume error: ", err)
		}
	}()

	select {}
}
