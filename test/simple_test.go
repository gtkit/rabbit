package test

import (
	"context"
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/gtkit/rabbit"
)

func TestSimpleMq(t *testing.T) {
	requireManualDemo(t)
	example12()
}

type Consumefail struct{}

func (m *Consumefail) Process(msg []byte, msgID string) error {
	log.Println("------------Simple Consume Msg ----------- : ", string(msg), " -----", msgID)
	return nil
}

func (m *Consumefail) Failed(msg rabbit.FailedMsg) {
	log.Printf("------------failed msg handler ----------- :  %s\n", string(msg.Message))
}

func example12() {
	queueName := "queue-simple"
	rabbitmq2, err2 := rabbit.NewConsumeSimple(
		queueName,
		mqURL,
		rabbit.WithConnectionName("123"),
		rabbit.WithContext(context.Background()),
	)
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println(err2)
	}

	go func() {
		for i := range 100 {
			func(i int) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				rabbitmq1, _ := rabbit.NewPubSimple(
					queueName,
					mqURL,
					rabbit.WithConnectionName("121"),
					rabbit.WithContext(ctx),
				)
				defer rabbitmq1.Destroy()

				if _, err := rabbitmq1.Publish([]byte("消息：" + strconv.Itoa(i))); err != nil {
					log.Println("publish err: ", err)
				}
				time.Sleep(2 * time.Second)
			}(i)
		}
	}()

	go func() {
		if err := rabbitmq2.Consume(&Consumefail{}); err != nil {
			log.Println("----Consume error: ", err)
		}
	}()

	forever := make(chan bool)
	<-forever
}

func TestSimpleMqDlx(t *testing.T) {
	requireManualDemo(t)
	example12Dlx()
}

type SimpleMqDlx struct{}

func (m *SimpleMqDlx) Process(_ []byte, _ string) error {
	return nil
}

func (m *SimpleMqDlx) Failed(_ rabbit.FailedMsg) {}

type doDlx struct{}

func (m *doDlx) Process(_ []byte, _ string) error {
	return nil
}

func (m *doDlx) Failed(_ rabbit.FailedMsg) {}

func example12Dlx() {
	queueName := "queue3-dlx"

	rabbitmq2, err2 := rabbit.NewConsumeSimple(
		queueName,
		mqURL,
		rabbit.WithContext(context.Background()),
	)
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println(err2)
	}

	go func() {
		for i := range 100 {
			func(i int) {
				time.Sleep(2 * time.Second)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				rabbitmq1, _ := rabbit.NewPubSimple(
					queueName,
					mqURL,
					rabbit.WithContext(ctx),
				)
				defer rabbitmq1.Destroy()

				if _, err := rabbitmq1.Publish([]byte("消息：" + strconv.Itoa(i))); err != nil {
					log.Println("publish err: ", err)
				}
			}(i)
		}
	}()

	go func() {
		if err := rabbitmq2.ConsumeFailToDlx(&SimpleMqDlx{}); err != nil {
			log.Println("----Consume error: ", err)
		}
	}()

	go func() {
		if err := rabbitmq2.ConsumeDlx(&doDlx{}); err != nil {
			log.Println("----DlqConsume error: ", err)
		}
	}()

	forever := make(chan bool)
	<-forever
}

func TestSimpleDelay(t *testing.T) {
	requireManualDemo(t)
	example12Delay()
}

type SimpleDelay struct{}

func (m *SimpleDelay) Process(_ []byte, _ string) error {
	return nil
}

func (m *SimpleDelay) Failed(_ rabbit.FailedMsg) {}

func example12Delay() {
	queueName := "delay-queue"
	rabbitmq2, err2 := rabbit.NewConsumeSimple(
		queueName,
		mqURL,
		rabbit.WithContext(context.Background()),
	)
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println(err2)
	}

	go func() {
		for i := range 100 {
			func(i int) {
				time.Sleep(1 * time.Second)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				rabbitmq1, _ := rabbit.NewPubSimple(
					queueName,
					mqURL,
					rabbit.WithContext(ctx),
				)
				defer rabbitmq1.Destroy()

				if _, err := rabbitmq1.PublishDelay([]byte("消息："+strconv.Itoa(i)), 2*time.Second); err != nil {
					log.Println("publish delay err: ", err)
				}
			}(i)
		}
	}()

	go func() {
		if err := rabbitmq2.ConsumeDelay(&SimpleDelay{}); err != nil {
			log.Println("----ConsumeDelay error: ", err)
		}
	}()

	forever := make(chan bool)
	<-forever
}
