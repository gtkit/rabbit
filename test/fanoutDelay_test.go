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

func TestDelaySubMq(t *testing.T) {
	requireManualDemo(t)
	exampleDelay()
}

func exampleDelay() {
	rabbitmq2, err2 := rabbit.NewConsumeFanout(
		"exchange.delay",
		mqURL,
		rabbit.WithContext(context.Background()),
	)
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println(err2)
	}
	rabbitmq3, err3 := rabbit.NewConsumeFanout(
		"exchange.delay",
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
					"exchange.delay",
					mqURL,
					rabbit.WithContext(ctx),
				)
				defer rabbitmq1.Destroy()
				body := []byte("消息：" + strconv.Itoa(i))
				if _, err := rabbitmq1.PublishDelay(body, 2*time.Second); err != nil {
					log.Println("----PublishDelay error:", err)
					return
				}
				log.Printf("Delay Publish ---- msg=%s time=%s",
					string(body), time.Now().Format(time.DateTime))
			}(i)
		}
	}()

	go func() {
		if err := rabbitmq2.ConsumeDelay(&DelayMsg1{}); err != nil {
			log.Println("----ConsumeDelay 1 error: ", err)
		}
	}()

	go func() {
		if err := rabbitmq3.ConsumeDelay(&DelayMsg2{}); err != nil {
			log.Println("----ConsumeDelay 2 error: ", err)
		}
	}()

	forever := make(chan bool)
	<-forever
}

type DelayMsg1 struct{}

func (m *DelayMsg1) Process(msg []byte, _ string) error {
	log.Printf("fanout Consume Msg delay 1 ---- body=%s time=%s",
		string(msg), time.Now().Format(time.DateTime))
	return errors.New("test failed error")
}

func (m *DelayMsg1) Failed(msg rabbit.FailedMsg) {
	log.Printf("------------failed msg handler delay 1----------- :  %s\n", string(msg.Message))
}

type DelayMsg2 struct{}

func (m *DelayMsg2) Process(msg []byte, _ string) error {
	log.Printf("fanout Consume Msg delay 2 ---- body=%s time=%s",
		string(msg), time.Now().Format(time.DateTime))
	return nil
}

func (m *DelayMsg2) Failed(msg rabbit.FailedMsg) {
	log.Printf("------------failed msg handler delay 2----------- :  %s\n", string(msg.Message))
}
