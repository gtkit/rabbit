package main

import (
	"log"
	"time"

	"github.com/gtkit/rabbit"
)

func main() {
	mq, err := rabbit.NewPubSimple(
		"rpc.queue",
		"amqp://guest:guest@127.0.0.1:5672/",
		rabbit.WithConnectionName("rpc-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Destroy()

	// 发送 RPC 请求并等待应答
	log.Println("发送 RPC 请求...")
	start := time.Now()
	reply, err := mq.Call([]byte("ping"))
	if err != nil {
		log.Fatalf("RPC 调用失败: %v", err)
	}
	log.Printf("收到应答: body=%s 耗时=%v", string(reply), time.Since(start))
}
