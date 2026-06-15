package main

import (
	"log"

	"github.com/gtkit/rabbit"
)

func main() {
	mq, err := rabbit.NewConsumeSimple(
		"rpc.queue",
		"amqp://guest:guest@127.0.0.1:5672/",
		rabbit.WithConnectionName("rpc-server"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Destroy()

	log.Println("RPC 服务端启动，等待请求...")
	if err := mq.ServeRPC(rabbit.RPCHandlerFunc(func(body []byte) ([]byte, error) {
		log.Printf("收到请求: %s", string(body))
		return []byte("pong"), nil
	})); err != nil {
		log.Fatal(err)
	}
}
