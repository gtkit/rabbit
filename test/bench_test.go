package test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gtkit/rabbit"
)

// 端到端 benchmark，依赖真实 RabbitMQ broker。
//
//	MQ_INTEGRATION=1 go test -bench=BenchmarkE2E -benchmem -run='^$' \
//	  -benchtime=3s ./test -timeout=2m

func BenchmarkE2ESimplePublish(b *testing.B) {
	requireRabbitMQBench(b)

	queue := fmt.Sprintf("bench-pub-%d", time.Now().UnixNano())
	b.Cleanup(func() { deleteQueue(b, queue) })

	mq, err := rabbit.NewPubSimple(queue, mqURL, rabbit.WithContext(context.Background()))
	if err != nil {
		b.Fatalf("new publisher: %v", err)
	}
	b.Cleanup(mq.Destroy)

	body := []byte("benchmark payload")

	b.ReportAllocs()
	for b.Loop() {
		if _, err := mq.Publish(body); err != nil {
			b.Fatalf("publish: %v", err)
		}
	}
}

func BenchmarkE2ESimplePublishConsumeRoundtrip(b *testing.B) {
	requireRabbitMQBench(b)

	queue := fmt.Sprintf("bench-rt-%d", time.Now().UnixNano())
	b.Cleanup(func() { deleteQueue(b, queue) })

	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)

	var received int64
	done := make(chan struct{})

	cons, err := rabbit.NewConsumeSimple(queue, mqURL, rabbit.WithContext(ctx))
	if err != nil {
		b.Fatalf("new consumer: %v", err)
	}
	b.Cleanup(cons.Destroy)

	target := int64(0)
	h := benchHandler{
		onProcess: func() {
			n := atomic.AddInt64(&received, 1)
			if n == atomic.LoadInt64(&target) {
				select {
				case <-done:
				default:
					close(done)
				}
			}
		},
	}

	go func() { _ = cons.Consume(h) }()

	pub, err := rabbit.NewPubSimple(queue, mqURL, rabbit.WithContext(ctx))
	if err != nil {
		b.Fatalf("new publisher: %v", err)
	}
	b.Cleanup(pub.Destroy)

	body := []byte("benchmark payload")

	b.ReportAllocs()
	var published int64
	for b.Loop() {
		atomic.AddInt64(&published, 1)
		if _, err := pub.Publish(body); err != nil {
			b.Fatalf("publish: %v", err)
		}
	}
	atomic.StoreInt64(&target, atomic.LoadInt64(&published))
	// 如果 consumer 已经在 target 之前 receive 完，需要再检查一次。
	if atomic.LoadInt64(&received) >= atomic.LoadInt64(&published) {
		return
	}

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		b.Fatalf("only %d/%d messages consumed within 30s",
			atomic.LoadInt64(&received), atomic.LoadInt64(&published))
	}
}

type benchHandler struct {
	onProcess func()
}

func (h benchHandler) Process([]byte, string) error {
	h.onProcess()
	return nil
}

func (h benchHandler) Failed(rabbit.FailedMsg) {}
