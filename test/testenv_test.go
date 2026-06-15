package test

import (
	"os"
	"testing"
)

const defaultMQURL = "amqp://guest:guest@127.0.0.1:5672/"

var mqURL = defaultMQURL

// requireRabbitMQ 用于可结束的集成测试（如 e2e_test.go）。
// MQ_INTEGRATION=1 启用，MQ_URL 可覆盖连接串。
func requireRabbitMQ(t *testing.T) {
	t.Helper()

	if os.Getenv("MQ_INTEGRATION") != "1" {
		t.Skip("set MQ_INTEGRATION=1 to run RabbitMQ integration tests")
	}

	if value := os.Getenv("MQ_URL"); value != "" {
		mqURL = value
		return
	}

	mqURL = defaultMQURL
}

// requireManualDemo 用于永阻塞的手动 demo（simple_test.go / direct_test.go 等里
// 用 select{} / <-forever 的用例）。这些用例本意是给开发者本地手动观察消息行为，
// 不适合自动化运行；默认 skip，MQ_DEMO=1 才启用。
func requireManualDemo(t *testing.T) {
	t.Helper()

	if os.Getenv("MQ_DEMO") != "1" {
		t.Skip("manual demo test (blocks forever); set MQ_DEMO=1 to enable")
	}

	if value := os.Getenv("MQ_URL"); value != "" {
		mqURL = value
		return
	}

	mqURL = defaultMQURL
}

// requireRabbitMQBench 是 benchmark 版的 require gate。
// 直接接受 *testing.B 避免 testing.TB 的接口装箱。
func requireRabbitMQBench(b *testing.B) {
	b.Helper()

	if os.Getenv("MQ_INTEGRATION") != "1" {
		b.Skip("set MQ_INTEGRATION=1 to run RabbitMQ benchmark")
	}

	if value := os.Getenv("MQ_URL"); value != "" {
		mqURL = value
		return
	}

	mqURL = defaultMQURL
}
