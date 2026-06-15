package rabbit

import (
	"context"
	"testing"
	"time"
)

// testCtxKey 是测试中用于 context.WithValue 的具名 key 类型，
// 取代会触发 staticcheck SA1029 的 struct{}{} 字面量。
type testCtxKey struct{}

func TestNormalizeOptionDefaultsContextAndConnectionName(t *testing.T) {
	t.Parallel()

	opt, err := normalizeOption(MQOption{
		QueueName: "jobs",
		MQURL:     "amqp://guest:guest@localhost:5672/",
	})
	if err != nil {
		t.Fatalf("normalizeOption() error = %v", err)
	}

	if opt.Ctx == nil {
		t.Fatal("normalizeOption() returned nil context")
	}

	if opt.ConnName == "" {
		t.Fatal("normalizeOption() returned empty connection name")
	}

	if opt.MaxRetry != defaultMaxRetry {
		t.Fatalf("normalizeOption() maxRetry = %d, want %d", opt.MaxRetry, defaultMaxRetry)
	}

	if opt.RetryTTL != defaultRetryTTL {
		t.Fatalf("normalizeOption() retryTTL = %v, want %v", opt.RetryTTL, defaultRetryTTL)
	}
}

func TestNormalizeOptionPreservesProvidedContextAndConnectionName(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), testCtxKey{}, "marker")

	opt, err := normalizeOption(MQOption{
		QueueName: "jobs",
		MQURL:     "amqp://guest:guest@localhost:5672/",
		ConnName:  "producer-a",
		Ctx:       ctx,
	})
	if err != nil {
		t.Fatalf("normalizeOption() error = %v", err)
	}

	if opt.Ctx != ctx {
		t.Fatal("normalizeOption() replaced the provided context")
	}

	if opt.ConnName != "producer-a" {
		t.Fatalf("normalizeOption() connName = %q, want %q", opt.ConnName, "producer-a")
	}

	if opt.MaxRetry != defaultMaxRetry {
		t.Fatalf("normalizeOption() maxRetry = %d, want %d", opt.MaxRetry, defaultMaxRetry)
	}

	if opt.RetryTTL != defaultRetryTTL {
		t.Fatalf("normalizeOption() retryTTL = %v, want %v", opt.RetryTTL, defaultRetryTTL)
	}
}

func TestNewOptionAppliesFunctionalOptions(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), testCtxKey{}, "marker")

	opt, err := newOption(
		"amqp://guest:guest@localhost:5672/",
		WithQueueName("jobs"),
		WithConnectionName("producer-a"),
		WithContext(ctx),
		WithMaxRetry(5),
		WithRetryTTL(3500*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("newOption() error = %v", err)
	}

	if opt.QueueName != "jobs" {
		t.Fatalf("newOption() queueName = %q, want %q", opt.QueueName, "jobs")
	}

	if opt.ConnName != "producer-a" {
		t.Fatalf("newOption() connName = %q, want %q", opt.ConnName, "producer-a")
	}

	if opt.Ctx != ctx {
		t.Fatal("newOption() replaced the provided context")
	}

	if opt.MaxRetry != 5 {
		t.Fatalf("newOption() maxRetry = %d, want %d", opt.MaxRetry, 5)
	}

	if opt.RetryTTL != 3500*time.Millisecond {
		t.Fatalf("newOption() retryTTL = %v, want %v", opt.RetryTTL, 3500*time.Millisecond)
	}
}
