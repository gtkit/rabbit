package rabbit

import (
	"math"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestRetryCountSupportsCommonNumericTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		headers amqp.Table
		want    int32
	}{
		{
			name:    "missing",
			headers: amqp.Table{},
			want:    0,
		},
		{
			name: "int32",
			headers: amqp.Table{
				"x-retry": int32(2),
			},
			want: 2,
		},
		{
			name: "int64",
			headers: amqp.Table{
				"x-retry": int64(3),
			},
			want: 3,
		},
		{
			name: "int",
			headers: amqp.Table{
				"x-retry": 4,
			},
			want: 4,
		},
		{
			name: "uint8",
			headers: amqp.Table{
				"x-retry": uint8(5),
			},
			want: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := retryCount(tt.headers); got != tt.want {
				t.Fatalf("retryCount() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestTTLToString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		d    time.Duration
		want string
	}{
		{"positive", 2500 * time.Millisecond, "2500"},
		{"zero", 0, ""},
		{"negative", -time.Second, ""},
		{"seconds", 5 * time.Second, "5000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := ttlToString(tt.d); got != tt.want {
				t.Fatalf("ttlToString(%v) = %q, want %q", tt.d, got, tt.want)
			}
		})
	}
}

func TestClampInt32(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		v    int64
		want int32
	}{
		{"normal", 42, 42},
		{"zero", 0, 0},
		{"negative", -5, -5},
		{"over max", int64(math.MaxInt32) + 1, math.MaxInt32},
		{"under min", int64(math.MinInt32) - 1, math.MinInt32},
		{"boundary max", math.MaxInt32, math.MaxInt32},
		{"boundary min", math.MinInt32, math.MinInt32},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := clampInt32(tt.v); got != tt.want {
				t.Fatalf("clampInt32(%d) = %d, want %d", tt.v, got, tt.want)
			}
		})
	}
}

func TestSafeNamePart(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"normal", "order.queue", "order.queue"},
		{"empty", "", "default"},
		{"spaces", "  ", "default"},
		{"special chars", "order/created:test*star", "order_created_teststarstar"},
		{"backslash", "a\\b:c", "a_b_c"},
		{"hash", "logs#error", "logshasherror"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := safeNamePart(tt.input); got != tt.want {
				t.Fatalf("safeNamePart(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestCopyHeaders(t *testing.T) {
	t.Parallel()

	t.Run("nil", func(t *testing.T) {
		got := copyHeaders(nil)
		if len(got) != 0 {
			t.Fatalf("copyHeaders(nil) = %v, want empty", got)
		}
	})

	t.Run("empty", func(t *testing.T) {
		got := copyHeaders(amqp.Table{})
		if len(got) != 0 {
			t.Fatalf("copyHeaders(empty) = %v, want empty", got)
		}
	})

	t.Run("populated", func(t *testing.T) {
		original := amqp.Table{"x-retry": int32(2), "trace": "abc"}
		got := copyHeaders(original)
		if got["x-retry"] != int32(2) || got["trace"] != "abc" {
			t.Fatalf("copyHeaders = %v", got)
		}
		// 修改副本不应影响原表
		got["x-retry"] = int32(99)
		if original["x-retry"] != int32(2) {
			t.Fatal("copyHeaders modified original")
		}
	})
}

func TestMaxRetry(t *testing.T) {
	t.Parallel()

	t.Run("nil mq", func(t *testing.T) {
		var m *MQ
		if got := m.maxRetry(); got != defaultMaxRetry {
			t.Fatalf("nil maxRetry = %d, want %d", got, defaultMaxRetry)
		}
	})

	t.Run("custom", func(t *testing.T) {
		m := &MQ{opt: MQOption{MaxRetry: 7}}
		if got := m.maxRetry(); got != 7 {
			t.Fatalf("maxRetry = %d, want 7", got)
		}
	})

	t.Run("zero kept as default", func(t *testing.T) {
		m := &MQ{opt: MQOption{MaxRetry: 0}}
		if got := m.maxRetry(); got != defaultMaxRetry {
			t.Fatalf("zero maxRetry = %d, want default", got)
		}
	})
}

func TestRetryTTL(t *testing.T) {
	t.Parallel()

	t.Run("nil mq", func(t *testing.T) {
		var m *MQ
		if got := m.retryTTL(); got != defaultRetryTTL {
			t.Fatalf("nil retryTTL = %v, want %v", got, defaultRetryTTL)
		}
	})

	t.Run("custom", func(t *testing.T) {
		m := &MQ{opt: MQOption{RetryTTL: 5 * time.Second}}
		if got := m.retryTTL(); got != 5*time.Second {
			t.Fatalf("retryTTL = %v, want 5s", got)
		}
	})
}

func TestRetryBackoffDelayCapsAtThirtySeconds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 0, want: time.Second},
		{attempt: 1, want: 2 * time.Second},
		{attempt: 4, want: 16 * time.Second},
		{attempt: 5, want: 30 * time.Second},
		{attempt: 10, want: 30 * time.Second},
	}

	for _, tt := range tests {
		t.Run(time.Duration(tt.attempt).String(), func(t *testing.T) {
			t.Parallel()

			if got := retryBackoffDelay(tt.attempt); got != tt.want {
				t.Fatalf("retryBackoffDelay(%d) = %v, want %v", tt.attempt, got, tt.want)
			}
		})
	}
}

func TestExtractURLScheme(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		url  string
		want string
	}{
		{"amqp", "amqp://localhost:5672/", "amqp://"},
		{"amqps", "amqps://localhost:5671/", "amqps://"},
		{"no scheme", "localhost:5672", "localhost:5672"},
		{"empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := extractURLScheme(tt.url); got != tt.want {
				t.Fatalf("extractURLScheme(%q) = %q, want %q", tt.url, got, tt.want)
			}
		})
	}
}

func TestTotalSizeBytes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   [][]byte
		want int
	}{
		{"nil", nil, 0},
		{"empty", [][]byte{}, 0},
		{"single", [][]byte{{1, 2, 3}}, 3},
		{"multiple", [][]byte{{1, 2}, {3, 4, 5}}, 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := totalSizeBytes(tt.in); got != tt.want {
				t.Fatalf("totalSizeBytes = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestResolveDeliveryMode(t *testing.T) {
	t.Parallel()

	t.Run("nil", func(t *testing.T) {
		var m *MQ
		if got := m.resolveDeliveryMode(); got != amqp.Persistent {
			t.Fatalf("nil resolveDeliveryMode = %d, want %d", got, amqp.Persistent)
		}
	})

	t.Run("default persistent", func(t *testing.T) {
		m := &MQ{}
		if got := m.resolveDeliveryMode(); got != amqp.Persistent {
			t.Fatalf("default = %d, want persistent", got)
		}
	})

	t.Run("custom transient", func(t *testing.T) {
		m := &MQ{opt: MQOption{DeliveryMode: amqp.Transient}}
		if got := m.resolveDeliveryMode(); got != amqp.Transient {
			t.Fatalf("transient = %d, want %d", got, amqp.Transient)
		}
	})
}
