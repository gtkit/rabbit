package rabbit

import (
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
