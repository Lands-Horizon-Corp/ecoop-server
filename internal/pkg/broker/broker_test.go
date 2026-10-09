package broker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/logger"
)

func startedLogger(t *testing.T) logger.LoggerService {
	t.Helper()
	log := logger.NewLoggerService("broker-test", "warn", "json")
	if err := log.Start(context.Background()); err != nil {
		t.Fatalf("start logger: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_ = log.Stop(ctx) // no collector in tests
	})
	return log
}

func noop(context.Context, []byte, []byte) error { return nil }

func TestValidation(t *testing.T) {
	ctx := context.Background()
	b := NewBrokerService([]string{"localhost:1"}, "group", startedLogger(t))

	if err := b.Publish(ctx, "t", nil, []byte("x")); !errors.Is(err, ErrNotRunning) {
		t.Errorf("Publish before Run: got %v", err)
	}
	if err := b.Stop(ctx); !errors.Is(err, ErrNotRunning) {
		t.Errorf("Stop before Run: got %v", err)
	}
	if err := b.Subscribe(ctx, "", noop); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("empty topic: got %v", err)
	}
	if err := b.Subscribe(ctx, "t", nil); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("nil handler: got %v", err)
	}
	if err := b.Subscribe(ctx, "t", noop); err != nil {
		t.Fatal(err)
	}
	if err := b.Subscribe(ctx, "t", noop); !errors.Is(err, ErrDuplicateSubscription) {
		t.Errorf("duplicate: got %v", err)
	}

	if err := NewBrokerService([]string{"localhost:1"}, "group", nil).Run(ctx); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("nil logger: got %v", err)
	}
	if err := NewBrokerService(nil, "group", startedLogger(t)).Run(ctx); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("no brokers: got %v", err)
	}
	if err := NewBrokerService([]string{"localhost:1"}, "group", logger.NewLoggerService("x", "", "")).Run(ctx); !errors.Is(err, logger.ErrNotStarted) {
		t.Errorf("logger not started: got %v", err)
	}
}

func TestSafeHandleRecoversPanic(t *testing.T) {
	err := safeHandle(context.Background(), func(context.Context, []byte, []byte) error { panic("boom") }, nil, nil)
	if err == nil {
		t.Fatal("want error from panic")
	}
}

// The tests below need a real Kafka: KAFKA_BROKER=localhost:9094 go test ./internal/pkg/broker/
func kafkaBroker(t *testing.T) string {
	addr := os.Getenv("KAFKA_BROKER")
	if addr == "" {
		t.Skip("KAFKA_BROKER not set")
	}
	return addr
}

func TestPublishSubscribe(t *testing.T) {
	addr := kafkaBroker(t)
	ctx := context.Background()
	topic := fmt.Sprintf("broker-test-%d", time.Now().UnixNano())

	b := NewBrokerService([]string{addr}, "broker-test-"+topic, startedLogger(t))
	got := make(chan string, 1)
	if err := b.Subscribe(ctx, topic, func(_ context.Context, key, value []byte) error {
		got <- string(key) + "=" + string(value)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.Run(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := b.Stop(context.Background()); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})

	if err := b.Publish(ctx, topic, []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-got:
		if m != "k=v" {
			t.Errorf("got %q", m)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("message not consumed")
	}
}

func TestRetryThenDeadLetter(t *testing.T) {
	addr := kafkaBroker(t)
	ctx := context.Background()
	topic := fmt.Sprintf("broker-test-dlq-%d", time.Now().UnixNano())

	b := NewBrokerService([]string{addr}, "broker-test-"+topic, startedLogger(t), WithRetries(2, 10*time.Millisecond))
	var attempts atomic.Int32
	if err := b.Subscribe(ctx, topic, func(context.Context, []byte, []byte) error {
		attempts.Add(1)
		return errors.New("always fails")
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.Run(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Stop(context.Background()) })

	// Subscribing after Run starts consuming right away.
	dead := make(chan string, 1)
	if err := b.Subscribe(ctx, topic+".dlq", func(_ context.Context, _, value []byte) error {
		dead <- string(value)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := b.Publish(ctx, topic, nil, []byte("poison")); err != nil {
		t.Fatal(err)
	}
	select {
	case v := <-dead:
		if v != "poison" {
			t.Errorf("dlq value = %q", v)
		}
		if n := attempts.Load(); n != 3 {
			t.Errorf("attempts = %d, want 3 (1 + 2 retries)", n)
		}
	case <-time.After(45 * time.Second):
		t.Fatalf("message never reached the DLQ (attempts %d)", attempts.Load())
	}
}
