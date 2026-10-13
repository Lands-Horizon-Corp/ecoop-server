//go:build integration

package broker_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/broker"
)

// readDLQ returns the first record on topic, read by a throwaway group.
func readDLQ(t *testing.T, topic string) *kgo.Record {
	t.Helper()
	c, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	for {
		fetches := c.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("nothing arrived on %s", topic)
		}
		var first *kgo.Record
		fetches.EachRecord(func(r *kgo.Record) {
			if first == nil {
				first = r
			}
		})
		if first != nil {
			return first
		}
	}
}

func header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// A message that never succeeds lands on <topic>.dlq with where it came
// from and why it failed; the good messages around it are still delivered.
func TestDLQ_ExhaustedMessageIsParked(t *testing.T) {
	t.Parallel()
	topic, group := names()
	b := service(t, group)
	var in inbox
	var tries atomic.Int64
	_ = b.Subscribe(ctxT(t), topic, func(ctx context.Context, key, value []byte) error {
		if string(value) == "poison" {
			tries.Add(1)
			panic("cannot parse")
		}
		return in.handle(ctx, key, value)
	})
	for _, v := range []string{"a", "poison", "b"} {
		_ = b.Publish(ctxT(t), topic, []byte("k"), []byte(v))
	}
	if got := in.wait(t, 2); got[0] != "k=a" || got[1] != "k=b" {
		t.Fatalf("received %v", got)
	}
	dead := readDLQ(t, topic+broker.DLQSuffix)
	if string(dead.Key) != "k" || string(dead.Value) != "poison" {
		t.Fatalf("dead letter = %s/%s", dead.Key, dead.Value)
	}
	if header(dead, broker.HeaderDLQTopic) != topic || header(dead, broker.HeaderDLQGroup) != group ||
		header(dead, broker.HeaderDLQAttempts) != "3" || !strings.Contains(header(dead, broker.HeaderDLQError), "panicked") {
		t.Fatalf("headers = %+v", dead.Headers)
	}
	if n := tries.Load(); n != 3 {
		t.Fatalf("tried %d times, want 3", n)
	}
}

// The suffix and retry count come from options (so from BROKER_* env).
func TestDLQ_Options(t *testing.T) {
	t.Parallel()
	if skipReason != "" {
		t.Skip(skipReason)
	}
	topic, group := names()
	b := broker.NewBrokerService([]string{addr}, group, nil,
		broker.WithDLQSuffix(".dead"), broker.WithRetry(1, time.Millisecond))
	if err := b.Run(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Stop(context.Background()) })
	var tries atomic.Int64
	_ = b.Subscribe(ctxT(t), topic, func(context.Context, []byte, []byte) error {
		tries.Add(1)
		return context.Canceled
	})
	_ = b.Publish(ctxT(t), topic, []byte("k"), []byte("v"))
	dead := readDLQ(t, topic+".dead")
	if string(dead.Value) != "v" || tries.Load() != 1 {
		t.Fatalf("value=%s tries=%d", dead.Value, tries.Load())
	}
}

func TestFromEnv(t *testing.T) {
	t.Parallel()
	env := map[string]string{"BROKER_HOST": "h", "BROKER_PORT": "1", "BROKER_HANDLER_ATTEMPTS": "x"}
	get := func(k string) string { return env[k] }
	if _, err := broker.NewBrokerServiceFromEnv(get, nil); err == nil {
		t.Fatal("bad BROKER_HANDLER_ATTEMPTS accepted")
	}
	env["BROKER_HANDLER_ATTEMPTS"] = "2"
	if _, err := broker.NewBrokerServiceFromEnv(get, nil); err != nil {
		t.Fatal(err)
	}
}
