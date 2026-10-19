//go:build integration

package broker_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/broker"
)

// A message whose handler panics or keeps failing is retried, then skipped:
// it cannot crash the consumer or block the topic.
func TestPoison_BadMessageDoesNotBlockTheTopic(t *testing.T) {
	t.Parallel()
	topic, group := names()
	b := service(t, group)
	var poisonTries atomic.Int64
	var in inbox
	_ = b.Subscribe(ctxT(t), topic, func(ctx context.Context, key, value []byte) error {
		if string(value) == "poison" {
			poisonTries.Add(1)
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
	time.Sleep(100 * time.Millisecond)
	if n := poisonTries.Load(); n != 3 {
		t.Fatalf("poison message tried %d times, want 3", n)
	}
}

// Binary, empty and large payloads arrive byte for byte.
func TestPoison_Payloads(t *testing.T) {
	t.Parallel()
	topic, group := names()
	b := service(t, group)
	got := make(chan []byte, 3)
	_ = b.Subscribe(ctxT(t), topic, func(_ context.Context, _, v []byte) error { got <- append([]byte(nil), v...); return nil })
	payloads := [][]byte{{0x00, 0xff, '\n', 0x7f}, {}, bytes.Repeat([]byte("x"), 500<<10)}
	for _, p := range payloads {
		if err := b.Publish(ctxT(t), topic, nil, p); err != nil {
			t.Fatal(err)
		}
	}
	for i, want := range payloads {
		select {
		case v := <-got:
			if !bytes.Equal(v, want) {
				t.Fatalf("payload %d changed (%d bytes, want %d)", i, len(v), len(want))
			}
		case <-time.After(45 * time.Second):
			t.Fatalf("payload %d never arrived", i)
		}
	}
}

// A poison message is parked on the DLQ with its key, value, origin and
// reason; the messages around it are still delivered.
func TestPoison_ParkedOnDLQ(t *testing.T) {
	t.Parallel()
	topic, group := names()
	b := service(t, group)
	var in inbox
	_ = b.Subscribe(ctxT(t), topic, func(ctx context.Context, key, value []byte) error {
		if string(value) == "poison" {
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
}

// Binary payloads survive the trip to the DLQ byte for byte.
func TestPoison_DLQPayloads(t *testing.T) {
	t.Parallel()
	topic, group := names()
	b := service(t, group)
	_ = b.Subscribe(ctxT(t), topic, func(context.Context, []byte, []byte) error { return errors.New("no") })
	want := []byte{0x00, 0xff, '\n', 0x7f}
	_ = b.Publish(ctxT(t), topic, nil, want)
	if dead := readDLQ(t, topic+broker.DLQSuffix); !bytes.Equal(dead.Value, want) {
		t.Fatalf("dead letter = %v", dead.Value)
	}
}
