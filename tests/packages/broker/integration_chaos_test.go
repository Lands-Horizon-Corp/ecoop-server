//go:build integration

package broker_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/broker"
)

// Two instances in one group: when the one consuming stops mid-stream, the
// other takes over and nothing is lost (at least once).
func TestIntegrationChaos_InstanceStopsMidStream(t *testing.T) {
	t.Parallel()
	topic, group := names()
	var in inbox
	a := broker.NewBrokerService([]string{addr}, group, nil)
	_ = a.Subscribe(ctxT(t), topic, func(ctx context.Context, k, v []byte) error {
		time.Sleep(20 * time.Millisecond) // a slow worker
		return in.handle(ctx, k, v)
	})
	if err := a.Run(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	sender := service(t, group+"-sender")
	const n = 40
	publish(t, sender, topic, n, "k")
	for len(in.all()) < 5 {
		time.Sleep(10 * time.Millisecond)
	}
	_ = a.Stop(context.Background())
	b := service(t, group)
	_ = b.Subscribe(ctxT(t), topic, in.handle)
	in.wait(t, n) // duplicates are allowed, gaps are not
	seen := map[string]bool{}
	for _, m := range in.all() {
		seen[m] = true
	}
	for i := range n {
		if !seen[fmt.Sprintf("k=%d", i)] {
			t.Fatalf("message %d was lost when the instance stopped", i)
		}
	}
}

// The instance that dead-letters a poison message stops; the next one takes
// over, delivers the rest, and the poison message is on the DLQ exactly once
// (it was committed after being parked, so it is not replayed).
func TestIntegrationChaos_PoisonThenFailover(t *testing.T) {
	t.Parallel()
	topic, group := names()
	var in inbox
	handler := func(ctx context.Context, k, v []byte) error {
		if string(v) == "poison" {
			return errors.New("bad row")
		}
		return in.handle(ctx, k, v)
	}
	a := broker.NewBrokerService([]string{addr}, group, nil)
	_ = a.Subscribe(ctxT(t), topic, handler)
	if err := a.Run(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	sender := service(t, group+"-sender")
	_ = sender.Publish(ctxT(t), topic, []byte("k"), []byte("poison"))
	readDLQ(t, topic+broker.DLQSuffix)
	_ = a.Stop(context.Background())
	b := service(t, group)
	_ = b.Subscribe(ctxT(t), topic, handler)
	publish(t, sender, topic, 5, "k")
	in.wait(t, 5)
	if n := countFor(t, topic+broker.DLQSuffix, 3*time.Second); n != 1 {
		t.Fatalf("poison message dead-lettered %d times, want 1", n)
	}
}
