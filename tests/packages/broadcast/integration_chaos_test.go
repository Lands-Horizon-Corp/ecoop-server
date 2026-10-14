//go:build integration

package broadcast_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/broadcast"
)

// Integration chaos: several app instances publishing to many browsers at
// once, browsers coming and going.

// Four instances publish numbered events to shared channels at random; every
// subscriber receives exactly the events of its channels, each once.
func TestIntegrationChaos_ManyPublishersManySubscribers(t *testing.T) {
	t.Parallel()
	requireServer(t)
	chs := []string{channel(), channel(), channel()}
	subs := make([]*subscriber, len(chs))
	for i, c := range chs {
		subs[i] = connect(t)
		subs[i].subscribe(t, c)
	}
	instances := make([]broadcast.IBroadcastService, 4)
	for i := range instances {
		instances[i] = started(t)
	}
	seed := uint64(time.Now().UnixNano())
	var mu sync.Mutex
	sent := map[string]map[string]bool{} // channel -> event ids
	for _, c := range chs {
		sent[c] = map[string]bool{}
	}
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Go(func() {
			r := rand.New(rand.NewPCG(seed, uint64(w)))
			for i := range 25 {
				c := chs[r.IntN(len(chs))]
				id := fmt.Sprintf("%d-%d", w, i)
				if err := instances[r.IntN(len(instances))].Publish(ctxT(t), c, "tick", map[string]string{"id": id}); err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				sent[c][`{"id":"`+id+`"}`] = true
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	for i, c := range chs {
		got := map[string]int{}
		for _, m := range subs[i].expect(t, len(sent[c]), 20*time.Second) {
			if m.Channel != c {
				t.Fatalf("subscriber of %s received %s's event", c, m.Channel)
			}
			got[m.Data]++
		}
		for data := range sent[c] {
			if got[data] != 1 {
				t.Fatalf("%s received %s %d times", c, data, got[data])
			}
		}
		subs[i].quiet(t, 300*time.Millisecond)
	}
}

// A browser that drops and reconnects receives what is sent after it is
// back (Pusher channels do not replay history).
func TestIntegrationChaos_SubscriberReconnects(t *testing.T) {
	t.Parallel()
	b := started(t)
	ch := channel()
	first := connect(t)
	first.subscribe(t, ch)
	_ = b.Publish(ctxT(t), ch, "one", 1)
	first.expect(t, 1, 10*time.Second)
	_ = first.conn.CloseNow()
	_ = b.Publish(ctxT(t), ch, "while.away", 2)
	second := connect(t)
	second.subscribe(t, ch)
	_ = b.Publish(ctxT(t), ch, "two", 3)
	if m := second.expect(t, 1, 10*time.Second)[0]; m.Event != "two" {
		t.Fatalf("after reconnecting received %+v", m)
	}
}

// One instance stopping mid-traffic: its own publishes finish or are
// refused cleanly, the others carry on.
func TestIntegrationChaos_OneInstanceStops(t *testing.T) {
	t.Parallel()
	a, b := started(t), started(t)
	ch := channel()
	sub := connect(t)
	sub.subscribe(t, ch)
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 50 {
			_ = a.Publish(context.Background(), ch, "from.a", 1)
		}
	})
	time.Sleep(20 * time.Millisecond)
	_ = a.Stop(context.Background())
	wg.Wait()
	if err := b.Publish(ctxT(t), ch, "from.b", 2); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if m, ok := sub.next(time.Until(deadline)); ok && m.Event == "from.b" {
			return
		}
	}
	t.Fatal("the instance still running could not publish")
}
