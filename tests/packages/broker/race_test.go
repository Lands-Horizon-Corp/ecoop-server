//go:build integration

package broker_test

import (
	"fmt"
	"sync"
	"testing"
)

// Many goroutines publishing at once: every message arrives. Run with -race.
func TestRace_ConcurrentPublish(t *testing.T) {
	t.Parallel()
	topic, group := names()
	b := service(t, group)
	var in inbox
	_ = b.Subscribe(ctxT(t), topic, in.handle)
	var wg sync.WaitGroup
	for w := range 10 {
		wg.Go(func() {
			for i := range 10 {
				if err := b.Publish(ctxT(t), topic, nil, []byte(fmt.Sprintf("%d-%d", w, i))); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	seen := map[string]bool{}
	for _, m := range in.wait(t, 100) {
		seen[m] = true
	}
	if len(seen) != 100 {
		t.Fatalf("%d distinct messages, want 100", len(seen))
	}
}
