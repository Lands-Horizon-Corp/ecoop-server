//go:build integration

package broker_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/broker"
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

// Many poison messages failing at once: each is parked.
func TestRace_ConcurrentDeadLetters(t *testing.T) {
	t.Parallel()
	topic, group := names()
	b := service(t, group)
	_ = b.Subscribe(ctxT(t), topic, func(context.Context, []byte, []byte) error { return errors.New("no") })
	const n = 30
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() { _ = b.Publish(ctxT(t), topic, nil, []byte(fmt.Sprint(i))) })
	}
	wg.Wait()
	seen := map[string]bool{}
	for _, r := range readAll(t, topic+broker.DLQSuffix, n) {
		seen[string(r.Value)] = true
	}
	if len(seen) != n {
		t.Fatalf("%d distinct dead letters, want %d", len(seen), n)
	}
}
