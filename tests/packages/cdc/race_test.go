//go:build integration

package cdc_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
)

// Many goroutines publishing and the handler running at once: every change
// arrives. Run with -race.
func TestRace_ConcurrentChanges(t *testing.T) {
	t.Parallel()
	topic := names()
	s := service(t)
	var in inbox
	_ = s.Subscribe(ctxT(t), topic, in.handle)
	p := producer(t)
	var wg sync.WaitGroup
	for w := range 10 {
		wg.Go(func() {
			for i := range 10 {
				id := w*10 + i
				if err := p.Publish(ctxT(t), topic, key(id), event(cdc.OpCreate, id, "n")); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	seen := map[int64]bool{}
	for _, m := range in.wait(t, 100) {
		seen[m.ID] = true
	}
	if len(seen) != 100 {
		t.Fatalf("%d distinct changes, want 100", len(seen))
	}
}

// Many failing changes at once: each is parked.
func TestRace_ConcurrentDeadLetters(t *testing.T) {
	t.Parallel()
	topic := names()
	s := service(t)
	_ = s.Subscribe(ctxT(t), topic, func(context.Context, json.RawMessage, cdc.ChangeEvent) error { return errors.New("no") })
	const n = 30
	p := producer(t)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() { _ = p.Publish(ctxT(t), topic, key(i), event(cdc.OpCreate, i, "x")) })
	}
	wg.Wait()
	seen := map[string]bool{}
	for _, r := range readAll(t, topic+".dlq", n) {
		seen[string(r.Key)] = true
	}
	if len(seen) != n {
		t.Fatalf("%d distinct dead letters, want %d", len(seen), n)
	}
}
