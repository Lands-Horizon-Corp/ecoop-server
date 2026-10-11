//go:build integration

package broadcast_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/broadcast"
)

// Sync & race: one service used from many goroutines; Stop racing the
// publishes it must wait for. Run with -race.

func TestRace_ConcurrentPublishesAllArrive(t *testing.T) {
	t.Parallel()
	b := started(t)
	ch := channel()
	sub := connect(t)
	sub.subscribe(t, ch)
	const workers, each = 16, 10
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for i := range each {
				if err := b.Publish(ctxT(t), ch, "e", fmt.Sprintf("%d-%d", w, i)); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	seen := map[string]bool{}
	for _, m := range sub.expect(t, workers*each, 20*time.Second) {
		if seen[m.Data] {
			t.Fatalf("%s arrived twice", m.Data)
		}
		seen[m.Data] = true
	}
}

// Stop waits for a publish in flight: it arrives, and Stop returns after.
func TestRace_StopWaitsForAPublish(t *testing.T) {
	t.Parallel()
	b, p := viaProxy(t)
	ch := channel()
	sub := connect(t)
	sub.subscribe(t, ch)
	p.SetDelay(400 * time.Millisecond) // each way: the request takes 800ms or more
	done := make(chan error, 1)
	go func() { done <- b.Publish(context.Background(), ch, "slow", 1) }()
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	if err := b.Stop(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 500*time.Millisecond {
		t.Fatalf("Stop returned after %v, before the publish in flight could finish", d)
	}
	if err := <-done; err != nil {
		t.Fatalf("publish cut off by Stop: %v", err)
	}
	sub.expect(t, 1, 10*time.Second)
}

func TestRace_StopGivesUpWhenCtxEnds(t *testing.T) {
	t.Parallel()
	b, p := viaProxy(t)
	p.SetDelay(3 * time.Second)
	go func() { _ = b.Publish(context.Background(), channel(), "slow", 1) }()
	time.Sleep(100 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := b.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop = %v, want DeadlineExceeded", err)
	}
}

// Runner stops by itself when the service stops.
func TestRace_RunnerEndsWithTheService(t *testing.T) {
	t.Parallel()
	b := started(t)
	done := make(chan error, 1)
	go func() { done <- b.Runner(context.Background()) }()
	time.Sleep(1500 * time.Millisecond)
	_ = b.Stop(ctxT(t))
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Runner = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Runner kept going after Stop")
	}
}

// Start and Stop churn under traffic: calls either work or say the service
// is stopped; nothing panics.
func TestRace_LifecycleChurnWithTraffic(t *testing.T) {
	t.Parallel()
	b := started(t)
	ch := channel()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				mustNotPanic(t, "publish during churn", func() {
					if err := b.Publish(ctxT(t), ch, "e", 1); err != nil && !errors.Is(err, broadcast.ErrNotStarted) {
						t.Errorf("Publish: %v", err)
					}
				})
				time.Sleep(2 * time.Millisecond)
			}
		})
	}
	for i := range 20 {
		if i%2 == 0 {
			_ = b.Stop(ctxT(t))
		} else if err := b.Start(ctxT(t)); err != nil {
			t.Error(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
}
