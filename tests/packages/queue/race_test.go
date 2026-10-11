//go:build integration

package queue_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/queue"
)

// Sync & race: one queue used from many goroutines. Run with -race.

const workers = 32

func TestRace_ConcurrentEnqueue(t *testing.T) {
	t.Parallel()
	q := newQueue(t)
	typ := taskType(t, "concurrent")
	var r recorder
	q.Handle(typ, r.handler(nil))
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for i := range 10 {
				if _, err := q.Enqueue(ctxT(t), typ, fmt.Sprint(w, "-", i)); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	eventually(t, 20*time.Second, func() bool { return r.count() == workers*10 }, "not every job ran")
}

func TestRace_HandleWhileDispatching(t *testing.T) {
	t.Parallel()
	q := newQueue(t)
	typ := taskType(t, "swap")
	var ran atomic.Int64
	h := func(context.Context, []byte) error { ran.Add(1); return nil }
	q.Handle(typ, h)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				q.Handle(typ, h, queue.RateLimit(1000, time.Second))
			}
		}
	})
	for i := range 50 {
		if _, err := q.Enqueue(ctxT(t), typ, i); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, 15*time.Second, func() bool { return ran.Load() == 50 }, "jobs lost while the handler was being replaced")
	close(stop)
	wg.Wait()
}

func TestRace_LifecycleChurn(t *testing.T) {
	t.Parallel()
	q := newQueue(t)
	typ := taskType(t, "churn")
	q.Handle(typ, (&recorder{}).handler(nil))
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
				mustNotPanic(t, "ops during churn", func() {
					if _, err := q.Enqueue(context.Background(), typ, nil); err != nil && !errors.Is(err, queue.ErrNotStarted) {
						t.Errorf("Enqueue: %v", err)
					}
					_ = q.Ping(context.Background())
				})
				time.Sleep(time.Millisecond)
			}
		})
	}
	for range 10 {
		_ = q.Stop(context.Background())
		_ = q.Start(context.Background())
	}
	close(stop)
	wg.Wait()
}

func TestRace_ConcurrentScheduleAndUnschedule(t *testing.T) {
	t.Parallel()
	q := newQueue(t)
	typ := taskType(t, "sched")
	q.Handle(typ, (&recorder{}).handler(nil))
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for range 20 {
				id, err := q.Schedule("@every 1h", typ, nil)
				if err != nil {
					t.Error(err)
					return
				}
				if err := q.Unschedule(id); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
}

func TestRace_ConcurrentStartAndStop(t *testing.T) {
	t.Parallel()
	q := newQueue(t)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			if i%2 == 0 {
				_ = q.Stop(context.Background())
			} else {
				_ = q.Start(context.Background())
			}
		})
	}
	wg.Wait()
	_ = q.Start(context.Background())
	if err := q.Ping(context.Background()); err != nil {
		t.Fatalf("queue unusable after concurrent Start/Stop: %v", err)
	}
}
