//go:build integration

package queue_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/queue"
)

// Integration chaos: several instances on one Valkey, as behind a load
// balancer: shared work, shared rate limits, one cron leader with handover.

func TestIntegrationChaos_InstancesShareTheWork(t *testing.T) {
	t.Parallel()
	a, b := newQueue(t), newQueue(t)
	typ := taskType(t, "shared")
	var byA, byB atomic.Int64
	var mu sync.Mutex
	seen := map[int]int{}
	handler := func(counter *atomic.Int64) queue.Handler {
		return queue.JSON(func(_ context.Context, id int) error {
			counter.Add(1)
			mu.Lock()
			seen[id]++
			mu.Unlock()
			// Long enough that one instance cannot drain the queue alone
			// between the other's polls.
			time.Sleep(100 * time.Millisecond)
			return nil
		})
	}
	a.Handle(typ, handler(&byA))
	b.Handle(typ, handler(&byB))
	const n = 60
	for i := range n {
		src := a
		if i%2 == 1 {
			src = b
		}
		if _, err := src.Enqueue(ctxT(t), typ, i); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, 20*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(seen) == n }, "not every job ran")
	mu.Lock()
	for id, c := range seen {
		if c != 1 {
			t.Errorf("job %d ran %d times", id, c)
		}
	}
	mu.Unlock()
	if byA.Load() == 0 || byB.Load() == 0 {
		t.Errorf("work not shared: a ran %d, b ran %d", byA.Load(), byB.Load())
	}
}

func TestIntegrationChaos_RateLimitSharedAcrossInstances(t *testing.T) {
	t.Parallel()
	a, b := newQueue(t), newQueue(t)
	typ := taskType(t, "shared-limit")
	var r recorder
	a.Handle(typ, r.handler(nil), queue.RateLimit(4, time.Second))
	b.Handle(typ, r.handler(nil), queue.RateLimit(4, time.Second))
	const n = 16
	for i := range n {
		if _, err := a.Enqueue(ctxT(t), typ, i); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, 20*time.Second, func() bool { return r.count() == n }, "not every job ran")
	_, times := r.snapshot()
	first, last := times[0], times[0]
	for _, tm := range times {
		first, last = minTime(first, tm), maxTime(last, tm)
	}
	if span, min := last.Sub(first), time.Duration(n/4-2)*time.Second; span < min {
		t.Fatalf("two instances ran %d jobs in %v; a shared limit allows no faster than %v", n, span, min)
	}
}

func TestIntegrationChaos_JobFindsTheInstanceWithItsHandler(t *testing.T) {
	t.Parallel()
	without, with := newQueue(t), newQueue(t)
	typ := taskType(t, "routed")
	var r recorder
	with.Handle(typ, r.handler(nil))
	for i := range 10 {
		if _, err := without.Enqueue(ctxT(t), typ, i); err != nil {
			t.Fatal(err)
		}
	}
	// Each job lands on either instance at random and bounces UnhandledRetry
	// when it hits the wrong one, so allow for several bounces.
	eventually(t, 12*queue.UnhandledRetry, func() bool { return r.count() == 10 }, "jobs did not reach the instance with the handler")
	if n := archived(t, queue.Default); n != 0 {
		t.Fatalf("%d jobs archived while bouncing between instances", n)
	}
}

func TestIntegrationChaos_OneCronLeaderAndHandover(t *testing.T) {
	t.Parallel()
	a, b := newQueue(t), newQueue(t)
	typ := taskType(t, "tick")
	var r recorder
	for _, q := range []queue.QueueServices{a, b} {
		q.Handle(typ, r.handler(nil))
		if _, err := q.Schedule("@every 1s", typ, nil); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(4 * time.Second)
	if n := r.count(); n > 6 {
		t.Fatalf("%d ticks in 4s from two instances: both are enqueueing (want one leader)", n)
	}
	if r.count() == 0 {
		t.Fatal("no instance led the schedule")
	}
	// Stop both in turn; whichever was leading, the other takes over within
	// LeaderRenew because Stop resigns the lock.
	_ = a.Stop(context.Background())
	before := r.count()
	eventually(t, queue.LeaderRenew+3*time.Second, func() bool { return r.count() >= before+2 }, "schedule stopped after one instance left")
}

func TestIntegrationChaos_DeliverFromManyInstances(t *testing.T) {
	t.Parallel()
	type msg struct{ From, N int }
	var mu sync.Mutex
	got := map[string]bool{}
	typ := taskType(t, "deliver")
	var jobs []*queue.Job[msg]
	for range 3 {
		q := newQueue(t)
		jobs = append(jobs, queue.NewJob(q, typ, func(_ context.Context, m msg) error {
			mu.Lock()
			got[fmt.Sprint(m.From, "-", m.N)] = true
			mu.Unlock()
			return nil
		}, queue.Priority(queue.Critical), queue.MaxRetry(3)))
	}
	var wg sync.WaitGroup
	for i, job := range jobs {
		wg.Go(func() {
			for n := range 20 {
				if err := job.Deliver(ctxT(t), msg{From: i, N: n}); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	eventually(t, 20*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(got) == 60 }, "not every delivered job ran")
}

func minTime(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}
