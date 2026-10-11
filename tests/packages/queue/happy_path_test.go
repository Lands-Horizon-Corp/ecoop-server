//go:build integration

package queue_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/queue"
)

func TestHappyPath_TypedJob(t *testing.T) {
	t.Parallel()
	q := newQueue(t)
	type invoice struct {
		ID     int     `json:"id"`
		Amount float64 `json:"amount"`
		Tags   []string
	}
	got := make(chan invoice, 1)
	job := queue.NewJob(q, taskType(t, "invoice"), func(_ context.Context, v invoice) error {
		got <- v
		return nil
	})
	want := invoice{ID: 7, Amount: 1250.5, Tags: []string{"loan", "cebu"}}
	if err := job.Deliver(ctxT(t), want); err != nil {
		t.Fatal(err)
	}
	select {
	case v := <-got:
		if v.ID != want.ID || v.Amount != want.Amount || len(v.Tags) != 2 {
			t.Fatalf("got %+v, want %+v", v, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("job never ran")
	}
}

func TestHappyPath_RawBytesPayload(t *testing.T) {
	t.Parallel()
	q := newQueue(t)
	var r recorder
	typ := taskType(t, "raw")
	q.Handle(typ, r.handler(nil))
	raw := []byte{0x00, 0xff, 'x'}
	if _, err := q.Enqueue(ctxT(t), typ, raw); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, func() bool { return r.count() == 1 }, "job did not run")
	if p, _ := r.snapshot(); p[0] != string(raw) {
		t.Fatalf("[]byte payload altered: %q", p[0])
	}
}

func TestHappyPath_EnqueueReturnsDistinctIDs(t *testing.T) {
	t.Parallel()
	q := newQueue(t)
	typ := taskType(t, "ids")
	q.Handle(typ, (&recorder{}).handler(nil))
	seen := map[string]bool{}
	for range 20 {
		id, err := q.Enqueue(ctxT(t), typ, nil)
		if err != nil || id == "" || seen[id] {
			t.Fatalf("Enqueue = %q, %v (duplicate: %v)", id, err, seen[id])
		}
		seen[id] = true
	}
}

func TestHappyPath_AllPriorities(t *testing.T) {
	t.Parallel()
	q := newQueue(t)
	var r recorder
	typ := taskType(t, "prio")
	q.Handle(typ, r.handler(nil))
	for _, p := range []string{queue.Critical, queue.Default, queue.Low} {
		if _, err := q.Enqueue(ctxT(t), typ, p, queue.Priority(p)); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, 10*time.Second, func() bool { return r.count() == 3 }, "not every priority was served")
}

func TestHappyPath_HandlerReplaced(t *testing.T) {
	t.Parallel()
	q := newQueue(t)
	var first, second recorder
	typ := taskType(t, "replace")
	q.Handle(typ, first.handler(nil))
	q.Handle(typ, second.handler(nil))
	if _, err := q.Enqueue(ctxT(t), typ, nil); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, func() bool { return second.count() == 1 }, "latest handler did not run")
	if first.count() != 0 {
		t.Fatal("replaced handler still ran")
	}
}

func TestHappyPath_Delay(t *testing.T) {
	t.Parallel()
	q := newQueue(t)
	var r recorder
	typ := taskType(t, "delay")
	q.Handle(typ, r.handler(nil))
	start := time.Now()
	if _, err := q.Enqueue(ctxT(t), typ, nil, queue.Delay(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, func() bool { return r.count() == 1 }, "delayed job never ran")
	// asynq stores the run time in whole seconds, so a delay can end up to
	// 1s early; it must never run with no delay at all.
	if _, times := r.snapshot(); times[0].Sub(start) < time.Second {
		t.Fatalf("ran after %v, well before its 2s delay", times[0].Sub(start))
	}
}

func TestHappyPath_UniqueRejectsDuplicate(t *testing.T) {
	t.Parallel()
	q := newQueue(t)
	typ := taskType(t, "unique")
	q.Handle(typ, (&recorder{}).handler(nil))
	opts := []queue.Option{queue.Unique(time.Minute), queue.Delay(time.Minute)}
	if _, err := q.Enqueue(ctxT(t), typ, "same", opts...); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue(ctxT(t), typ, "same", opts...); err == nil {
		t.Fatal("duplicate accepted within the Unique window")
	}
	if _, err := q.Enqueue(ctxT(t), typ, "other", opts...); err != nil {
		t.Fatalf("different payload rejected: %v", err)
	}
}

func TestHappyPath_RetriesUntilSuccess(t *testing.T) {
	t.Parallel()
	q := newQueue(t)
	var calls int
	done := make(chan struct{})
	typ := taskType(t, "flaky")
	q.Handle(typ, func(context.Context, []byte) error {
		calls++
		if calls < 3 {
			return errors.New("not yet")
		}
		close(done)
		return nil
	})
	if _, err := q.Enqueue(ctxT(t), typ, nil, queue.MaxRetry(5)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done: // retries at 1s then 2s
	case <-time.After(15 * time.Second):
		t.Fatalf("succeeded after %d calls, want 3", calls)
	}
}

func TestHappyPath_RetryAfterDelaysWithoutFailing(t *testing.T) {
	t.Parallel()
	q := newQueue(t)
	var r recorder
	typ := taskType(t, "later")
	first := true
	q.Handle(typ, func(ctx context.Context, p []byte) error {
		_ = r.handler(nil)(ctx, p)
		if first {
			first = false
			return queue.RetryAfter(500*time.Millisecond, errors.New("busy"))
		}
		return nil
	})
	if _, err := q.Enqueue(ctxT(t), typ, nil, queue.MaxRetry(0)); err != nil {
		t.Fatal(err)
	}
	// MaxRetry(0) would archive a real failure; RetryAfter must not count.
	eventually(t, 10*time.Second, func() bool { return r.count() == 2 }, "RetryAfter job did not run again")
	_, times := r.snapshot()
	if gap := times[1].Sub(times[0]); gap < 500*time.Millisecond {
		t.Fatalf("retried after %v, before its 500ms RetryAfter", gap)
	}
	if n := archived(t, queue.Default); n != 0 {
		t.Fatalf("RetryAfter counted as failure: %d archived", n)
	}
}

func TestHappyPath_RateLimit(t *testing.T) {
	t.Parallel()
	q := newQueue(t)
	var r recorder
	typ := taskType(t, "limited")
	q.Handle(typ, r.handler(nil), queue.RateLimit(3, time.Second))
	const n = 12
	for i := range n {
		if _, err := q.Enqueue(ctxT(t), typ, i); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, 20*time.Second, func() bool { return r.count() == n }, "not every rate-limited job ran")
	_, times := r.snapshot()
	// n jobs at 3 per window span at least n/3-2 windows (edges straddle).
	if span, min := times[n-1].Sub(times[0]), time.Duration(n/3-2)*time.Second; span < min {
		t.Fatalf("%d jobs ran in %v; the limit allows no faster than %v", n, span, min)
	}
}

func TestHappyPath_ScheduleAndUnschedule(t *testing.T) {
	t.Parallel()
	q := newQueue(t)
	var r recorder
	typ := taskType(t, "cron")
	q.Handle(typ, r.handler(nil))
	id, err := q.Schedule("@every 1s", typ, "tick")
	if err != nil || id == "" {
		t.Fatalf("Schedule = %q, %v", id, err)
	}
	// The first tick waits for leadership (taken at Start) and the 1s spec.
	eventually(t, 10*time.Second, func() bool { return r.count() >= 2 }, "schedule did not fire twice")
	if err := q.Unschedule(id); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond) // a tick already queued may still run
	after := r.count()
	never(t, 2500*time.Millisecond, func() bool { return r.count() > after }, "schedule kept firing after Unschedule")
	if p, _ := r.snapshot(); p[0] != `"tick"` {
		t.Fatalf("scheduled payload = %q", p[0])
	}
}

func TestHappyPath_RestartKeepsWorking(t *testing.T) {
	t.Parallel()
	q := newQueue(t)
	var r recorder
	typ := taskType(t, "restart")
	q.Handle(typ, r.handler(nil))
	for i := range 3 {
		if err := q.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := q.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, err := q.Enqueue(ctxT(t), typ, fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, 10*time.Second, func() bool { return r.count() == 3 }, "jobs lost across restarts")
}
