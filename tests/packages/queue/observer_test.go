//go:build integration

package queue_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/queue"
	"github.com/Lands-Horizon-Corp/ecoop-server/tests/internal/obstest"
)

func observedQueue(t testing.TB) (queue.IQueueService, *observability.Service) {
	t.Helper()
	requireValkey(t)
	o := obstest.New(t)
	q := queue.NewQueueService(newCache(t, valkeyAddr), queue.WithNamespace(ns(t)), queue.WithObserver(o))
	if err := q.Start(context.Background()); err != nil {
		t.Fatalf("queue Start: %v", err)
	}
	t.Cleanup(func() { _ = q.Stop(context.Background()) })
	return q, o
}

func queueCount(o *observability.Service, op, outcome string) float64 {
	return obstest.Count(o, "queue", op, outcome)
}

func waitQueueCount(t testing.TB, o *observability.Service, op, outcome string, want float64) {
	t.Helper()
	eventually(t, 15*time.Second, func() bool { return queueCount(o, op, outcome) >= want },
		"%s/%s to reach %v (now %v)", op, outcome, want, queueCount(o, op, outcome))
}

// Enqueueing and running a job are each one counted, timed operation.
func TestObserver_EnqueueAndRunAreOperations(t *testing.T) {
	q, o := observedQueue(t)
	task := taskType(t, "ok")
	var ran atomic.Int64
	q.Handle(task, func(context.Context, []byte) error { ran.Add(1); return nil })
	for range 3 {
		if _, err := q.Enqueue(ctxT(t), task, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, 15*time.Second, func() bool { return ran.Load() == 3 }, "3 runs")
	if got := queueCount(o, "enqueue", "success"); got != 3 {
		t.Fatalf("enqueue success = %v, want 3", got)
	}
	waitQueueCount(t, o, "run", "success", 3)
}

// A job that fails is an error of the run; asking to run again later is a retry,
// not a failure, so it never raises the error rate.
func TestObserver_RetryAfterIsARetryNotAnError(t *testing.T) {
	q, o := observedQueue(t)
	task := taskType(t, "later")
	var calls atomic.Int64
	q.Handle(task, func(context.Context, []byte) error {
		if calls.Add(1) == 1 {
			return queue.RetryAfter(time.Second, errors.New("not yet"))
		}
		return nil
	})
	if _, err := q.Enqueue(ctxT(t), task, []byte("x")); err != nil {
		t.Fatal(err)
	}
	waitQueueCount(t, o, "run", "retry", 1)
	waitQueueCount(t, o, "run", "success", 1)
	if got := queueCount(o, "run", "error"); got != 0 {
		t.Fatalf("a RetryAfter counted as %v errors", got)
	}
}

func TestObserver_AFailingJobIsAnError(t *testing.T) {
	q, o := observedQueue(t)
	task := taskType(t, "fail")
	q.Handle(task, func(context.Context, []byte) error { return errors.Join(errors.New("bad"), queue.ErrSkipRetry) })
	if _, err := q.Enqueue(ctxT(t), task, []byte("x")); err != nil {
		t.Fatal(err)
	}
	waitQueueCount(t, o, "run", "error", 1)
}

// A job type this instance has no handler for goes back for another instance:
// that is skipped, not an error.
func TestObserver_NoHandlerIsSkipped(t *testing.T) {
	q, o := observedQueue(t)
	task := taskType(t, "nobody")
	if _, err := q.Enqueue(ctxT(t), task, []byte("x")); err != nil {
		t.Fatal(err)
	}
	waitQueueCount(t, o, "run", "skipped", 1)
	if got := queueCount(o, "run", "error"); got != 0 {
		t.Fatalf("a missing handler counted as %v errors", got)
	}
}

// An enqueue that cannot happen (the service is not started) is refused before an
// operation begins, so it is not counted, and nothing is left in flight.
func TestObserver_EnqueueOnAStoppedServiceIsNotCounted(t *testing.T) {
	requireValkey(t)
	o := obstest.New(t)
	idle := queue.NewQueueService(newCache(t, valkeyAddr), queue.WithNamespace(ns(t)), queue.WithObserver(o)) // never started
	if _, err := idle.Enqueue(ctxT(t), "t", []byte("x")); !errors.Is(err, queue.ErrNotStarted) {
		t.Fatalf("Enqueue = %v, want ErrNotStarted", err)
	}
	if got := queueCount(o, "enqueue", "error") + queueCount(o, "enqueue", "success"); got != 0 {
		t.Fatalf("a refused enqueue was counted %v times", got)
	}
}

// No observer, and a bare struct, are fine.
func TestObserver_NoneIsFine(t *testing.T) {
	q := newQueue(t)
	task := taskType(t, "plain")
	var ran atomic.Int64
	q.Handle(task, func(context.Context, []byte) error { ran.Add(1); return nil })
	if _, err := q.Enqueue(ctxT(t), task, []byte("x")); err != nil {
		t.Fatal(err)
	}
	eventually(t, 15*time.Second, func() bool { return ran.Load() == 1 }, "the job to run")
	var zero queue.QueueService
	if _, err := zero.Enqueue(ctxT(t), "t", nil); err == nil {
		t.Fatal("a zero service must return an error, not panic")
	}
}
