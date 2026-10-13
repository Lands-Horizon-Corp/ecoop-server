//go:build integration

package queue_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/queue"
)

// Zero value & absence: nil caches, zero structs, nil payloads, nil options,
// nil errors, nil receivers.

func TestZeroValue_ZeroStruct(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	q := &queue.QueueService{}
	mustNotPanic(t, "zero struct", func() {
		if err := q.Start(ctx); !errors.Is(err, queue.ErrNoCache) {
			t.Errorf("Start = %v, want ErrNoCache", err)
		}
		if _, err := q.Enqueue(ctx, "t", nil); !errors.Is(err, queue.ErrNotStarted) {
			t.Errorf("Enqueue = %v, want ErrNotStarted", err)
		}
		if err := q.Ping(ctx); !errors.Is(err, queue.ErrNotStarted) {
			t.Errorf("Ping = %v, want ErrNotStarted", err)
		}
		if err := q.Stop(ctx); err != nil {
			t.Errorf("Stop = %v", err)
		}
		q.Handle("t", func(context.Context, []byte) error { return nil })
		if err := q.Unschedule("1"); !errors.Is(err, queue.ErrNoSchedule) {
			t.Errorf("Unschedule = %v, want ErrNoSchedule", err)
		}
		if _, err := q.Schedule("@every 1h", "t", nil); err == nil {
			t.Error("Schedule on a zero struct succeeded")
		}
	})
}

func TestZeroValue_StopIsIdempotent(t *testing.T) {
	t.Parallel()
	q := newQueue(t)
	for range 3 {
		if err := q.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestZeroValue_NilAndEmptyPayloads(t *testing.T) {
	t.Parallel()
	q := newQueue(t)
	var r recorder
	typ := taskType(t, "empty")
	q.Handle(typ, r.handler(nil))
	for _, p := range []any{nil, "", []byte{}, []byte(nil), struct{}{}, 0} {
		if _, err := q.Enqueue(ctxT(t), typ, p); err != nil {
			t.Fatalf("payload %#v: %v", p, err)
		}
	}
	eventually(t, 10*time.Second, func() bool { return r.count() == 6 }, "empty payloads did not all run")
}

func TestZeroValue_TypedJobGetsZeroValueForNull(t *testing.T) {
	t.Parallel()
	q := newQueue(t)
	type data struct{ N int }
	got := make(chan data, 1)
	job := queue.NewJob(q, taskType(t, "null"), func(_ context.Context, d data) error { got <- d; return nil })
	if err := job.Deliver(ctxT(t), data{}); err != nil {
		t.Fatal(err)
	}
	select {
	case d := <-got:
		if d != (data{}) {
			t.Fatalf("got %+v", d)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("job never ran")
	}
}

func TestZeroValue_EmptyTaskType(t *testing.T) {
	t.Parallel()
	q := newQueue(t)
	var err error
	mustNotPanic(t, "Enqueue(\"\")", func() { _, err = q.Enqueue(ctxT(t), "", nil) })
	if err == nil {
		t.Fatal("empty task type accepted")
	}
}

func TestZeroValue_NilOptionIsIgnored(t *testing.T) {
	t.Parallel()
	// A conditional option left unset (var opt queue.Option) must not crash
	// the caller.
	q := newQueue(t)
	var unset queue.Option
	typ := taskType(t, "nil-opt")
	var r recorder
	mustNotPanic(t, "Handle with nil option", func() { q.Handle(typ, r.handler(nil), unset) })
	mustNotPanic(t, "Enqueue with nil option", func() { _, _ = q.Enqueue(ctxT(t), typ, nil, unset) })
}

func TestZeroValue_RetryAfterWithNilError(t *testing.T) {
	t.Parallel()
	err := queue.RetryAfter(time.Second, nil)
	mustNotPanic(t, "Error() on RetryAfter(nil)", func() { _ = err.Error() })
}

func TestZeroValue_NilContext(t *testing.T) {
	t.Parallel()
	// A nil ctx is a programming error: it must fail fast, not hang.
	q := newQueue(t)
	var nilCtx context.Context
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = capturePanic(func() { _, _ = q.Enqueue(nilCtx, taskType(t, "nil-ctx"), nil) })
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Enqueue(nil ctx) hung")
	}
}

// Dangling pointers: a nil *QueueService or nil interface is a wiring bug
// and must fail loudly.
func TestZeroValue_NilReceiver(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var ptr *queue.QueueService
	var iface queue.IQueueService
	ops := map[string]func(queue.IQueueService){
		"Start":   func(q queue.IQueueService) { _ = q.Start(ctx) },
		"Stop":    func(q queue.IQueueService) { _ = q.Stop(ctx) },
		"Ping":    func(q queue.IQueueService) { _ = q.Ping(ctx) },
		"Handle":  func(q queue.IQueueService) { q.Handle("t", nil) },
		"Enqueue": func(q queue.IQueueService) { _, _ = q.Enqueue(ctx, "t", nil) },
	}
	for name, op := range ops {
		t.Run(name+"/nil pointer", func(t *testing.T) { mustPanic(t, name, func() { op(ptr) }) })
		t.Run(name+"/nil interface", func(t *testing.T) { mustPanic(t, name, func() { op(iface) }) })
	}
	t.Run("NewJob/nil queue", func(t *testing.T) {
		mustPanic(t, "NewJob", func() { queue.NewJob[int](nil, "t", func(context.Context, int) error { return nil }) })
	})
}
