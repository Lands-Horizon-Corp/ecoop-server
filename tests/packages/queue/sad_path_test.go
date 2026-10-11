package queue_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/queue"
)

func TestSadPath_StartFailures(t *testing.T) {
	t.Parallel()
	requireValkey(t)
	ctx := ctxT(t)
	if err := queue.NewQueueService(nil).Start(ctx); !errors.Is(err, queue.ErrNoCache) {
		t.Errorf("nil cache: Start = %v, want ErrNoCache", err)
	}
	if err := queue.NewQueueService(cache.NewCacheService(nil, "", nil)).Start(ctx); !errors.Is(err, cache.ErrNoAddresses) {
		t.Errorf("no addresses: Start = %v, want cache.ErrNoAddresses", err)
	}
	if err := queue.NewQueueService(cache.NewCacheService([]string{"127.0.0.1:1"}, "", nil)).Start(ctx); err == nil {
		t.Error("unreachable Valkey: Start succeeded")
	}
	if err := queue.NewQueueService(cache.NewCacheService([]string{valkeyAddr}, "wrong-"+valkeyPassword, nil)).Start(ctx); err == nil {
		t.Error("wrong password: Start succeeded")
	}
}

func TestSadPath_NotStarted(t *testing.T) {
	t.Parallel()
	requireValkey(t)
	ctx := ctxT(t)
	q := queue.NewQueueService(newCache(t, valkeyAddr), queue.Namespace(ns(t)))
	if _, err := q.Enqueue(ctx, "t", nil); !errors.Is(err, queue.ErrNotStarted) {
		t.Errorf("Enqueue before Start = %v", err)
	}
	if err := q.Ping(ctx); !errors.Is(err, queue.ErrNotStarted) {
		t.Errorf("Ping before Start = %v", err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	_ = q.Stop(context.Background())
	if _, err := q.Enqueue(ctx, "t", nil); !errors.Is(err, queue.ErrNotStarted) {
		t.Errorf("Enqueue after Stop = %v", err)
	}
}

func TestSadPath_UnencodablePayload(t *testing.T) {
	t.Parallel()
	q := newQueue(t)
	for name, p := range map[string]any{"chan": make(chan int), "func": func() {}, "complex": complex(1, 2)} {
		if _, err := q.Enqueue(ctxT(t), taskType(t, name), p); err == nil {
			t.Errorf("%s payload accepted", name)
		}
		if _, err := q.Schedule("@every 1s", taskType(t, name), p); err == nil {
			t.Errorf("%s payload accepted by Schedule", name)
		}
	}
}

func TestSadPath_BadSchedules(t *testing.T) {
	t.Parallel()
	q := newQueue(t)
	for _, spec := range []string{"", "not a spec", "* * *", "61 * * * *", "@every nope"} {
		if _, err := q.Schedule(spec, taskType(t, "bad"), nil); err == nil {
			t.Errorf("spec %q accepted", spec)
		}
	}
	for _, id := range []string{"", "abc", "-1", "999999", "1.5"} {
		if err := q.Unschedule(id); !errors.Is(err, queue.ErrNoSchedule) {
			t.Errorf("Unschedule(%q) = %v, want ErrNoSchedule", id, err)
		}
	}
}

func TestSadPath_FailuresAreRetriedThenArchived(t *testing.T) {
	t.Parallel()
	q := newQueue(t)
	var r recorder
	typ := taskType(t, "failing")
	q.Handle(typ, r.handler(errors.New("boom")))
	if _, err := q.Enqueue(ctxT(t), typ, nil, queue.MaxRetry(1)); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, func() bool { return archived(t, queue.Default) == 1 }, "failing job was not archived")
	if r.count() != 2 {
		t.Fatalf("ran %d times, want 2 (first try + 1 retry)", r.count())
	}
}

func TestSadPath_SkipRetryGivesUpAtOnce(t *testing.T) {
	t.Parallel()
	q := newQueue(t)
	var r recorder
	typ := taskType(t, "skip")
	q.Handle(typ, r.handler(errors.Join(errors.New("bad input"), queue.ErrSkipRetry)))
	if _, err := q.Enqueue(ctxT(t), typ, nil, queue.MaxRetry(5)); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, func() bool { return archived(t, queue.Default) == 1 }, "skipped job was not archived")
	never(t, 1500*time.Millisecond, func() bool { return r.count() > 1 }, "ErrSkipRetry job was retried")
}

func TestSadPath_DeadContext(t *testing.T) {
	t.Parallel()
	q := newQueue(t)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := q.Enqueue(cancelled, taskType(t, "dead"), nil); err == nil {
		t.Fatal("Enqueue with cancelled ctx succeeded")
	}
}

func TestSadPath_StopRespectsDeadline(t *testing.T) {
	t.Parallel()
	q := newQueue(t)
	typ := taskType(t, "slow")
	started := make(chan struct{})
	q.Handle(typ, func(ctx context.Context, _ []byte) error {
		close(started)
		<-ctx.Done() // a job that only ends when the worker gives up on it
		return ctx.Err()
	})
	if _, err := q.Enqueue(ctxT(t), typ, nil); err != nil {
		t.Fatal(err)
	}
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := q.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop = %v, want DeadlineExceeded", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Stop overran its deadline: %v", d)
	}
}
