package queue_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/queue"
)

// Poison pill: payloads, handlers and options that try to crash the worker,
// loop forever, or lose jobs.

func TestPoisonPill_UndecodablePayloadIsNotRetried(t *testing.T) {
	q := newQueue(t)
	var calls atomic.Int64
	typ := taskType(t, "typed")
	queue.NewJob(q, typ, func(context.Context, struct{ N int }) error { calls.Add(1); return nil })
	if _, err := q.Enqueue(ctxT(t), typ, []byte("not json {")); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, func() bool { return archived(t, queue.Default) == 1 }, "undecodable job not archived")
	if calls.Load() != 0 {
		t.Fatal("typed handler ran on an undecodable payload")
	}
}

func TestPoisonPill_PanickingHandlerDoesNotKillTheWorker(t *testing.T) {
	q := newQueue(t)
	bad, good := taskType(t, "panics"), taskType(t, "fine")
	q.Handle(bad, func(context.Context, []byte) error { panic("handler bug") })
	var r recorder
	q.Handle(good, r.handler(nil))
	if _, err := q.Enqueue(ctxT(t), bad, nil, queue.MaxRetry(0)); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue(ctxT(t), good, nil); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, func() bool { return r.count() == 1 }, "worker stopped after a handler panic")
	eventually(t, 10*time.Second, func() bool { return archived(t, queue.Default) == 1 }, "panicking job was not archived")
}

func TestPoisonPill_HostileTaskTypesAndPayloads(t *testing.T) {
	q := newQueue(t)
	cases := map[string]any{
		"spaces and colons": "a:b c:d",
		"unicode":           "ключ-鍵-🔑",
		"long type":         strings.Repeat("t", 4096),
		"null bytes":        "a\x00b",
		"glob chars":        "*?[]",
	}
	for name, suffix := range cases {
		t.Run(name, func(t *testing.T) {
			var r recorder
			typ := taskType(t, suffix.(string))
			q.Handle(typ, r.handler(nil))
			payload := map[string]any{"html": "<script>alert(1)</script>", "big": strings.Repeat("x", 1<<20), "nul": "\x00"}
			var err error
			mustNotPanic(t, "Enqueue", func() { _, err = q.Enqueue(ctxT(t), typ, payload) })
			if err != nil {
				t.Fatal(err)
			}
			eventually(t, 10*time.Second, func() bool { return r.count() == 1 }, "job did not run")
		})
	}
}

func TestPoisonPill_JobTypeWithNoHandlerWaitsForOne(t *testing.T) {
	q := newQueue(t)
	typ := taskType(t, "late-handler")
	if _, err := q.Enqueue(ctxT(t), typ, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second) // the job is bounced at least once
	if n := archived(t, queue.Default); n != 0 {
		t.Fatalf("job with no handler yet was archived (%d)", n)
	}
	var r recorder
	q.Handle(typ, r.handler(nil))
	eventually(t, 2*queue.UnhandledRetry, func() bool { return r.count() == 1 }, "job never reached its late handler")
}

func TestPoisonPill_RetryAfterIsNotLostWithoutRetriesLeft(t *testing.T) {
	// A job with no retries left (MaxRetry(0)) that is rate limited must wait
	// for the next window, not be archived.
	q := newQueue(t)
	var r recorder
	typ := taskType(t, "limited-once")
	q.Handle(typ, r.handler(nil), queue.RateLimit(1, time.Second))
	for range 3 {
		if _, err := q.Enqueue(ctxT(t), typ, nil, queue.MaxRetry(0)); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, 10*time.Second, func() bool { return r.count() == 3 }, "rate-limited jobs with MaxRetry(0) were archived instead of waiting for the next window")
}

func TestPoisonPill_DegenerateRateLimits(t *testing.T) {
	// A misconfigured limit must not stop the job type from running.
	cases := map[string]queue.Option{
		"zero count":      queue.RateLimit(0, time.Second),
		"negative count":  queue.RateLimit(-5, time.Second),
		"zero window":     queue.RateLimit(5, 0),
		"negative window": queue.RateLimit(5, -time.Second),
	}
	for name, opt := range cases {
		t.Run(name, func(t *testing.T) {
			q := newQueue(t)
			var r recorder
			typ := taskType(t, "degenerate")
			q.Handle(typ, r.handler(nil), opt)
			if _, err := q.Enqueue(ctxT(t), typ, nil, queue.MaxRetry(1)); err != nil {
				t.Fatal(err)
			}
			eventually(t, 10*time.Second, func() bool { return r.count() == 1 }, "job never ran with %s", name)
		})
	}
}

func TestPoisonPill_HandlerErrorsAreKeptForErrorsIs(t *testing.T) {
	sentinel := errors.New("sentinel")
	err := queue.RetryAfter(time.Second, sentinel)
	if !errors.Is(err, sentinel) || err.Error() != "sentinel" {
		t.Fatalf("RetryAfter lost its cause: %v", err)
	}
}
