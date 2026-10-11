//go:build integration

package queue_test

import (
	"context"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/queue"
)

// Smoke: start, ping, queue one typed job, see it run, stop.
func TestSmoke(t *testing.T) {
	t.Parallel()
	requireValkey(t)
	ctx := ctxT(t)
	q := queue.NewQueueService(newCache(t, valkeyAddr), queue.Namespace(ns(t)))
	if err := q.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := q.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	type greeting struct{ Name string }
	got := make(chan string, 1)
	job := queue.NewJob(q, taskType(t, "smoke"), func(_ context.Context, g greeting) error {
		got <- g.Name
		return nil
	}, queue.Priority(queue.Default))
	if err := job.Deliver(ctx, greeting{Name: "ana"}); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	select {
	case name := <-got:
		if name != "ana" {
			t.Fatalf("payload = %q", name)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("job never ran")
	}
	if err := q.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
