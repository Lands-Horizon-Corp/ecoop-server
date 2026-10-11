//go:build integration

package cron_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cron"
)

// Chaos: Valkey disappears, handlers fail and stall.

// The scheduler keeps going through a Valkey outage: ticks resume once the
// connection is back.
func TestChaos_ValkeyOutage(t *testing.T) {
	t.Parallel()
	p := newProxy(t)
	q := newQueue(t, p.Addr())
	c := cron.NewCronService(q, nil)
	if err := c.Start(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	defer c.Stop(context.Background())
	var r recorder
	if err := c.CreateJob(job(t, "@every 1s", r.handle)); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "a run before the outage", func() bool { return r.n.Load() >= 1 })
	p.Down()
	time.Sleep(2 * time.Second)
	p.Up()
	n := r.n.Load()
	eventually(t, 30*time.Second, "runs after the outage", func() bool { return r.n.Load() > n })
}

// A job that fails a few times is retried until it succeeds.
func TestChaos_TransientFailuresAreRetried(t *testing.T) {
	t.Parallel()
	c, _ := newCron(t)
	var calls, ok atomic.Int64
	j := job(t, "@yearly", func(context.Context, []byte) error {
		if calls.Add(1) <= 2 {
			return errors.New("bank API timed out")
		}
		ok.Add(1)
		return nil
	})
	if err := c.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	if err := c.ExecuteJobByID(ctxT(t), j.JobID, nil); err != nil {
		t.Fatal(err)
	}
	eventually(t, 20*time.Second, "success after two failures", func() bool { return ok.Load() == 1 })
}

// A handler slower than its schedule does not stop the schedule.
func TestChaos_SlowHandler(t *testing.T) {
	t.Parallel()
	c, _ := newCron(t)
	var finished atomic.Int64
	if err := c.CreateJob(job(t, "@every 1s", func(ctx context.Context, _ []byte) error {
		select {
		case <-time.After(2500 * time.Millisecond):
			finished.Add(1)
		case <-ctx.Done():
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	eventually(t, 20*time.Second, "slow runs to keep finishing", func() bool { return finished.Load() >= 2 })
}
