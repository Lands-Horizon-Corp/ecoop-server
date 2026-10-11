//go:build integration

package cron_test

import (
	"context"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cron"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/queue"
)

// Integration chaos: several instances of the app behind a load balancer,
// each defining the same jobs, sharing one Valkey.

type instance struct {
	q queue.QueueServices
	c cron.CronServices
}

func instances(t *testing.T, n int, schedule string, r *recorder) []instance {
	t.Helper()
	out := make([]instance, n)
	for i := range out {
		q := newQueue(t)
		c := cron.NewCronService(q, nil)
		if err := c.CreateJob(job(t, schedule, r.handle)); err != nil {
			t.Fatal(err)
		}
		if err := c.Start(ctxT(t)); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Stop(context.Background()) })
		out[i] = instance{q, c}
	}
	return out
}

// Three instances, one schedule: each tick runs once, not once per instance.
func TestIntegrationChaos_EachTickRunsOnceAcrossInstances(t *testing.T) {
	t.Parallel()
	var r recorder
	instances(t, 3, "@every 1s", &r)
	time.Sleep(500 * time.Millisecond)
	start := r.n.Load()
	time.Sleep(5 * time.Second)
	runs := r.n.Load() - start
	if runs < 3 || runs > 7 {
		t.Fatalf("%d runs in 5s across 3 instances; each tick must run once", runs)
	}
}

// The leading instance goes away: another takes over the schedule.
func TestIntegrationChaos_LeaderLeaves(t *testing.T) {
	t.Parallel()
	var r recorder
	all := instances(t, 2, "@every 1s", &r)
	eventually(t, 10*time.Second, "runs", func() bool { return r.n.Load() >= 2 })
	for _, in := range all[:1] { // the first one started, so it leads
		_ = in.c.Stop(context.Background())
		_ = in.q.Stop(context.Background())
	}
	n := r.n.Load()
	eventually(t, 3*queue.LeaderRenew+5*time.Second, "the other instance to take over", func() bool { return r.n.Load() >= n+2 })
}

// An instance that stops while ticks keep coming: its share goes to the
// others, nothing is lost.
func TestIntegrationChaos_StoppedInstanceLosesNothing(t *testing.T) {
	t.Parallel()
	var r recorder
	all := instances(t, 2, "@yearly", &r)
	ctx := ctxT(t)
	_ = all[1].c.Stop(ctx) // its worker keeps pulling jobs, but the cron gate is closed
	const n = 10
	for range n {
		if err := all[0].c.ExecuteJob(ctx, job(t, "", nil).TaskName, nil); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, 30*time.Second, "every run, on the instance still running", func() bool { return r.n.Load() == n })
}
