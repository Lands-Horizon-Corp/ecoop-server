//go:build integration

package cron_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cron"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/tests/internal/obstest"
)

func observedCron(t testing.TB) (cron.ICronServices, *observability.Service) {
	t.Helper()
	q := newQueue(t)
	o := obstest.New(t)
	c := cron.NewCronService(q, nil, cron.WithObserver(o))
	if err := c.Start(ctxT(t)); err != nil {
		t.Fatalf("cron Start: %v", err)
	}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	return c, o
}

func cronCount(o *observability.Service, op, outcome string) float64 {
	return obstest.Count(o, "cron", op, outcome)
}

// Creating, running now and removing a job are each one counted operation.
func TestObserver_JobManagementIsCounted(t *testing.T) {
	t.Parallel()
	c, o := observedCron(t)
	var r recorder
	j := job(t, "@yearly", r.handle) // never ticks during the test
	if err := c.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	if err := c.ExecuteJob(ctxT(t), j.TaskName, []byte("now")); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "the run", func() bool { return r.n.Load() == 1 })
	if err := c.RemoveJob(j.JobID); err != nil {
		t.Fatal(err)
	}
	for _, op := range []string{"create_job", "execute", "remove_job"} {
		if got := cronCount(o, op, "success"); got != 1 {
			t.Errorf("%s success = %v, want 1", op, got)
		}
	}
}

// A refused job (a bad schedule, an unknown id) is an error, not a success.
func TestObserver_RefusedCallsAreErrors(t *testing.T) {
	t.Parallel()
	c, o := observedCron(t)
	var r recorder
	if err := c.CreateJob(job(t, "not a schedule", r.handle)); !errors.Is(err, cron.ErrInvalidSchedule) {
		t.Fatalf("CreateJob = %v, want ErrInvalidSchedule", err)
	}
	if err := c.RemoveJob("no-such-job"); !errors.Is(err, cron.ErrJobNotFound) {
		t.Fatalf("RemoveJob = %v, want ErrJobNotFound", err)
	}
	if got := cronCount(o, "create_job", "error"); got != 1 {
		t.Fatalf("create_job error = %v, want 1", got)
	}
	if got := cronCount(o, "remove_job", "error"); got != 1 {
		t.Fatalf("remove_job error = %v, want 1", got)
	}
	if got := cronCount(o, "create_job", "success"); got != 0 {
		t.Fatalf("a refused job counted as success %v times", got)
	}
}

// With no observer, and a bare struct, nothing changes.
func TestObserver_NoneIsFine(t *testing.T) {
	t.Parallel()
	c, _ := newCron(t)
	var r recorder
	j := job(t, "@yearly", r.handle)
	if err := c.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	if err := c.RemoveJob(j.JobID); err != nil {
		t.Fatal(err)
	}
	var zero cron.CronService
	if err := zero.CreateJob(cron.CronJob{}); err == nil {
		t.Fatal("a zero service must return an error, not panic")
	}
}
