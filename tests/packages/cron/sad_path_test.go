//go:build integration

package cron_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cron"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/queue"
)

// Sad path: wrong wiring and wrong calls are refused with the right error.

func TestSad_StartNeedsARunningQueue(t *testing.T) {
	t.Parallel()
	requireValkey(t)
	ctx := context.Background()
	if err := cron.NewCronService(nil, nil).Start(ctx); !errors.Is(err, cron.ErrNoQueue) {
		t.Errorf("nil queue: %v", err)
	}
	var typedNil *queue.QueueService
	if err := cron.NewCronService(typedNil, nil).Start(ctx); !errors.Is(err, cron.ErrNoQueue) {
		t.Errorf("typed-nil queue: %v", err)
	}
	stopped := queue.NewQueueService(cache.NewCacheService([]string{valkeyAddr}, valkeyPassword, nil))
	if err := cron.NewCronService(stopped, nil).Start(ctx); !errors.Is(err, cron.ErrQueueDown) {
		t.Errorf("queue not started: %v", err)
	}
}

func TestSad_CreateJobReportsEveryProblem(t *testing.T) {
	t.Parallel()
	c, _ := newCron(t)
	err := c.CreateJob(cron.CronJob{Schedule: "not a schedule"})
	for _, want := range []error{cron.ErrNoJobID, cron.ErrNoTaskName, cron.ErrNoHandler, cron.ErrInvalidSchedule} {
		if !errors.Is(err, want) {
			t.Errorf("CreateJob(empty) = %v, missing %v", err, want)
		}
	}
}

func TestSad_InvalidSchedules(t *testing.T) {
	t.Parallel()
	c, _ := newCron(t)
	var r recorder
	for _, spec := range []string{"", "   ", "@every", "@never", "* * * *", "* * * * * *", "61 * * * *", "* 25 * * *",
		"CRON_TZ=Asia/Manila 0 9 * * *", "TZ=UTC @hourly"} {
		j := job(t, spec, r.handle)
		if err := c.CreateJob(j); !errors.Is(err, cron.ErrInvalidSchedule) {
			t.Errorf("%q: %v, want ErrInvalidSchedule", spec, err)
		}
	}
}

func TestSad_DuplicatesAndUnknowns(t *testing.T) {
	t.Parallel()
	c, _ := newCron(t)
	ctx := ctxT(t)
	var r recorder
	j := job(t, "@yearly", r.handle)
	if err := c.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	if err := c.CreateJob(j); !errors.Is(err, cron.ErrJobExists) {
		t.Errorf("same id twice: %v", err)
	}
	other := j
	other.JobID += "-2"
	if err := c.CreateJob(other); !errors.Is(err, cron.ErrTaskTaken) {
		t.Errorf("same task name for two jobs: %v", err)
	}
	if err := c.RemoveJob("nope"); !errors.Is(err, cron.ErrJobNotFound) {
		t.Errorf("RemoveJob(unknown): %v", err)
	}
	if err := c.ExecuteJob(ctx, "nope", nil); !errors.Is(err, cron.ErrUnknownTask) {
		t.Errorf("ExecuteJob(unknown): %v", err)
	}
	if err := c.ExecuteJobByID(ctx, "nope", nil); !errors.Is(err, cron.ErrJobNotFound) {
		t.Errorf("ExecuteJobByID(unknown): %v", err)
	}
	if err := c.RemoveJob(j.JobID); err != nil {
		t.Fatal(err)
	}
	if err := c.RemoveJob(j.JobID); !errors.Is(err, cron.ErrJobNotFound) {
		t.Errorf("RemoveJob twice: %v", err)
	}
	if err := c.SetLocation(nil); !errors.Is(err, cron.ErrNilLocation) {
		t.Errorf("SetLocation(nil): %v", err)
	}
}

func TestSad_CallsBeforeStartAndAfterStop(t *testing.T) {
	t.Parallel()
	q := newQueue(t)
	c := cron.NewCronService(q, nil)
	var r recorder
	j := job(t, "@yearly", r.handle)
	if err := c.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	ctx := ctxT(t)
	check := func(when string) {
		t.Helper()
		for name, err := range map[string]error{
			"Ping": c.Ping(ctx), "ExecuteJob": c.ExecuteJob(ctx, j.TaskName, nil), "ExecuteJobByID": c.ExecuteJobByID(ctx, j.JobID, nil),
		} {
			if !errors.Is(err, cron.ErrNotStarted) {
				t.Errorf("%s: %s = %v, want ErrNotStarted", when, name, err)
			}
		}
	}
	check("before Start")
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	check("after Stop")
	time.Sleep(200 * time.Millisecond)
	if r.n.Load() != 0 {
		t.Fatal("a refused execution ran")
	}
}
