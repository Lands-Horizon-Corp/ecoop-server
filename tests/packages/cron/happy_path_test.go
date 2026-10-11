//go:build integration

package cron_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cron"
)

// Happy path: jobs run on schedule, on demand, and stop when removed.

func TestHappy_RunsOnScheduleWithItsPayload(t *testing.T) {
	t.Parallel()
	c, _ := newCron(t)
	var r recorder
	if err := c.CreateJob(job(t, "@every 1s", r.handle)); err != nil {
		t.Fatal(err)
	}
	eventually(t, 15*time.Second, "two scheduled runs", func() bool { return r.n.Load() >= 2 })
	for _, p := range r.payloads() {
		if string(p) != "payload" {
			t.Fatalf("payload = %q", p)
		}
	}
}

func TestHappy_ExecuteNow(t *testing.T) {
	t.Parallel()
	c, _ := newCron(t)
	var r recorder
	j := job(t, "@yearly", r.handle) // never ticks during the test
	if err := c.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	ctx := ctxT(t)
	if err := c.ExecuteJob(ctx, j.TaskName, []byte("manual")); err != nil {
		t.Fatal(err)
	}
	if err := c.ExecuteJobByID(ctx, j.JobID, nil); err != nil { // nil: the job's own payload
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "both runs", func() bool { return r.n.Load() == 2 })
	got := map[string]bool{}
	for _, p := range r.payloads() {
		got[string(p)] = true
	}
	if !got["manual"] || !got["payload"] {
		t.Fatalf("payloads = %v", got)
	}
}

func TestHappy_RemoveJobStopsTheTicks(t *testing.T) {
	t.Parallel()
	c, _ := newCron(t)
	var r recorder
	j := job(t, "@every 1s", r.handle)
	if err := c.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "a run", func() bool { return r.n.Load() >= 1 })
	if err := c.RemoveJob(j.JobID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond) // a tick already queued may still run
	after := r.n.Load()
	time.Sleep(2500 * time.Millisecond)
	if r.n.Load() != after {
		t.Fatalf("runs went from %d to %d after RemoveJob", after, r.n.Load())
	}
}

// Jobs can be defined before Start, as an app does at boot.
func TestHappy_JobsDefinedBeforeStart(t *testing.T) {
	t.Parallel()
	q := newQueue(t)
	c := cron.NewCronService(q, nil)
	var r recorder
	if err := c.CreateJob(job(t, "@every 1s", r.handle)); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	defer c.Stop(context.Background())
	eventually(t, 10*time.Second, "a run after Start", func() bool { return r.n.Load() >= 1 })
}

func TestHappy_SetLocationReschedules(t *testing.T) {
	t.Parallel()
	c, _ := newCron(t)
	manila, err := time.LoadLocation("Asia/Manila")
	if err != nil {
		t.Skip("no tz database:", err)
	}
	var r, own recorder
	if err := c.CreateJob(job(t, "@every 1s", r.handle)); err != nil {
		t.Fatal(err)
	}
	j := job(t, "@every 1s", own.handle)
	j.JobID, j.TaskName, j.Loc = j.JobID+"-own", j.TaskName+"-own", time.UTC
	if err := c.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	if err := c.SetLocation(manila); err != nil {
		t.Fatal(err)
	}
	before, beforeOwn := r.n.Load(), own.n.Load()
	eventually(t, 10*time.Second, "both jobs to keep running", func() bool {
		return r.n.Load() > before && own.n.Load() > beforeOwn
	})
}

// Stop pauses this instance; Start (or Run) resumes it.
func TestHappy_StopThenStartAgain(t *testing.T) {
	t.Parallel()
	c, _ := newCron(t)
	var r recorder
	if err := c.CreateJob(job(t, "@every 1s", r.handle)); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "a run", func() bool { return r.n.Load() >= 1 })
	if err := c.Stop(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	if err := c.Run(ctxT(t)); err != nil { // Run is Start
		t.Fatal(err)
	}
	n := r.n.Load()
	eventually(t, 10*time.Second, "runs after restarting", func() bool { return r.n.Load() > n })
}

// The job keeps its own copy of the payload.
func TestHappy_PayloadIsCopied(t *testing.T) {
	t.Parallel()
	c, _ := newCron(t)
	var r recorder
	j := job(t, "@yearly", r.handle)
	buf := []byte("original")
	j.Payload = buf
	if err := c.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	copy(buf, "MUTATED!")
	if err := c.ExecuteJobByID(ctxT(t), j.JobID, nil); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "the run", func() bool { return r.n.Load() == 1 })
	if p := r.payloads()[0]; !bytes.Equal(p, []byte("original")) {
		t.Fatalf("payload = %q", p)
	}
}
