//go:build integration

package cron_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cron"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/queue"
)

// Zero value & absence: zero structs, nil and typed-nil dependencies, empty
// payloads, handles to removed jobs, nil contexts and nil receivers.

func TestZeroValue_ZeroStruct(t *testing.T) {
	t.Parallel()
	c := &cron.CronService{}
	ctx := context.Background()
	var r recorder
	mustNotPanic(t, "zero struct", func() {
		for name, got := range map[string]struct{ err, want error }{
			"Start":       {c.Start(ctx), cron.ErrNoTracker},
			"CreateJob":   {c.CreateJob(job(t, "@yearly", r.handle)), cron.ErrNoTracker},
			"RemoveJob":   {c.RemoveJob("x"), cron.ErrJobNotFound},
			"ExecuteJob":  {c.ExecuteJob(ctx, "x", nil), cron.ErrUnknownTask},
			"ExecuteByID": {c.ExecuteJobByID(ctx, "x", nil), cron.ErrJobNotFound},
			"SetLocation": {c.SetLocation(time.UTC), cron.ErrNoTracker},
			"Ping":        {c.Ping(ctx), cron.ErrNotStarted},
		} {
			if !errors.Is(got.err, got.want) {
				t.Errorf("%s = %v, want %v", name, got.err, got.want)
			}
		}
		if err := c.Stop(ctx); err != nil {
			t.Errorf("Stop = %v", err)
		}
	})
}

func TestZeroValue_TypedNilQueue(t *testing.T) {
	t.Parallel()
	var q *queue.QueueService
	c := cron.NewCronService(q, nil)
	var r recorder
	mustNotPanic(t, "typed-nil queue", func() {
		if err := c.CreateJob(job(t, "@yearly", r.handle)); !errors.Is(err, cron.ErrNoQueue) {
			t.Errorf("CreateJob = %v", err)
		}
		if err := c.Start(context.Background()); !errors.Is(err, cron.ErrNoQueue) {
			t.Errorf("Start = %v", err)
		}
	})
}

// No payload is a valid payload; nil asks for the job's own, empty sends
// nothing.
func TestZeroValue_EmptyPayloads(t *testing.T) {
	t.Parallel()
	c, _ := newCron(t)
	var r recorder
	j := job(t, "@yearly", r.handle)
	j.Payload = nil
	if err := c.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	ctx := ctxT(t)
	if err := c.ExecuteJobByID(ctx, j.JobID, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.ExecuteJob(ctx, j.TaskName, []byte{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "both runs", func() bool { return r.n.Load() == 2 })
	for _, p := range r.payloads() {
		if len(p) != 0 {
			t.Fatalf("payload = %q, want empty", p)
		}
	}
}

// Dangling: a job id kept after RemoveJob, or a service kept after Stop.
func TestZeroValue_DanglingHandles(t *testing.T) {
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
	ctx := ctxT(t)
	if err := c.ExecuteJobByID(ctx, j.JobID, nil); !errors.Is(err, cron.ErrJobNotFound) {
		t.Errorf("executing a removed job = %v", err)
	}
	if err := c.ExecuteJob(ctx, j.TaskName, nil); !errors.Is(err, cron.ErrUnknownTask) {
		t.Errorf("executing a removed job's task = %v", err)
	}
	if err := c.CreateJob(j); err != nil { // the id and task name are free again
		t.Errorf("recreating a removed job: %v", err)
	}
	kept := c
	_ = c.Stop(context.Background())
	if err := kept.ExecuteJobByID(ctx, j.JobID, nil); !errors.Is(err, cron.ErrNotStarted) {
		t.Errorf("executing through a handle kept after Stop = %v", err)
	}
}

func TestZeroValue_NilContextFailsFast(t *testing.T) {
	t.Parallel()
	c, _ := newCron(t)
	var r recorder
	j := job(t, "@yearly", r.handle)
	_ = c.CreateJob(j)
	var nilCtx context.Context
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = capturePanic(func() { _ = c.ExecuteJobByID(nilCtx, j.JobID, nil) })
		_, _ = capturePanic(func() { _ = c.Ping(nilCtx) })
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("a nil context hung")
	}
}

// A nil *CronService or nil interface is a wiring bug and fails loudly.
func TestZeroValue_NilReceiver(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var ptr *cron.CronService
	var iface cron.CronServices
	ops := map[string]func(cron.CronServices){
		"Start":     func(c cron.CronServices) { _ = c.Start(ctx) },
		"CreateJob": func(c cron.CronServices) { _ = c.CreateJob(cron.CronJob{}) },
		"Execute":   func(c cron.CronServices) { _ = c.ExecuteJob(ctx, "x", nil) },
	}
	for name, op := range ops {
		t.Run(name+"/nil pointer", func(t *testing.T) { mustPanic(t, name, func() { op(ptr) }) })
		t.Run(name+"/nil interface", func(t *testing.T) { mustPanic(t, name, func() { op(iface) }) })
	}
}
