//go:build integration

package cron_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cron"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/queue"
)

// Poison pill: hostile handlers, payloads, names and schedules.

// A panicking job fails that run (and is retried), never the worker.
func TestPoison_PanickingHandler(t *testing.T) {
	t.Parallel()
	c, _ := newCron(t)
	var calls, ok atomic.Int64
	j := job(t, "@yearly", func(context.Context, []byte) error {
		if calls.Add(1) == 1 {
			panic("job blew up")
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
	eventually(t, 15*time.Second, "the retry after the panic to succeed", func() bool { return ok.Load() == 1 })
	var r recorder // the worker still runs other jobs
	other := job(t, "@yearly", r.handle)
	other.JobID, other.TaskName = other.JobID+"-other", other.TaskName+"-other"
	if err := c.CreateJob(other); err != nil {
		t.Fatal(err)
	}
	if err := c.ExecuteJobByID(ctxT(t), other.JobID, nil); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "another job after the panic", func() bool { return r.n.Load() == 1 })
}

// A job that says it cannot succeed is not retried.
func TestPoison_SkipRetry(t *testing.T) {
	t.Parallel()
	c, _ := newCron(t)
	var calls atomic.Int64
	j := job(t, "@yearly", func(context.Context, []byte) error {
		calls.Add(1)
		return errors.Join(errors.New("bad input"), queue.ErrSkipRetry)
	})
	if err := c.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	if err := c.ExecuteJobByID(ctxT(t), j.JobID, nil); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "the run", func() bool { return calls.Load() == 1 })
	time.Sleep(3 * time.Second) // past the first retry delay
	if n := calls.Load(); n != 1 {
		t.Fatalf("a skip-retry job ran %d times", n)
	}
}

// Binary and large payloads arrive byte for byte.
func TestPoison_BinaryAndLargePayloads(t *testing.T) {
	t.Parallel()
	c, _ := newCron(t)
	var r recorder
	j := job(t, "@yearly", r.handle)
	if err := c.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	binary := []byte("\x00\xff\xfe{not json}\x00\n\r'\";DROP TABLE jobs;--")
	large := bytes.Repeat([]byte{0xAB, 0x00, 0x7F}, 350_000) // ~1 MB
	ctx := ctxT(t)
	for _, p := range [][]byte{binary, large} {
		if err := c.ExecuteJob(ctx, j.TaskName, p); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, 10*time.Second, "both runs", func() bool { return r.n.Load() == 2 })
	got := r.payloads()
	if (!bytes.Equal(got[0], binary) || !bytes.Equal(got[1], large)) && (!bytes.Equal(got[1], binary) || !bytes.Equal(got[0], large)) {
		t.Fatal("payloads changed in transit")
	}
}

// Odd but valid ids and task names work, and stay separate.
func TestPoison_HostileNames(t *testing.T) {
	t.Parallel()
	c, _ := newCron(t)
	ctx := ctxT(t)
	suffix := strings.NewReplacer("/", "-").Replace(t.Name())
	for i, name := range []string{"spaces in name", "ünïcödé-名前", strings.Repeat("x", 2000), "a:b:c", "*", "{json}"} {
		var r recorder
		j := cron.CronJob{JobID: name + suffix, Schedule: "@yearly", TaskName: name + suffix, Handler: r.handle, Payload: []byte{byte(i)}}
		if err := c.CreateJob(j); err != nil {
			t.Fatalf("%q: %v", name, err)
		}
		if err := c.ExecuteJobByID(ctx, j.JobID, nil); err != nil {
			t.Fatalf("%q: %v", name, err)
		}
		eventually(t, 10*time.Second, "run of "+name[:min(len(name), 20)], func() bool { return r.n.Load() == 1 })
		if p := r.payloads()[0]; len(p) != 1 || p[0] != byte(i) {
			t.Fatalf("%q got another job's payload %v", name, p)
		}
	}
}

// Garbage schedules are refused, never panic.
func TestPoison_GarbageSchedules(t *testing.T) {
	t.Parallel()
	c, _ := newCron(t)
	var r recorder
	for _, spec := range []string{"\x00", "@every -1s", "@every 1y", "@yearly extra", "*/0 * * * *", "1-0 * * * *",
		strings.Repeat("* ", 10000), "@every 99999999999999999999h", "$(rm -rf /)"} {
		j := job(t, spec, r.handle)
		var err error
		mustNotPanic(t, spec[:min(len(spec), 20)], func() { err = c.CreateJob(j) })
		if !errors.Is(err, cron.ErrInvalidSchedule) {
			t.Errorf("%.30q: %v, want ErrInvalidSchedule", spec, err)
		}
	}
}
