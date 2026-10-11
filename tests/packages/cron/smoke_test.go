//go:build integration

package cron_test

import (
	"context"
	"testing"
	"time"
)

// Smoke: define a job, it runs on schedule, stop.
func TestSmoke(t *testing.T) {
	t.Parallel()
	c, _ := newCron(t)
	var r recorder
	if err := c.CreateJob(job(t, "@every 1s", r.handle)); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if err := c.Ping(ctxT(t)); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	eventually(t, 10*time.Second, "the job to run", func() bool { return r.n.Load() >= 1 })
	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
