//go:build integration

package cron_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cron"
)

// Sync & race: one service used from many goroutines; Stop racing running
// jobs. Run with -race.

func TestRace_ConcurrentDefinitionsAndCalls(t *testing.T) {
	t.Parallel()
	c, _ := newCron(t)
	ctx := ctxT(t)
	var r recorder
	var wg sync.WaitGroup
	for w := range 16 {
		wg.Go(func() {
			for i := range 10 {
				id := fmt.Sprintf("%s-%d-%d", t.Name(), w, i)
				if err := c.CreateJob(cron.CronJob{JobID: id, TaskName: id, Schedule: "@yearly", Handler: r.handle}); err != nil {
					t.Error(err)
					return
				}
				if err := c.ExecuteJobByID(ctx, id, nil); err != nil {
					t.Error(err)
				}
				if i%3 == 0 {
					if err := c.RemoveJob(id); err != nil {
						t.Error(err)
					}
				}
				if i%5 == 0 {
					_ = c.SetLocation(time.UTC)
				}
			}
		})
	}
	wg.Wait()
	eventually(t, 20*time.Second, "every execution", func() bool { return r.n.Load() == 160 })
}

func TestRace_OneWinnerPerJobID(t *testing.T) {
	t.Parallel()
	c, _ := newCron(t)
	var r recorder
	var wins atomic.Int64
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			if c.CreateJob(job(t, "@yearly", r.handle)) == nil {
				wins.Add(1)
			}
		})
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("%d goroutines created the same job", wins.Load())
	}
}

// Stop waits for a running job to finish.
func TestRace_StopWaitsForARunningJob(t *testing.T) {
	t.Parallel()
	c, _ := newCron(t)
	started := make(chan struct{})
	var finished atomic.Int64
	var once sync.Once
	j := job(t, "@yearly", func(context.Context, []byte) error {
		once.Do(func() { close(started) })
		time.Sleep(time.Second)
		finished.Add(1)
		return nil
	})
	if err := c.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	if err := c.ExecuteJobByID(ctxT(t), j.JobID, nil); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := c.Stop(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	if finished.Load() != 1 {
		t.Fatal("Stop returned before the running job finished")
	}
}

func TestRace_StopGivesUpWhenCtxEnds(t *testing.T) {
	t.Parallel()
	c, _ := newCron(t)
	started := make(chan struct{})
	var once sync.Once
	j := job(t, "@yearly", func(ctx context.Context, _ []byte) error {
		once.Do(func() { close(started) })
		time.Sleep(3 * time.Second)
		return nil
	})
	if err := c.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	_ = c.ExecuteJobByID(ctxT(t), j.JobID, nil)
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := c.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop = %v, want DeadlineExceeded", err)
	}
}

// Start and Stop churn while jobs are executed: calls either work or say
// the service is stopped; nothing panics.
func TestRace_LifecycleChurn(t *testing.T) {
	t.Parallel()
	c, _ := newCron(t)
	var r recorder
	j := job(t, "@every 1s", r.handle)
	if err := c.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				mustNotPanic(t, "execute during churn", func() {
					if err := c.ExecuteJob(ctxT(t), j.TaskName, nil); err != nil && !errors.Is(err, cron.ErrNotStarted) {
						t.Errorf("ExecuteJob: %v", err)
					}
				})
				time.Sleep(5 * time.Millisecond)
			}
		})
	}
	for i := range 20 {
		if i%2 == 0 {
			_ = c.Stop(ctxT(t))
		} else if err := c.Start(ctxT(t)); err != nil {
			t.Error(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
}
