package inflight_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

func TestSadPath_WaitDeadlineWhileBusy(t *testing.T) {
	t.Parallel()
	f := utils.NewInflight()
	done := f.Track()
	defer done()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := f.Wait(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait = %v, want DeadlineExceeded", err)
	}
	if d := time.Since(start); d > 300*time.Millisecond {
		t.Fatalf("Wait overran its deadline: %v", d)
	}
	if f.Count() != 1 {
		t.Fatalf("timed-out Wait changed Count to %d", f.Count())
	}
}

func TestSadPath_WaitCancelledWhileBusy(t *testing.T) {
	t.Parallel()
	f := utils.NewInflight()
	defer f.Track()()
	ctx, cancel := context.WithCancel(context.Background())
	res := waitAsync(f, ctx)
	cancel()
	if err := recv(t, res, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait = %v, want Canceled", err)
	}
}

func TestSadPath_DeadCtxButIdleSucceeds(t *testing.T) {
	t.Parallel()
	// Nothing to wait for, so a dead ctx is irrelevant: idle wins.
	f := utils.NewInflight()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := f.Wait(ctx); err != nil {
		t.Fatalf("idle Wait with cancelled ctx = %v, want nil", err)
	}
}

func TestSadPath_TimedOutWaitThenDrain(t *testing.T) {
	t.Parallel()
	f := utils.NewInflight()
	done := f.Track()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	_ = f.Wait(ctx)
	cancel()

	// A later Wait still works once the operation ends.
	res := waitAsync(f, context.Background())
	done()
	if err := recv(t, res, time.Second); err != nil {
		t.Fatalf("Wait after earlier timeout = %v", err)
	}
}
