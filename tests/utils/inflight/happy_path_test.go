package inflight_test

import (
	"context"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

func TestHappyPath_CountTracksOperations(t *testing.T) {
	t.Parallel()
	f := utils.NewInflight()
	dones := make([]func(), 5)
	for i := range dones {
		dones[i] = f.Track()
		if f.Count() != i+1 {
			t.Fatalf("after %d Track, Count = %d", i+1, f.Count())
		}
	}
	for i, done := range dones {
		done()
		if want := len(dones) - i - 1; f.Count() != want {
			t.Fatalf("after %d done, Count = %d, want %d", i+1, f.Count(), want)
		}
	}
}

func TestHappyPath_WaitReturnsWhenLastOperationEnds(t *testing.T) {
	t.Parallel()
	f := utils.NewInflight()
	a, b := f.Track(), f.Track()
	res := waitAsync(f, context.Background())

	a()
	if !blocked(res, 50*time.Millisecond) {
		t.Fatal("Wait returned while an operation was still in flight")
	}
	b()
	if err := recv(t, res, time.Second); err != nil {
		t.Fatalf("Wait = %v", err)
	}
}

func TestHappyPath_WaitWhenIdleReturnsImmediately(t *testing.T) {
	t.Parallel()
	f := utils.NewInflight()
	start := time.Now()
	if err := f.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Fatalf("idle Wait took %v", d)
	}
}

func TestHappyPath_AllWaitersAreReleased(t *testing.T) {
	t.Parallel()
	f := utils.NewInflight()
	done := f.Track()
	waiters := make([]<-chan error, 10)
	for i := range waiters {
		waiters[i] = waitAsync(f, context.Background())
	}
	time.Sleep(20 * time.Millisecond)
	done()
	for i, w := range waiters {
		if err := recv(t, w, time.Second); err != nil {
			t.Fatalf("waiter %d: %v", i, err)
		}
	}
}

func TestHappyPath_ReusableAcrossCycles(t *testing.T) {
	t.Parallel()
	f := utils.NewInflight()
	for cycle := range 5 {
		done := f.Track()
		res := waitAsync(f, context.Background())
		if !blocked(res, 20*time.Millisecond) {
			t.Fatalf("cycle %d: Wait returned early", cycle)
		}
		done()
		if err := recv(t, res, time.Second); err != nil {
			t.Fatalf("cycle %d: %v", cycle, err)
		}
	}
}

func TestHappyPath_DeferredDone(t *testing.T) {
	t.Parallel()
	f := utils.NewInflight()
	work := func() {
		defer f.Track()()
		if f.Count() != 1 {
			t.Errorf("Count inside op = %d", f.Count())
		}
	}
	work()
	if f.Count() != 0 {
		t.Fatalf("Count after deferred done = %d", f.Count())
	}
}
