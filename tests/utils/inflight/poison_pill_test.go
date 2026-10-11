package inflight_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Poison pill: misuse that would corrupt the count or deadlock a naive tracker.

func TestPoisonPill_DoneCalledManyTimes(t *testing.T) {
	f := utils.NewInflight()
	keep := f.Track()
	done := f.Track()
	for range 100 {
		mustNotPanic(t, "repeated done", done)
	}
	if f.Count() != 1 {
		t.Fatalf("Count = %d after repeated done; another op was released", f.Count())
	}
	res := waitAsync(f, context.Background())
	if !blocked(res, 30*time.Millisecond) {
		t.Fatal("repeated done let Wait return while an op was still running")
	}
	keep()
	if err := recv(t, res, time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestPoisonPill_DoneCalledConcurrently(t *testing.T) {
	f := utils.NewInflight()
	keep := f.Track()
	done := f.Track()
	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() { ; done() })
	}
	wg.Wait()
	if f.Count() != 1 {
		t.Fatalf("Count = %d, want 1", f.Count())
	}
	keep()
}

func TestPoisonPill_StaleDoneFromEarlierCycle(t *testing.T) {
	f := utils.NewInflight()
	stale := f.Track()
	stale()
	_ = f.Wait(context.Background())

	current := f.Track()
	stale() // a leftover done from the previous cycle must not end the new op
	if f.Count() != 1 {
		t.Fatalf("stale done affected the new cycle: Count = %d", f.Count())
	}
	current()
}

func TestPoisonPill_DoneOnAnotherGoroutine(t *testing.T) {
	f := utils.NewInflight()
	done := f.Track()
	go func() {
		time.Sleep(20 * time.Millisecond)
		done()
	}()
	if err := f.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPoisonPill_ManyOperations(t *testing.T) {
	f := utils.NewInflight()
	const n = 200_000
	dones := make([]func(), n)
	for i := range dones {
		dones[i] = f.Track()
	}
	if f.Count() != n {
		t.Fatalf("Count = %d, want %d", f.Count(), n)
	}
	for _, d := range dones {
		d()
	}
	if err := f.Wait(context.Background()); err != nil || f.Count() != 0 {
		t.Fatalf("after %d ops: Count = %d, Wait = %v", n, f.Count(), err)
	}
}

func TestPoisonPill_DoneNeverCalledDoesNotHangWithDeadline(t *testing.T) {
	f := utils.NewInflight()
	_ = f.Track() // leaked: caller forgot done
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	res := waitAsync(f, ctx)
	if err := recv(t, res, time.Second); err == nil {
		t.Fatal("Wait reported drained with a leaked operation")
	}
}
