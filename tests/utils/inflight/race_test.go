package inflight_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Sync & race: concurrent Track/done/Count/Wait. Run with `go test -race`.

func TestRace_ConcurrentTrackDoneCountWait(t *testing.T) {
	f := utils.NewInflight()
	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			for range 500 {
				done := f.Track()
				_ = f.Count()
				done()
			}
		})
	}
	for range 16 {
		wg.Go(func() {
			for range 200 {
				ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
				_ = f.Wait(ctx)
				cancel()
			}
		})
	}
	wg.Wait()
	if f.Count() != 0 {
		t.Fatalf("Count = %d", f.Count())
	}
}

func TestRace_WaitReleasedOnlyByTheLastDone(t *testing.T) {
	f := utils.NewInflight()
	const n = 100
	dones := make([]func(), n)
	for i := range dones {
		dones[i] = f.Track()
	}
	res := waitAsync(f, context.Background())

	var wg sync.WaitGroup
	for _, d := range dones[:n-1] {
		wg.Go(func() { ; d() })
	}
	wg.Wait()
	if !blocked(res, 30*time.Millisecond) {
		t.Fatal("Wait returned before the last operation ended")
	}
	dones[n-1]()
	if err := recv(t, res, time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestRace_TrackDuringWait(t *testing.T) {
	// New operations starting while someone waits must not corrupt state
	// (the classic sync.WaitGroup reuse hazard).
	f := utils.NewInflight()
	first := f.Track()
	res := waitAsync(f, context.Background())

	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			for range 100 {
				f.Track()()
			}
		})
	}
	first()
	wg.Wait()
	if err := recv(t, res, time.Second); err != nil {
		t.Fatal(err)
	}
	if f.Count() != 0 {
		t.Fatalf("Count = %d", f.Count())
	}
}
