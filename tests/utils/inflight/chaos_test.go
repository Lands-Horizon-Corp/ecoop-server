package inflight_test

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Chaos: random operation lifetimes, random waiters with random deadlines,
// and done calls repeated at random.

func chaosRand(t *testing.T) (*rand.Rand, uint64) {
	t.Helper()
	seed := uint64(time.Now().UnixNano())
	t.Logf("chaos seed: %d", seed)
	return rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)), seed
}

func TestChaos_RandomOperationsAndWaiters(t *testing.T) {
	t.Parallel()
	f := utils.NewInflight()
	_, seed := chaosRand(t)
	var ops, waiters sync.WaitGroup

	for i := range 200 {
		ops.Go(func() {
			r := rand.New(rand.NewPCG(seed, uint64(i)))
			done := f.Track()
			time.Sleep(time.Duration(r.IntN(3000)) * time.Microsecond)
			for range 1 + r.IntN(3) { // sometimes called more than once
				done()
			}
		})
	}
	for i := range 50 {
		waiters.Go(func() {
			r := rand.New(rand.NewPCG(seed, uint64(1000+i)))
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(r.IntN(5000))*time.Microsecond)
			defer cancel()
			if err := f.Wait(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("Wait: %v", err)
			}
		})
	}

	ops.Wait()
	waiters.Wait()
	if c := f.Count(); c != 0 {
		t.Fatalf("Count = %d after every op ended", c)
	}
	if err := f.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestChaos_CountNeverNegative(t *testing.T) {
	t.Parallel()
	f := utils.NewInflight()
	r, _ := chaosRand(t)
	var live []func()
	for range 10_000 {
		if len(live) == 0 || r.IntN(2) == 0 {
			live = append(live, f.Track())
		} else {
			i := r.IntN(len(live))
			live[i]()
			if r.IntN(4) != 0 { // keep some ended dones around to call again
				live = append(live[:i], live[i+1:]...)
			}
		}
		if f.Count() < 0 {
			t.Fatalf("Count went negative: %d", f.Count())
		}
	}
	for _, d := range live {
		d()
	}
	if f.Count() != 0 {
		t.Fatalf("Count = %d at the end", f.Count())
	}
}
