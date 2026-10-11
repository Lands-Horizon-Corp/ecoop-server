package otp_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/otp"
)

// Sync & race: one service under concurrent use. Run with -race.

func TestRace_ConcurrentRightGuessesOneWins(t *testing.T) {
	o := newOTP(t)
	for round := range 20 {
		k := key(t)
		code, _ := o.Generate(ctxT(t), k)
		var wins atomic.Int64
		var wg sync.WaitGroup
		for range otp.MaxAttempts {
			wg.Go(func() {
				if ok, _ := o.Verify(ctxT(t), k, code); ok {
					wins.Add(1)
				}
			})
		}
		wg.Wait()
		if wins.Load() != 1 {
			t.Fatalf("round %d: %d wins, want exactly 1", round, wins.Load())
		}
	}
}

func TestRace_ConcurrentGenerateOneCodeSurvives(t *testing.T) {
	o := newOTP(t)
	k := key(t)
	const n = 4 // below MaxAttempts so every code can be tried
	codes := make([]string, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() { codes[i], _ = o.Generate(ctxT(t), k) })
	}
	wg.Wait()
	valid := 0
	for _, c := range codes {
		if ok, _ := o.Verify(ctxT(t), k, c); ok {
			valid++
		}
	}
	if valid > 1 {
		t.Fatalf("%d of %d concurrently generated codes verified; only the last write may", valid, n)
	}
}

func TestRace_ManyKeysInParallel(t *testing.T) {
	o := newOTP(t)
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			k := key(t)
			for range 5 {
				code, err := o.Generate(ctxT(t), k)
				if err != nil {
					t.Error(err)
					return
				}
				if ok, err := o.Verify(ctxT(t), k, code); !ok || err != nil {
					t.Errorf("Verify = %v, %v", ok, err)
					return
				}
			}
			_ = o.Revoke(ctxT(t), k)
		})
	}
	wg.Wait()
}

func TestRace_StopDuringTraffic(t *testing.T) {
	o := newOTP(t)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				mustNotPanic(t, "ops during Stop", func() {
					k := key(t)
					if code, err := o.Generate(context.Background(), k); err == nil {
						_, _ = o.Verify(context.Background(), k, code)
					} else if !errors.Is(err, otp.ErrNotStarted) {
						t.Errorf("Generate: %v", err)
					}
				})
			}
		})
	}
	for range 10 {
		_ = o.Stop(context.Background())
		_ = o.Start(context.Background())
	}
	close(stop)
	wg.Wait()
}
