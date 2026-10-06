package regressions

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
)

// 24 Soak: a mixed workload (transfers, replays, reads, CDC deliveries) for SOAK_DURATION (default 5s
// as a smoke run; `make test-soak SOAK_DURATION=2h` for the real thing). Heap after GC, goroutines and
// open connections must stay flat, and the money must still balance at the end.

func TestBankDBSoak_MixedWorkloadStaysFlat(t *testing.T) {
	duration := 5 * time.Second
	if s := os.Getenv("SOAK_DURATION"); s != "" {
		d, err := time.ParseDuration(s)
		must(t, err)
		duration = d
	}
	baseline := goleak.IgnoreCurrent()
	t.Run("workload", func(t *testing.T) {
		const maxOpen = 8
		b := newBDBank(t, bdOpts{maxOpen: maxOpen, app: "soak"})
		ids := []string{"a", "b", "c", "d", "e"}
		for _, id := range ids {
			b.open(id, 1_000_000)
		}
		events := b.events["bank_accounts"]

		type sample struct {
			heap       uint64
			goroutines int
			open       int
		}
		var samples []sample
		measure := func() sample {
			runtime.GC()
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			return sample{m.HeapAlloc, runtime.NumGoroutine(), b.svc.Writer().Client().DB.Stats().OpenConnections}
		}

		ctx, cancel := context.WithTimeout(bg, duration)
		defer cancel()
		var ops, failures atomic.Int64
		var wg sync.WaitGroup
		for w := range 16 {
			wg.Go(func() {
				r := rand.New(rand.NewPCG(uint64(w), 99))
				for i := 0; ctx.Err() == nil; i++ {
					var err error
					switch r.IntN(10) {
					case 0, 1, 2, 3: // a new transfer
						from := r.IntN(len(ids))
						_, _, err = b.Transfer(ctx, transferReq{Key: fmt.Sprintf("w%d-%d", w, i), From: ids[from], To: ids[(from+1)%len(ids)], Amount: int64(r.IntN(50) + 1)})
					case 4: // a replay of an earlier request
						_, _, err = b.Transfer(ctx, transferReq{Key: fmt.Sprintf("w%d-%d", w, max(i-1, 0)), From: ids[0], To: ids[1], Amount: 1})
						if errors.Is(err, errBankKeyReused) {
							err = nil
						}
					case 5, 6, 7: // reads
						_, err = b.accounts.Find(ctx, eqFilter("id", ids[r.IntN(len(ids))]))
					default: // a CDC delivery
						v := int64(i + 1)
						err = publishTo(events, fmt.Sprintf("soak-%d-%d", w, i), bdAccount{ID: fmt.Sprintf("cdc%d", w), Owner: fmt.Sprintf("cdc%d", w),
							Balance: v, Version: v, UpdatedAt: time.Now()})
					}
					if err != nil && ctx.Err() == nil && !errors.Is(err, errBankNoFunds) {
						failures.Add(1)
						t.Errorf("op failed: %v", err)
					}
					ops.Add(1)
				}
			})
		}
		tick := time.NewTicker(max(duration/10, 200*time.Millisecond))
		defer tick.Stop()
		for done := false; !done; {
			select {
			case <-ctx.Done():
				done = true
			case <-tick.C:
				samples = append(samples, measure())
			}
		}
		wg.Wait()

		if len(samples) >= 4 {
			warm, last := samples[1], samples[len(samples)-1]
			if last.heap > warm.heap*2+16<<20 {
				t.Errorf("heap grew from %d to %d bytes over the run", warm.heap, last.heap)
			}
			if last.goroutines > warm.goroutines+16 {
				t.Errorf("goroutines grew from %d to %d", warm.goroutines, last.goroutines)
			}
		}
		for _, s := range samples {
			if s.open > maxOpen {
				t.Errorf("%d open writer connections; the pool allows %d", s.open, maxOpen)
			}
		}
		if total := count(t, b.h.writer, `SELECT SUM(balance) FROM bank_accounts WHERE id IN ('a','b','c','d','e')`); total != 5_000_000 {
			t.Errorf("total = %d after the soak; want 5000000", total)
		}
		if n := count(t, b.h.writer, `SELECT count(*) FROM bank_accounts a WHERE a.id IN ('a','b','c','d','e') AND a.balance <> 1000000 + COALESCE((SELECT SUM(delta) FROM bank_audit WHERE account_id = a.id), 0)`); n != 0 {
			t.Errorf("%d accounts do not reconcile with their audit trail", n)
		}
		t.Logf("%d operations in %v (%d failures)", ops.Load(), duration, failures.Load())
	})
	verifyNoDBLeaks(t, baseline)
}
