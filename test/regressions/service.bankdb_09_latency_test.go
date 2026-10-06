package regressions

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"go.uber.org/goleak"
)

// 09 Latency, cancellation and resource leaks: a cancelled request frees its connection and its
// server-side query at once, leaked rows are caught, and a full banking workload (and repeated
// restarts) leave no goroutine and no server connection behind. Every bank test also asserts at
// cleanup that both pools are fully drained (bdLedger.assertPoolsDrained).

func TestBankDBLeak_CancelledQueryFreesItsConnectionAndServerQuery(t *testing.T) {
	b := newBDBank(t, bdOpts{app: "cancel"})
	b.open("a", 1)
	b.replicate()

	ctx, cancel := context.WithCancel(bg)
	done := make(chan error, 1)
	go func() { done <- second(b.accounts.Find(ctx, sleepFilter(10))) }() // a 10s query on the reader
	b.awaitReader(`SELECT count(*) FROM pg_stat_activity WHERE application_name = 'cancel-r' AND state = 'active' AND query LIKE '%pg_sleep%'`, 1)

	cancelled := time.Now()
	cancel() // the client disconnects
	select {
	case err := <-done:
		requireKind(t, err, database.ErrCanceled)
	case <-time.After(2 * time.Second):
		t.Fatal("the query kept running after its context was cancelled")
	}
	if waited := time.Since(cancelled); waited > time.Second {
		t.Errorf("cancellation took %v to return", waited)
	}
	b.awaitReader(`SELECT count(*) FROM pg_stat_activity WHERE application_name = 'cancel-r' AND state = 'active' AND query LIKE '%pg_sleep%'`, 0)
	if inUse := b.svc.Reader().Client().DB.Stats().InUse; inUse != 0 {
		t.Fatalf("%d reader connections still in use right after cancellation", inUse)
	}
}

func TestBankDBLeak_UnclosedRowsAreDetected(t *testing.T) {
	b := newBDBank(t, bdOpts{noRun: true})
	b.open("a", 1)
	ctx := withDeadline(t, 5*time.Second)

	rows, err := b.svc.Writer().Client().QueryContext(ctx, `SELECT id FROM bank_accounts`)
	must(t, err)
	if inUse := b.svc.Writer().Client().DB.Stats().InUse; inUse != 1 {
		t.Fatalf("open rows hold %d connections; want 1 (the drain check relies on this)", inUse)
	}
	must(t, rows.Close()) // the fix; without it the cleanup drain check fails the test
	if inUse := b.svc.Writer().Client().DB.Stats().InUse; inUse != 0 {
		t.Fatalf("%d connections in use after rows.Close", inUse)
	}
}

func TestBankDBLeak_WorkloadLeavesNoGoroutinesOrConnections(t *testing.T) {
	baseline := goleak.IgnoreCurrent()
	t.Run("workload", func(t *testing.T) {
		b := newBDBank(t, bdOpts{app: "leakwork"})
		ids := []string{"a", "b", "c", "d"}
		for _, id := range ids {
			b.open(id, 100_000)
		}
		errs := runParallel(t, 200, func(ctx context.Context, i int) error {
			_, _, err := b.Transfer(ctx, transferReq{Key: fmt.Sprintf("k%03d", i), From: ids[i%4], To: ids[(i+1)%4], Amount: 5})
			if err != nil {
				return err
			}
			_, err = b.accounts.Find(ctx, eqFilter("id", ids[i%4])) // reader traffic alongside
			return err
		})
		if len(errs) > 0 {
			t.Fatalf("%d pipelines failed, first: %v", len(errs), errs[0])
		}
		b.replicate() // the runners are busy right up to Stop
		must(t, b.svc.Stop(bg))
		awaitCount(t, b.h.writer, 0, `SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'leakwork-%'`)
	})
	verifyNoDBLeaks(t, baseline)
}

func TestBankDBLeak_RepeatedRestartsDoNotLeak(t *testing.T) {
	baseline := goleak.IgnoreCurrent()
	t.Run("restarts", func(t *testing.T) {
		b := newBDBank(t, bdOpts{app: "restart"})
		b.open("a", 1)
		for i := range 10 {
			must(t, b.svc.Stop(bg))
			must(t, b.svc.Start(bg))
			b.svc.Run(bg)
			accounts, err := database.Get[bdAccount, bdAccountRes, bdNoRequest, string](b.svc)
			must(t, err)
			if _, err := accounts.IncrementByID(withDeadline(t, 5*time.Second), "a", "balance", 1); err != nil {
				t.Fatalf("cycle %d: %v", i, err)
			}
		}
		must(t, b.svc.Stop(bg))
		awaitCount(t, b.h.writer, 0, `SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'restart-%'`)
	})
	verifyNoDBLeaks(t, baseline)
}

// Transfers are network-bound (several round trips each), so the budget is a regression alarm
// rather than a micro-benchmark: p99 of sequential transfers on a local database stays under 250ms.
func TestBankDBLeak_TransferLatencyBudget(t *testing.T) {
	b := newBDBank(t, bdOpts{noRun: true})
	b.open("a", 1_000_000)
	b.open("b", 0)
	var lat []time.Duration
	for i := range 200 {
		ctx, cancel := context.WithTimeout(bg, 2*time.Second)
		started := time.Now()
		_, _, err := b.Transfer(ctx, transferReq{Key: fmt.Sprintf("k%03d", i), From: "a", To: "b", Amount: 1})
		cancel()
		must(t, err)
		lat = append(lat, time.Since(started))
	}
	slices.Sort(lat)
	p50, p99 := lat[len(lat)/2], lat[len(lat)*99/100]
	t.Logf("transfer latency p50=%v p99=%v max=%v", p50, p99, lat[len(lat)-1])
	if p99 > 250*time.Millisecond {
		t.Fatalf("p99 transfer latency %v exceeds the 250ms budget", p99)
	}
}

// BenchmarkBankDBTransfer measures full ACID transfers in parallel and fails if the run leaked a
// goroutine or left a connection checked out.
//
//	go test ./test/regressions/ -run '^$' -bench BankDBTransfer -benchmem
func BenchmarkBankDBTransfer(b *testing.B) {
	bank := newBDBank(b, bdOpts{noRun: true, maxOpen: 16})
	ids := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	for _, id := range ids {
		bank.open(id, 1<<40)
	}
	baseline := goleak.IgnoreCurrent()
	var seq atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := seq.Add(1)
			_, _, err := bank.Transfer(bg, transferReq{Key: fmt.Sprintf("b%d", n), From: ids[n%8], To: ids[(n+3)%8], Amount: 1})
			if err != nil && !errors.Is(err, errBankNoFunds) {
				b.Error(err)
				return
			}
		}
	})
	b.StopTimer()
	verifyNoDBLeaks(b, baseline)
}
