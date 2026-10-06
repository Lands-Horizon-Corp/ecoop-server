package regressions

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/uptrace/bun"
)

// 07 Chaos and infrastructure faults: a backend killed or a TCP connection reset in the middle of a
// transaction commits nothing and the pool recovers; an unreachable database fails fast; a slow
// network honours deadlines; an exhausted pool queues instead of failing.

// debitThenFail runs the first half of a transfer (debit a) inside a transaction, triggers fault, then
// tries the second half (credit b). It returns the transaction's error.
func debitThenFail(ctx context.Context, b *bdLedger, fault func(tx bun.Tx) error) error {
	return b.svc.Writer().Client().RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if _, err := b.accounts.IncrementByIDWithTx(ctx, tx, "a", "balance", -100); err != nil {
			return err
		}
		if err := fault(tx); err != nil {
			return err
		}
		_, err := b.accounts.IncrementByIDWithTx(ctx, tx, "b", "balance", 100)
		return err
	})
}

func TestBankDBChaos_BackendKilledMidTransactionCommitsNothing(t *testing.T) {
	b := newBDBank(t, bdOpts{})
	b.open("a", 500)
	b.open("b", 0)
	ctx := withDeadline(t, 10*time.Second)

	err := debitThenFail(ctx, b, func(tx bun.Tx) error {
		var pid int
		if err := tx.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
			return err
		}
		_, err := b.h.writer.Exec(`SELECT pg_terminate_backend($1)`, pid) // the DBA / failover kills it
		return err
	})
	requireKind(t, err, database.ErrUnavailable)
	if a, bb := b.writerBalance("a"), b.writerBalance("b"); a != 500 || bb != 0 {
		t.Fatalf("balances a=%d b=%d; the half-done transfer was committed", a, bb)
	}
	if _, _, err := b.Transfer(ctx, transferReq{Key: "after", From: "a", To: "b", Amount: 100}); err != nil {
		t.Fatalf("the pool did not recover from the killed backend: %v", err)
	}
}

func TestBankDBChaos_TCPResetMidTransactionCommitsNothing(t *testing.T) {
	proxy := newFaultProxy(t)
	b := newBDBank(t, bdOpts{route: proxy.route, noRun: true})
	b.open("a", 500)
	b.open("b", 0)
	ctx := withDeadline(t, 10*time.Second)

	err := debitThenFail(ctx, b, func(bun.Tx) error {
		proxy.resetAll() // RST every live connection, including this transaction's
		return nil
	})
	requireKind(t, err, database.ErrUnavailable)
	if a, bb := b.writerBalance("a"), b.writerBalance("b"); a != 500 || bb != 0 {
		t.Fatalf("balances a=%d b=%d; the half-done transfer was committed", a, bb)
	}
	if _, _, err := b.Transfer(ctx, transferReq{Key: "after", From: "a", To: "b", Amount: 100}); err != nil {
		t.Fatalf("no recovery after the connection reset: %v", err)
	}
}

func TestBankDBChaos_UnreachableDatabaseFailsFastAndRecovers(t *testing.T) {
	proxy := newFaultProxy(t)
	b := newBDBank(t, bdOpts{route: proxy.route, noRun: true})
	b.open("a", 500)
	b.open("b", 0)

	proxy.refuse.Store(true)
	proxy.resetAll()
	ctx := withDeadline(t, 5*time.Second)
	started := time.Now()
	_, _, err := b.Transfer(ctx, transferReq{Key: "down", From: "a", To: "b", Amount: 1})
	requireKind(t, err, database.ErrUnavailable)
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("took %v to report an unreachable database; it must fail fast, not wait out the deadline", elapsed)
	}

	proxy.refuse.Store(false)
	if _, _, err := b.Transfer(withDeadline(t, 5*time.Second), transferReq{Key: "up", From: "a", To: "b", Amount: 1}); err != nil {
		t.Fatalf("no recovery once the database was reachable again: %v", err)
	}
}

func TestBankDBChaos_SlowNetworkHonoursDeadlines(t *testing.T) {
	proxy := newFaultProxy(t)
	b := newBDBank(t, bdOpts{route: proxy.route, noRun: true})
	b.open("a", 500)
	b.open("b", 0)

	proxy.latency.Store(int64(40 * time.Millisecond)) // every packet in each direction
	ctx, cancel := context.WithTimeout(bg, 30*time.Millisecond)
	defer cancel()
	_, _, err := b.Transfer(ctx, transferReq{Key: "slow", From: "a", To: "b", Amount: 1})
	requireKind(t, err, database.ErrTimeout)

	if _, _, err := b.Transfer(withDeadline(t, 20*time.Second), transferReq{Key: "patient", From: "a", To: "b", Amount: 1}); err != nil {
		t.Fatalf("a transfer with a generous deadline failed on the slow network: %v", err)
	}
	proxy.latency.Store(0)
	if a := b.writerBalance("a"); a != 499 {
		t.Fatalf("balance a = %d; want 499 (only the patient transfer applied)", a)
	}
}

func TestBankDBChaos_ExhaustedPoolQueuesTransfers(t *testing.T) {
	b := newBDBank(t, bdOpts{maxOpen: 3, noRun: true})
	ids := []string{"a", "b", "c", "d"}
	for _, id := range ids {
		b.open(id, 10_000)
	}
	errs := runParallel(t, 60, func(ctx context.Context, i int) error {
		_, _, err := b.Transfer(ctx, transferReq{Key: fmt.Sprintf("k%02d", i), From: ids[i%4], To: ids[(i+1)%4], Amount: 3})
		return err
	})
	for _, err := range errs {
		if errors.Is(err, database.ErrTimeout) {
			t.Fatalf("a transfer timed out waiting for a connection: %v", err)
		}
	}
	if len(errs) > 0 {
		t.Fatalf("%d transfers failed, first: %v", len(errs), errs[0])
	}
	stats := b.svc.Writer().Client().DB.Stats()
	if stats.WaitCount == 0 || stats.OpenConnections > 3 {
		t.Fatalf("pool stats %+v; want waits recorded and at most 3 connections", stats)
	}
	if total := b.writerTotal(); total != 40_000 {
		t.Fatalf("total = %d; want 40000", total)
	}
}
