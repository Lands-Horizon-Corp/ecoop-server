package regressions

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/cqrs"
)

// 10 Idempotency and financial auditability: a request replayed with the same Idempotency-Key moves
// money once (sequentially or concurrently) and returns the original result; reusing a key for a
// different request is refused; the final state does not depend on execution or delivery order; and
// the audit trail cannot be altered and always reconciles to the balances.

func TestBankDBIdempotency_SequentialReplayReturnsTheOriginal(t *testing.T) {
	b := newBDBank(t, bdOpts{noRun: true})
	b.open("a", 1000)
	b.open("b", 0)
	ctx := withDeadline(t, 10*time.Second)
	req := transferReq{Key: "pay-123", From: "a", To: "b", Amount: 250}

	first, replayed, err := b.Transfer(ctx, req)
	if err != nil || replayed {
		t.Fatalf("first call = %v, replayed=%v", err, replayed)
	}
	for i := range 5 {
		again, replayed, err := b.Transfer(ctx, req)
		if err != nil || !replayed || again.ID != first.ID || again.UpdatedAt != first.UpdatedAt {
			t.Fatalf("replay %d = %+v, replayed=%v, %v; want the original transfer", i, again, replayed, err)
		}
	}
	if a, bb := b.writerBalance("a"), b.writerBalance("b"); a != 750 || bb != 250 {
		t.Fatalf("balances a=%d b=%d; want 750/250 (money moved more than once)", a, bb)
	}
	if n := count(t, b.h.writer, `SELECT count(*) FROM bank_audit`); n != 2 {
		t.Fatalf("%d audit rows; want 2", n)
	}
}

func TestBankDBIdempotency_ConcurrentReplaysMoveMoneyOnce(t *testing.T) {
	b := newBDBank(t, bdOpts{noRun: true, maxOpen: 16})
	b.open("a", 1000)
	b.open("b", 0)
	req := transferReq{Key: "pay-dup", From: "a", To: "b", Amount: 100}
	var originals atomic.Int64
	ids := make(chan string, 30)

	errs := runParallel(t, 30, func(ctx context.Context, _ int) error {
		tr, replayed, err := b.Transfer(ctx, req)
		if err != nil {
			return err
		}
		if !replayed {
			originals.Add(1)
		}
		ids <- tr.ID
		return nil
	})
	close(ids)
	if len(errs) > 0 {
		t.Fatalf("%d concurrent replays failed, first: %v", len(errs), errs[0])
	}
	if originals.Load() != 1 {
		t.Fatalf("%d calls executed the transfer; want exactly 1", originals.Load())
	}
	for id := range ids {
		if id != "t-pay-dup" {
			t.Fatalf("a replay returned transfer %q; want t-pay-dup", id)
		}
	}
	if a := b.writerBalance("a"); a != 900 {
		t.Fatalf("balance a = %d; want 900", a)
	}
}

func TestBankDBIdempotency_KeyReuseWithADifferentRequestIsRefused(t *testing.T) {
	b := newBDBank(t, bdOpts{noRun: true})
	b.open("a", 1000)
	b.open("b", 0)
	ctx := withDeadline(t, 10*time.Second)
	must(t, second3(b.Transfer(ctx, transferReq{Key: "k", From: "a", To: "b", Amount: 100})))

	for name, req := range map[string]transferReq{
		"different amount":      {Key: "k", From: "a", To: "b", Amount: 101},
		"different destination": {Key: "k", From: "b", To: "a", Amount: 100},
	} {
		if tr, _, err := b.Transfer(ctx, req); !errors.Is(err, errBankKeyReused) || tr != nil {
			t.Errorf("%s with a used key = %+v, %v; want errBankKeyReused", name, tr, err)
		}
	}
	if a := b.writerBalance("a"); a != 900 {
		t.Fatalf("balance a = %d; want 900", a)
	}
}

func TestBankDBIdempotency_ExecutionOrderDoesNotChangeTheOutcome(t *testing.T) {
	transfers := []transferReq{
		{"k1", "a", "b", 120}, {"k2", "b", "c", 40}, {"k3", "c", "d", 75}, {"k4", "d", "a", 10},
		{"k5", "a", "c", 33}, {"k6", "b", "d", 61}, {"k7", "c", "a", 5}, {"k8", "d", "b", 18},
	}
	orders := map[string][]int{
		"forward":  {0, 1, 2, 3, 4, 5, 6, 7},
		"reverse":  {7, 6, 5, 4, 3, 2, 1, 0},
		"shuffled": {3, 0, 6, 2, 7, 5, 1, 4},
	}
	type outcome struct{ balances, audit string }
	results := map[string]outcome{}
	for name, order := range orders {
		t.Run(name, func(t *testing.T) {
			b := newBDBank(t, bdOpts{noRun: true})
			for _, id := range []string{"a", "b", "c", "d"} {
				b.open(id, 500) // enough that no order can be refused
			}
			ctx := withDeadline(t, 20*time.Second)
			for _, i := range order {
				must(t, second3(b.Transfer(ctx, transfers[i])))
			}
			for _, i := range slices.Backward(order) { // replaying everything changes nothing
				if _, replayed, err := b.Transfer(ctx, transfers[i]); err != nil || !replayed {
					t.Fatalf("replay of %s = replayed %v, %v", transfers[i].Key, replayed, err)
				}
			}
			results[name] = outcome{
				balances: rowsString(t, b, `SELECT id || '=' || balance FROM bank_accounts ORDER BY id`),
				audit:    rowsString(t, b, `SELECT transfer_id || ':' || account_id || ':' || delta FROM bank_audit ORDER BY 1`),
			}
		})
	}
	if results["forward"] != results["reverse"] || results["forward"] != results["shuffled"] {
		t.Fatalf("outcome depends on execution order:\n%+v", results)
	}
}

func TestBankDBIdempotency_OutOfOrderDeliveryKeepsTheNewestVersion(t *testing.T) {
	b := newBDBank(t, bdOpts{})
	events := b.events["bank_accounts"]
	version := func(v, bal int64) bdAccount {
		return bdAccount{ID: "a", Owner: "Ann", Balance: bal, Version: v, UpdatedAt: time.Unix(1_700_000_000+v, 0)}
	}

	// Separate batches: v3 lands first, then the stale v1 and v2 arrive.
	dbPublish(t, events, "a:v3", cqrs.ChangeTypeUpdated, version(3, 300))
	b.awaitReader(`SELECT count(*) FROM bank_accounts WHERE version = 3`, 1)
	dbPublish(t, events, "a:v1", cqrs.ChangeTypeCreated, version(1, 100))
	b.awaitReader(`SELECT count(*) FROM processed_events WHERE event_id = 'a:v1'`, 1)
	dbPublish(t, events, "a:v2", cqrs.ChangeTypeUpdated, version(2, 200))
	b.awaitReader(`SELECT count(*) FROM processed_events WHERE event_id = 'a:v2'`, 1)
	if v := count(t, b.h.reader, `SELECT version FROM bank_accounts WHERE id = 'a'`); v != 3 {
		t.Fatalf("reader is at version %d; a stale event overwrote the newest state", v)
	}

	// Same batch: v5 then v4 pushed back to back.
	dbPublish(t, events, "a:v5", cqrs.ChangeTypeUpdated, version(5, 500))
	dbPublish(t, events, "a:v4", cqrs.ChangeTypeUpdated, version(4, 400))
	b.awaitReader(`SELECT count(*) FROM processed_events WHERE event_id IN ('a:v4', 'a:v5')`, 2)
	if bal := count(t, b.h.reader, `SELECT balance FROM bank_accounts WHERE id = 'a'`); bal != 500 {
		t.Fatalf("reader balance = %d; want 500 from version 5", bal)
	}
}

func TestBankDBIdempotency_AuditTrailIsImmutableAndReconciles(t *testing.T) {
	b := newBDBank(t, bdOpts{noRun: true})
	opening := map[string]int64{"a": 1000, "b": 1000}
	for id, bal := range opening {
		b.open(id, bal)
	}
	ctx := withDeadline(t, 10*time.Second)
	for i := range 6 {
		from, to := "a", "b"
		if i%2 == 1 {
			from, to = to, from
		}
		must(t, second3(b.Transfer(ctx, transferReq{Key: fmt.Sprintf("k%d", i), From: from, To: to, Amount: int64(10 * (i + 1))})))
	}
	before := rowsString(t, b, `SELECT id || ':' || delta || ':' || balance_after FROM bank_audit ORDER BY id`)

	tampering := map[string]func() error{
		"cqrs update": func() error {
			return second(b.audit.UpdateByID(ctx, "t-k0-dr", bdAudit{ID: "t-k0-dr", TransferID: "t-k0", AccountID: "a", Delta: -1, BalanceAfter: 999}))
		},
		"cqrs increment": func() error { return second(b.audit.IncrementByID(ctx, "t-k0-dr", "delta", 1)) },
		"cqrs delete":    func() error { return b.audit.DeleteByID(ctx, "t-k0-dr") },
		"cqrs delete many": func() error {
			return b.audit.DeleteMany(ctx, []string{"t-k0-dr", "t-k0-cr"})
		},
		"raw sql update": func() error {
			_, err := b.svc.Writer().Client().ExecContext(ctx, `UPDATE bank_audit SET delta = 0`)
			return err
		},
	}
	for name, tamper := range tampering {
		t.Run(name, func(t *testing.T) { requireKind(t, tamper(), database.ErrRejected) })
	}
	if after := rowsString(t, b, `SELECT id || ':' || delta || ':' || balance_after FROM bank_audit ORDER BY id`); after != before {
		t.Fatalf("audit trail changed:\nbefore %s\nafter  %s", before, after)
	}
	for id, open := range opening {
		if diff := count(t, b.h.writer, `SELECT COALESCE(SUM(delta), 0) FROM bank_audit WHERE account_id = $1`, id); diff != b.writerBalance(id)-open {
			t.Errorf("account %s: audit says %+d, balance moved %+d", id, diff, b.writerBalance(id)-open)
		}
	}
	if n := count(t, b.h.writer, `SELECT count(*) FROM (SELECT transfer_id FROM bank_audit GROUP BY transfer_id HAVING SUM(delta) <> 0) x`); n != 0 {
		t.Fatalf("%d transfers whose audit entries do not balance to zero", n)
	}
}

func second3[A, B any](_ A, _ B, err error) error { return err }

func rowsString(t testing.TB, b *bdLedger, query string) string {
	t.Helper()
	rows, err := b.h.writer.Query(query)
	must(t, err)
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		must(t, rows.Scan(&s))
		out = append(out, s)
	}
	must(t, rows.Err())
	return strings.Join(out, ",")
}
