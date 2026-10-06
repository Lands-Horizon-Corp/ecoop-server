package regressions

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/uptrace/bun"
)

// 03 Sad path: every database failure surfaces as a mapped application error (errors.Is on a
// database.Err* kind) whose message carries no schema detail, while the Postgres cause stays
// available for logs; failed business operations leave no trace.

func TestBankDBSad_ConstraintViolationsMapToApplicationErrors(t *testing.T) {
	b := newBDBank(t, bdOpts{})
	b.open("a", 100)
	b.open("b", 0)
	ctx := withDeadline(t, 10*time.Second)
	if _, _, err := b.Transfer(ctx, transferReq{Key: "k1", From: "a", To: "b", Amount: 10}); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name  string
		call  func() error
		kind  error
		state string
	}{
		{"duplicate primary key", func() error { return second(b.accounts.Create(ctx, bdAccount{ID: "a", Owner: "x", Balance: 1})) }, database.ErrDuplicate, "23505"},
		{"duplicate owner+currency", func() error { return second(b.accounts.Create(ctx, bdAccount{ID: "z", Owner: "owner-a", Balance: 1})) }, database.ErrDuplicate, "23505"},
		{"transfer from unknown account", func() error {
			return second(b.transfers.Create(ctx, bdTransfer{ID: "t2", IdempotencyKey: "k2", FromAccount: "ghost", ToAccount: "b", Amount: 1, RequestHash: "h"}))
		}, database.ErrForeignKey, "23503"},
		{"delete a referenced account", func() error { return b.accounts.DeleteByID(ctx, "a") }, database.ErrForeignKey, "23503"},
		{"negative balance", func() error { return second(b.accounts.IncrementByID(ctx, "b", "balance", -1000)) }, database.ErrConstraint, "23514"},
		{"transfer to self", func() error {
			return second(b.transfers.Create(ctx, bdTransfer{ID: "t3", IdempotencyKey: "k3", FromAccount: "a", ToAccount: "a", Amount: 1, RequestHash: "h"}))
		}, database.ErrConstraint, "23514"},
		{"update a missing account", func() error { return second(b.accounts.UpdateByID(ctx, "ghost", bdAccount{ID: "ghost", Owner: "g"})) }, database.ErrNotFound, ""},
		{"delete a missing account", func() error { return b.accounts.DeleteByID(ctx, "ghost") }, database.ErrNotFound, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.call()
			requireKind(t, err, c.kind)
			var mapped *database.MappedError
			if !errors.As(database.MapError(err), &mapped) || mapped.SQLState() != c.state {
				t.Fatalf("SQLSTATE kept for logs = %q; want %q", mapped.SQLState(), c.state)
			}
		})
	}
	if got := b.writerTotal(); got != 100 {
		t.Fatalf("total balance = %d after only failed writes; want 100", got)
	}
}

func TestBankDBSad_SerializationFailureIsMappedAndRetried(t *testing.T) {
	b := newBDBank(t, bdOpts{})
	b.open("a", 100)
	b.open("b", 100)
	ctx := withDeadline(t, 10*time.Second)
	serializable := &sql.TxOptions{Isolation: sql.LevelSerializable}

	// Write skew: both read the total, then each debits a different account.
	begin := func() bun.Tx {
		tx, err := b.svc.Writer().Client().BeginTx(ctx, serializable)
		must(t, err)
		var total int64
		must(t, tx.NewSelect().Model((*bdAccount)(nil)).ColumnExpr("SUM(balance)").Scan(ctx, &total))
		return tx
	}
	t1, t2 := begin(), begin()
	_, err1 := b.accounts.IncrementByIDWithTx(ctx, t1, "a", "balance", -60)
	_, err2 := b.accounts.IncrementByIDWithTx(ctx, t2, "b", "balance", -60)
	must(t, errors.Join(err1, err2))
	must(t, t1.Commit())
	err := t2.Commit()
	_ = t2.Rollback()
	requireKind(t, err, database.ErrSerialization)

	// The documented client contract: retry the whole transaction.
	attempts := 0
	err = retrySerializable(ctx, 5, func() error {
		attempts++
		return b.svc.Writer().Client().RunInTx(ctx, serializable, func(ctx context.Context, tx bun.Tx) error {
			_, err := b.accounts.IncrementByIDWithTx(ctx, tx, "b", "balance", -60)
			return err
		})
	})
	if err != nil {
		t.Fatalf("retried transaction failed after %d attempts: %v", attempts, err)
	}
	if a, bb := b.writerBalance("a"), b.writerBalance("b"); a != 40 || bb != 40 {
		t.Fatalf("balances a=%d b=%d; want 40 and 40", a, bb)
	}
}

func TestBankDBSad_FailedTransfersLeaveNoTrace(t *testing.T) {
	b := newBDBank(t, bdOpts{})
	b.open("a", 100)
	b.open("b", 0)
	ctx := withDeadline(t, 10*time.Second)

	cases := []struct {
		name string
		req  transferReq
		want error
	}{
		{"insufficient funds", transferReq{"k1", "a", "b", 101}, errBankNoFunds},
		{"unknown destination", transferReq{"k2", "a", "ghost", 1}, database.ErrNotFound},
		{"unknown source", transferReq{"k3", "ghost", "b", 1}, database.ErrNotFound},
		{"zero amount", transferReq{"k4", "a", "b", 0}, errBankBadAmount},
		{"negative amount", transferReq{"k5", "a", "b", -50}, errBankBadAmount},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tr, _, err := b.Transfer(ctx, c.req)
			if tr != nil || !errors.Is(err, c.want) {
				t.Fatalf("Transfer = %+v, %v; want nil, %v", tr, err, c.want)
			}
		})
	}
	for query, want := range map[string]int64{
		`SELECT balance FROM bank_accounts WHERE id = 'a'`: 100,
		`SELECT version FROM bank_accounts WHERE id = 'a'`: 1,
		`SELECT count(*) FROM bank_transfers`:              0,
		`SELECT count(*) FROM bank_audit`:                  0,
	} {
		if got := count(t, b.h.writer, query); got != want {
			t.Errorf("%s = %d; want %d", query, got, want)
		}
	}
}

func TestBankDBSad_MappedErrorsDoNotLeakSchema(t *testing.T) {
	b := newBDBank(t, bdOpts{})
	b.open("a", 1)
	ctx := withDeadline(t, 5*time.Second)
	_, raw := b.accounts.Create(ctx, bdAccount{ID: "a", Owner: "x", Balance: 1})
	if raw == nil || !strings.Contains(raw.Error(), "bank_accounts") {
		t.Fatalf("expected the raw error to name the table, got %v", raw)
	}

	mapped := database.MapError(raw)
	for _, secret := range []string{"bank_", "pkey", "constraint", "violates", "duplicate key", "SQLSTATE"} {
		if strings.Contains(mapped.Error(), secret) {
			t.Errorf("mapped message %q leaks %q", mapped.Error(), secret)
		}
	}
	if database.MapError(mapped) != mapped {
		t.Error("MapError is not idempotent")
	}
	if database.MapError(nil) != nil {
		t.Error("MapError(nil) != nil")
	}
	m, ok := errors.AsType[*database.MappedError](mapped)
	if !ok || !strings.Contains(m.Cause.Error(), "bank_accounts") || m.SQLState() != "23505" {
		t.Error("the raw cause is no longer reachable for logging")
	}
}
