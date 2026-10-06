package regressions

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/pagination"
	"github.com/uptrace/bun"
)

// 26 Read contracts. A filter the code sets as a scope must never be silently widened, while a filter
// from the client may be lenient; and in-transaction reads lock by default but can be plain reads.

func TestBankDBReads_UnknownFieldsFailInScopesButAreDroppedFromClientFilters(t *testing.T) {
	b := newBDBank(t, bdOpts{noRun: true})
	ctx := withDeadline(t, 5*time.Second)
	typo := pagination.StructuredFilter{Filters: []pagination.Filter{{Field: "ownr", Mode: pagination.ModeEqual, Value: "Ann"}}}

	// A typo in a scope written by code would otherwise return every row: it must fail loudly.
	for name, call := range map[string]func() error{
		"Find":                 func() error { return second(b.accounts.Find(ctx, typo)) },
		"Count":                func() error { return second(b.accounts.Count(ctx, typo)) },
		"Exists":               func() error { return second(b.accounts.Exists(ctx, typo)) },
		"PaginateFilter scope": func() error { return second(b.accounts.PaginateFilter(ctx, typo, pagination.Pagination{})) },
	} {
		err := call()
		if !errors.Is(err, pagination.ErrUnknownField) {
			t.Errorf("%s with an unknown scope field = %v; want ErrUnknownField", name, err)
		}
		requireKind(t, err, database.ErrInvalidInput)
	}
	// The same filter coming from the client (query parameters) is dropped with a warning.
	if _, err := b.accounts.Paginate(ctx, pagination.Pagination{Filter: typo}); err != nil {
		t.Fatalf("client filter with an unknown field = %v; want it dropped", err)
	}
}

func TestBankDBReads_NonLockingReadsWorkInReadOnlyTransactionsAndDoNotBlockWriters(t *testing.T) {
	b := newBDBank(t, bdOpts{noRun: true})
	b.open("a", 100)
	ctx := withDeadline(t, 10*time.Second)

	// Default: an in-transaction read locks, which a read-only transaction refuses.
	err := database.RunInTx(ctx, b.svc, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx bun.Tx) error {
		return second(b.accounts.GetByIDWithTx(ctx, &tx, "a"))
	})
	if err == nil {
		t.Fatal("a locking read succeeded in a read-only transaction")
	}

	plain := database.WithoutRowLocks(ctx)
	err = database.RunInTx(plain, b.svc, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx bun.Tx) error {
		got, err := b.accounts.GetByIDWithTx(ctx, &tx, "a")
		if err == nil && got.Balance != 100 {
			t.Errorf("balance = %d", got.Balance)
		}
		if _, err := b.accounts.FindWithTx(ctx, &tx, pagination.StructuredFilter{}); err != nil {
			return err
		}
		// While this transaction is open, a writer is not blocked by it.
		done := make(chan error, 1)
		go func() { done <- second(b.accounts.IncrementByID(withDeadline(t, 5*time.Second), "a", "balance", 1)) }()
		select {
		case werr := <-done:
			return werr
		case <-time.After(2 * time.Second):
			return errors.New("a plain read blocked a writer")
		}
	})
	if err != nil {
		t.Fatalf("non-locking read in a read-only transaction: %v", err)
	}
	if got := b.writerBalance("a"); got != 101 {
		t.Fatalf("balance = %d; want 101", got)
	}
}
