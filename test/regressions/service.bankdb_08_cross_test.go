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

// 08 Cross-package chaos: one context and one transaction cross all three packages. Package A (cqrs)
// records the transfer and its audit rows, package B (pagination + cqrs) locks and updates both
// balances, package C (raw sql on the same transaction) fails in a different way each case. Every
// failure must roll back A and B completely: no orphaned transfer or audit row, balances unchanged,
// and the connection returned to the pool.

var errPackageC = errors.New("package C: compliance check failed")

func TestBankDBCross_UpstreamFailureRollsBackEveryPackage(t *testing.T) {
	cases := []struct {
		name    string
		ctx     func() (context.Context, context.CancelFunc)
		failC   func(ctx context.Context, tx bun.Tx, cancel context.CancelFunc) error
		want    error // mapped kind (or errPackageC)
		panics  bool
		timeout bool
	}{
		{name: "business error", failC: func(context.Context, bun.Tx, context.CancelFunc) error { return errPackageC }, want: errPackageC},
		{name: "panic", failC: func(context.Context, bun.Tx, context.CancelFunc) error { panic("package C crashed") }, panics: true},
		{name: "client disconnect (context cancelled)", failC: func(ctx context.Context, tx bun.Tx, cancel context.CancelFunc) error {
			cancel()
			_, err := tx.ExecContext(ctx, `SELECT 1`)
			return err
		}, want: database.ErrCanceled},
		{name: "shared deadline expires in package C", ctx: func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(bg, 300*time.Millisecond)
		}, failC: func(ctx context.Context, tx bun.Tx, _ context.CancelFunc) error {
			_, err := tx.ExecContext(ctx, `SELECT pg_sleep(5)`)
			return err
		}, want: database.ErrTimeout},
		{name: "constraint violation in package C", failC: func(ctx context.Context, tx bun.Tx, _ context.CancelFunc) error {
			_, err := tx.ExecContext(ctx, `INSERT INTO bank_audit (id, transfer_id, account_id, delta, balance_after) VALUES ('c', 't-x', 'a', 0, 0)`)
			return err
		}, want: database.ErrConstraint},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := newBDBank(t, bdOpts{noRun: true})
			b.open("a", 1000)
			b.open("b", 0)
			ctx, cancel := context.WithTimeout(bg, 10*time.Second)
			if c.ctx != nil {
				ctx, cancel = c.ctx()
			}
			defer cancel()

			var err error
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				err = b.svc.Writer().Client().RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
					// A: cqrs records the transfer and the audit trail.
					if _, err := b.transfers.CreateWithTx(ctx, tx, bdTransfer{ID: "t-x", IdempotencyKey: "x", FromAccount: "a", ToAccount: "b", Amount: 300, RequestHash: "h"}); err != nil {
						return err
					}
					if _, err := b.audit.CreateManyWithTx(ctx, tx, []bdAudit{
						{ID: "t-x-dr", TransferID: "t-x", AccountID: "a", Delta: -300, BalanceAfter: 700},
						{ID: "t-x-cr", TransferID: "t-x", AccountID: "b", Delta: 300, BalanceAfter: 300},
					}); err != nil {
						return err
					}
					// B: pagination locks both accounts, cqrs moves the money.
					if _, err := b.accounts.FindWithTx(ctx, &tx, eqFilter("owner", "owner-a")); err != nil {
						return err
					}
					if _, err := b.accounts.IncrementByIDWithTx(ctx, tx, "a", "balance", -300); err != nil {
						return err
					}
					if _, err := b.accounts.IncrementByIDWithTx(ctx, tx, "b", "balance", 300); err != nil {
						return err
					}
					// C: fails.
					return c.failC(ctx, tx, cancel)
				})
			}()

			switch {
			case c.panics:
				if recovered == nil {
					t.Fatal("the panic in package C was swallowed")
				}
			case errors.Is(c.want, errPackageC):
				if !errors.Is(err, errPackageC) {
					t.Fatalf("err = %v; want the package C error", err)
				}
			default:
				requireKind(t, err, c.want)
			}
			for q, want := range map[string]int64{
				`SELECT balance FROM bank_accounts WHERE id = 'a'`: 1000,
				`SELECT balance FROM bank_accounts WHERE id = 'b'`: 0,
				`SELECT count(*) FROM bank_transfers`:              0,
				`SELECT count(*) FROM bank_audit`:                  0,
			} {
				if got := count(t, b.h.writer, q); got != want {
					t.Errorf("after rollback %s = %d; want %d", q, got, want)
				}
			}
			// assertPoolsDrained (cleanup) verifies the transaction's connection came back.
		})
	}
}

func TestBankDBCross_SavepointContainsAFailedInnerStep(t *testing.T) {
	b := newBDBank(t, bdOpts{noRun: true})
	b.open("a", 1000)
	b.open("b", 0)
	ctx := withDeadline(t, 10*time.Second)

	err := b.svc.Writer().Client().RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if _, err := b.accounts.IncrementByIDWithTx(ctx, tx, "a", "balance", -100); err != nil {
			return err
		}
		if _, err := b.accounts.IncrementByIDWithTx(ctx, tx, "b", "balance", 100); err != nil {
			return err
		}
		// An optional fee step fails inside a savepoint; only that step is rolled back.
		inner := tx.RunInTx(ctx, nil, func(ctx context.Context, sp bun.Tx) error {
			if _, err := b.accounts.IncrementByIDWithTx(ctx, sp, "b", "balance", -5); err != nil {
				return err
			}
			return errPackageC
		})
		if !errors.Is(inner, errPackageC) {
			return fmt.Errorf("savepoint error = %v", inner)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("outer transaction: %v", err)
	}
	if a, bb := b.writerBalance("a"), b.writerBalance("b"); a != 900 || bb != 100 {
		t.Fatalf("balances a=%d b=%d; want 900 and 100 (fee rolled back, transfer kept)", a, bb)
	}
}

func TestBankDBCross_CommittedPipelineIsVisibleThroughEveryPackage(t *testing.T) {
	b := newBDBank(t, bdOpts{})
	b.open("a", 1000)
	b.open("b", 0)
	ctx := withDeadline(t, 10*time.Second)

	tr, _, err := b.Transfer(ctx, transferReq{Key: "k", From: "a", To: "b", Amount: 400})
	must(t, err)
	b.replicate()

	if got, err := b.transfers.GetByID(ctx, tr.ID); err != nil || got.Amount != 400 {
		t.Fatalf("pagination on the reader: transfer = %+v, %v", got, err)
	}
	if n, err := b.audit.Count(ctx, eqFilter("transfer_id", tr.ID)); err != nil || n != 2 {
		t.Fatalf("audit rows on the reader = %d, %v; want 2", n, err)
	}
	var total int64
	must(t, b.svc.Reader().Client().NewSelect().Model((*bdAccount)(nil)).ColumnExpr("SUM(balance)").Scan(ctx, &total))
	if total != 1000 {
		t.Fatalf("sql on the reader: total = %d; want 1000", total)
	}
}
