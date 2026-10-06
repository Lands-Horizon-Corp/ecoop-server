package regressions

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/uptrace/bun"
)

// 06 Concurrency, races and isolation (run with -race): pessimistic locking prevents double spending
// and lost updates, ordered locking prevents deadlocks, concurrent readers never observe half a
// transfer, and SERIALIZABLE writers converge with retries.

func TestBankDBConcurrency_NoDoubleSpending(t *testing.T) {
	b := newBDBank(t, bdOpts{})
	b.open("a", 1000)
	b.open("b", 0)
	var ok, refused atomic.Int64

	errs := runParallel(t, 50, func(ctx context.Context, i int) error {
		_, _, err := b.Transfer(ctx, transferReq{Key: fmt.Sprintf("k%02d", i), From: "a", To: "b", Amount: 100})
		switch {
		case err == nil:
			ok.Add(1)
		case errors.Is(err, errBankNoFunds):
			refused.Add(1)
		default:
			return err
		}
		return nil
	})
	if len(errs) > 0 {
		t.Fatalf("%d transfers failed unexpectedly, first: %v", len(errs), errs[0])
	}
	if ok.Load() != 10 || refused.Load() != 40 {
		t.Fatalf("%d succeeded and %d were refused; want exactly 10 and 40 (balance 1000 / 100)", ok.Load(), refused.Load())
	}
	for q, want := range map[string]int64{
		`SELECT balance FROM bank_accounts WHERE id = 'a'`: 0,
		`SELECT balance FROM bank_accounts WHERE id = 'b'`: 1000,
		`SELECT count(*) FROM bank_transfers`:              10,
		`SELECT count(*) FROM bank_audit`:                  20,
	} {
		if got := count(t, b.h.writer, q); got != want {
			t.Errorf("%s = %d; want %d", q, got, want)
		}
	}
}

func TestBankDBConcurrency_RandomTransfersConserveMoneyWithoutDeadlocks(t *testing.T) {
	b := newBDBank(t, bdOpts{})
	ids := []string{"a", "b", "c", "d", "e"}
	for _, id := range ids {
		b.open(id, 1000)
	}
	errs := runParallel(t, 200, func(ctx context.Context, i int) error {
		r := rand.New(rand.NewPCG(uint64(i), 42))
		from := ids[r.IntN(len(ids))]
		to := ids[(r.IntN(len(ids)-1)+1+indexOf(ids, from))%len(ids)]
		_, _, err := b.Transfer(ctx, transferReq{Key: fmt.Sprintf("k%03d", i), From: from, To: to, Amount: int64(r.IntN(300) + 1)})
		if errors.Is(err, errBankNoFunds) {
			return nil
		}
		return err
	})
	for _, err := range errs {
		if errors.Is(err, database.ErrSerialization) {
			t.Fatalf("deadlock or serialization failure under ordered locking: %v", err)
		}
	}
	if len(errs) > 0 {
		t.Fatalf("%d transfers failed, first: %v", len(errs), errs[0])
	}
	if total := b.writerTotal(); total != 5000 {
		t.Fatalf("total = %d; want 5000 (money created or destroyed)", total)
	}
	if n := count(t, b.h.writer, `SELECT count(*) FROM bank_accounts WHERE balance < 0`); n != 0 {
		t.Fatalf("%d negative balances", n)
	}
	if n := count(t, b.h.writer, `SELECT count(*) FROM bank_accounts a WHERE a.balance - 1000 <>
		COALESCE((SELECT SUM(delta) FROM bank_audit WHERE account_id = a.id), 0)`); n != 0 {
		t.Fatalf("%d accounts whose balance does not match their audit trail", n)
	}
}

func TestBankDBConcurrency_ReadersNeverSeeHalfATransfer(t *testing.T) {
	b := newBDBank(t, bdOpts{maxOpen: 16})
	for _, id := range []string{"a", "b", "c"} {
		b.open(id, 1000)
	}
	stop := make(chan struct{})
	var torn atomic.Int64
	readers := runParallelAsync(t, 10, func(ctx context.Context) error {
		for {
			select {
			case <-stop:
				return nil
			default:
			}
			var total int64
			if err := b.svc.Writer().Client().NewSelect().Model((*bdAccount)(nil)).ColumnExpr("SUM(balance)").Scan(ctx, &total); err != nil {
				return err
			}
			if total != 3000 {
				torn.Add(1)
			}
		}
	})
	errs := runParallel(t, 100, func(ctx context.Context, i int) error {
		pairs := [][2]string{{"a", "b"}, {"b", "c"}, {"c", "a"}}
		p := pairs[i%3]
		_, _, err := b.Transfer(ctx, transferReq{Key: fmt.Sprintf("k%03d", i), From: p[0], To: p[1], Amount: 7})
		return err
	})
	close(stop)
	if rerrs := readers(); len(rerrs) > 0 {
		t.Fatalf("reader failed: %v", rerrs[0])
	}
	if len(errs) > 0 {
		t.Fatalf("transfer failed: %v", errs[0])
	}
	if torn.Load() > 0 {
		t.Fatalf("readers saw a total other than 3000 %d times: a transfer was visible half-applied", torn.Load())
	}
}

func TestBankDBConcurrency_SerializableWritersConvergeWithRetry(t *testing.T) {
	b := newBDBank(t, bdOpts{maxOpen: 16})
	b.open("a", 0)
	serializable := &sql.TxOptions{Isolation: sql.LevelSerializable}
	var retries atomic.Int64

	errs := runParallel(t, 40, func(ctx context.Context, _ int) error {
		return retrySerializable(ctx, 50, func() error {
			err := b.svc.Writer().Client().RunInTx(ctx, serializable, func(ctx context.Context, tx bun.Tx) error {
				var bal int64 // read-modify-write without a row lock: only SERIALIZABLE keeps it correct
				if err := tx.NewSelect().Model((*bdAccount)(nil)).Column("balance").Where("id = 'a'").Scan(ctx, &bal); err != nil {
					return err
				}
				_, err := tx.NewUpdate().Model((*bdAccount)(nil)).Set("balance = ?", bal+1).Where("id = 'a'").Exec(ctx)
				return err
			})
			if errors.Is(database.MapError(err), database.ErrSerialization) {
				retries.Add(1)
			}
			return err
		})
	})
	if len(errs) > 0 {
		t.Fatalf("%d writers gave up, first: %v", len(errs), errs[0])
	}
	if got := b.writerBalance("a"); got != 40 {
		t.Fatalf("balance = %d; want 40 (a lost update slipped through)", got)
	}
	t.Logf("%d serialization retries", retries.Load())
}

func TestBankDBConcurrency_LockedRowBlocksWritersUntilCommit(t *testing.T) {
	b := newBDBank(t, bdOpts{})
	b.open("a", 100)
	b.open("b", 0)
	ctx := withDeadline(t, 10*time.Second)

	holder, err := b.accounts.StartTx(ctx)
	must(t, err)
	if _, err := b.accounts.GetByIDWithTx(ctx, &holder, "a"); err != nil { // SELECT ... FOR UPDATE
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := b.Transfer(ctx, transferReq{Key: "k", From: "a", To: "b", Amount: 50})
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("transfer finished (%v) while the row was locked", err)
	case <-time.After(200 * time.Millisecond):
	}
	if _, err := b.accounts.UpdateByIDWithTx(ctx, holder, "a", bdAccount{ID: "a", Owner: "owner-a", Balance: 50, Version: 2, UpdatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	must(t, b.accounts.EndTx(ctx, holder, nil))
	if err := <-done; err != nil {
		t.Fatalf("transfer after the lock was released: %v", err)
	}
	// The waiting transfer re-read the committed 50; had it used the stale 100, a would be 50.
	if a := b.writerBalance("a"); a != 0 {
		t.Fatalf("balance a = %d; want 0 (50 means the transfer overwrote the holder's update)", a)
	}
}

func indexOf(xs []string, x string) int {
	for i, v := range xs {
		if v == x {
			return i
		}
	}
	return -1
}

// runParallelAsync starts n goroutines running fn and returns a function that waits for them and
// returns their errors.
func runParallelAsync(t *testing.T, n int, fn func(ctx context.Context) error) func() []error {
	t.Helper()
	ctx, cancel := context.WithTimeout(bg, 60*time.Second)
	results := make(chan error, n)
	for range n {
		go func() { results <- fn(ctx) }()
	}
	return func() []error {
		defer cancel()
		var errs []error
		for range n {
			if err := <-results; err != nil {
				errs = append(errs, err)
			}
		}
		return errs
	}
}
