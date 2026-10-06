package regressions

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/pagination"
	"github.com/uptrace/bun"
)

// 02 Happy path: CRUD, a full ACID transfer, chained transfers whose audit trail reconciles to the
// balances, and exact SQL <-> struct mapping for every column type the bank uses.

func TestBankDBHappy_AccountCRUD(t *testing.T) {
	b := newBDBank(t, bdOpts{})
	ctx := withDeadline(t, 10*time.Second)

	// Steps share state, so they run in order and stop at the first failure.
	steps := []struct {
		name string
		run  func(t *testing.T)
	}{
		{"create", func(t *testing.T) {
			a, err := b.accounts.Create(ctx, bdAccount{ID: "a", Owner: "Ann", Balance: 100})
			if err != nil || a.Version != 1 || a.Currency != "PHP" {
				t.Fatalf("Create = %+v, %v; want version 1, currency PHP", a, err)
			}
		}},
		// Reads inside a transaction always lock (SELECT ... FOR UPDATE), so they need a
		// read-write transaction; a ReadOnly one is rejected by Postgres (25006).
		{"read on the writer inside a transaction", func(t *testing.T) {
			must(t, b.svc.Writer().Client().RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
				a, err := b.accounts.GetByIDWithTx(ctx, &tx, "a")
				if err == nil && a.Owner != "Ann" {
					t.Errorf("owner = %q", a.Owner)
				}
				return err
			}))
		}},
		{"update", func(t *testing.T) {
			a, err := b.accounts.UpdateByID(ctx, "a", bdAccount{ID: "a", Owner: "Ann Cruz", Balance: 100, Version: 2, UpdatedAt: time.Now()})
			if err != nil || a.Owner != "Ann Cruz" {
				t.Fatalf("UpdateByID = %+v, %v", a, err)
			}
		}},
		{"increment", func(t *testing.T) {
			a, err := b.accounts.IncrementByID(ctx, "a", "balance", 25)
			if err != nil || a.Balance != 125 {
				t.Fatalf("IncrementByID = %+v, %v; want 125", a, err)
			}
		}},
		{"list from the read model", func(t *testing.T) {
			b.replicate()
			got, err := b.accounts.Find(ctx, eqFilter("owner", "Ann Cruz"))
			if err != nil || len(got) != 1 || got[0].Balance != 125 {
				t.Fatalf("Find = %+v, %v; want Ann Cruz with 125", got, err)
			}
		}},
		{"delete", func(t *testing.T) {
			must(t, b.accounts.DeleteByID(ctx, "a"))
			if n := count(t, b.h.writer, `SELECT count(*) FROM bank_accounts`); n != 0 {
				t.Fatalf("%d accounts after delete", n)
			}
		}},
	}
	for _, s := range steps {
		if !t.Run(s.name, s.run) {
			return
		}
	}
}

func TestBankDBHappy_TransferIsAtomicAndBalanced(t *testing.T) {
	b := newBDBank(t, bdOpts{})
	b.open("a", 1000)
	b.open("b", 0)

	tr, replayed, err := b.Transfer(withDeadline(t, 5*time.Second), transferReq{Key: "k1", From: "a", To: "b", Amount: 250})
	if err != nil || replayed {
		t.Fatalf("Transfer = %+v, replayed=%v, %v", tr, replayed, err)
	}
	if tr.ID != "t-k1" || tr.Amount != 250 || tr.Kind != "transfer" {
		t.Fatalf("stored transfer = %+v", tr)
	}
	for _, c := range []struct {
		query string
		want  int64
	}{
		{`SELECT balance FROM bank_accounts WHERE id = 'a'`, 750},
		{`SELECT balance FROM bank_accounts WHERE id = 'b'`, 250},
		{`SELECT version FROM bank_accounts WHERE id = 'a'`, 2},
		{`SELECT SUM(balance) FROM bank_accounts`, 1000},
		{`SELECT count(*) FROM bank_audit WHERE transfer_id = 't-k1'`, 2},
		{`SELECT SUM(delta) FROM bank_audit WHERE transfer_id = 't-k1'`, 0},
		{`SELECT balance_after FROM bank_audit WHERE id = 't-k1-dr'`, 750},
		{`SELECT balance_after FROM bank_audit WHERE id = 't-k1-cr'`, 250},
	} {
		if got := count(t, b.h.writer, c.query); got != c.want {
			t.Errorf("%s = %d; want %d", c.query, got, c.want)
		}
	}
}

func TestBankDBHappy_ChainedTransfersReconcileWithTheAuditTrail(t *testing.T) {
	b := newBDBank(t, bdOpts{})
	opening := map[string]int64{"a": 1000, "b": 500, "c": 0}
	for id, bal := range opening {
		b.open(id, bal)
	}
	chain := []transferReq{
		{"k1", "a", "b", 300}, {"k2", "b", "c", 700}, {"k3", "c", "a", 200},
		{"k4", "a", "c", 50}, {"k5", "c", "b", 100}, {"k6", "b", "a", 1},
	}
	ctx := withDeadline(t, 10*time.Second)
	for _, r := range chain {
		if _, _, err := b.Transfer(ctx, r); err != nil {
			t.Fatalf("transfer %s: %v", r.Key, err)
		}
	}
	want := map[string]int64{"a": 851, "b": 199, "c": 450}
	for id, bal := range want {
		if got := b.writerBalance(id); got != bal {
			t.Errorf("balance %s = %d; want %d", id, got, bal)
		}
		if got := count(t, b.h.writer, `SELECT SUM(delta) FROM bank_audit WHERE account_id = $1`, id); got != bal-opening[id] {
			t.Errorf("audit deltas for %s = %d; want %d", id, got, bal-opening[id])
		}
		last := count(t, b.h.writer, `SELECT a.balance_after FROM bank_audit a JOIN bank_transfers t ON t.id = a.transfer_id
			WHERE a.account_id = $1 ORDER BY t.idempotency_key DESC LIMIT 1`, id)
		if last != bal {
			t.Errorf("latest audit balance_after for %s = %d; want the live balance %d", id, last, bal)
		}
	}
}

func TestBankDBHappy_SQLToStructMappingIsExact(t *testing.T) {
	b := newBDBank(t, bdOpts{})
	ctx := withDeadline(t, 10*time.Second)
	closed := time.Now().UTC().Truncate(time.Microsecond) // Postgres stores microseconds
	in := bdAccount{
		ID: "a", Owner: "Ann", Balance: 42,
		Profile:   map[string]any{"tier": "gold", "limits": map[string]any{"daily": 5000.0}, "tags": []any{"vip", "pep"}},
		Signature: []byte{0x00, 0x01, 0xfe, 0xff},
		ClosedAt:  &closed,
	}
	created, err := b.accounts.Create(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if created.Currency != "PHP" || created.Version != 1 || created.UpdatedAt.IsZero() {
		t.Fatalf("database defaults not mapped back: %+v", created)
	}
	b.replicate()

	got, err := b.accounts.GetByID(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	checks := map[string]bool{
		"owner":      got.Owner == in.Owner,
		"balance":    got.Balance == in.Balance,
		"profile":    reflect.DeepEqual(got.Profile, in.Profile),
		"signature":  reflect.DeepEqual(got.Signature, in.Signature),
		"closed_at":  got.ClosedAt != nil && got.ClosedAt.Equal(closed),
		"updated_at": got.UpdatedAt.Equal(created.UpdatedAt),
	}
	for field, ok := range checks {
		if !ok {
			t.Errorf("%s did not survive writer -> CDC -> reader: %+v", field, got)
		}
	}
	res, err := b.accounts.FindOneFormat(ctx, pagination.StructuredFilter{Filters: []pagination.Filter{{Field: "id", Mode: pagination.ModeEqual, Value: "a"}}})
	if err != nil || *res != (bdAccountRes{ID: "a", Owner: "Ann", Currency: "PHP", Balance: 42, Closed: true}) {
		t.Fatalf("resource = %+v, %v", res, err)
	}
}
