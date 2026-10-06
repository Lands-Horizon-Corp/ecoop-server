package regressions

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/pagination"
	"github.com/uptrace/bun"
)

// 15 Tenant (branch) isolation with Postgres row-level security. The service runs as a non-superuser
// role; each request carries its branch in the context (database.WithTenant) and every package stamps
// it on the transaction, so even a buggy query cannot cross branches, and a missing tenant fails closed.

func tenantBank(t *testing.T, maxOpen int) (*bdLedger, context.Context, context.Context) {
	t.Helper()
	b := newBDBank(t, bdOpts{tenancy: true, maxOpen: maxOpen, app: "tenant"})
	branchA, branchB := database.WithTenant(bg, "branch-a"), database.WithTenant(bg, "branch-b")
	for _, x := range []struct {
		ctx context.Context
		ids []string
	}{{branchA, []string{"a1", "a2"}}, {branchB, []string{"b1", "b2"}}} {
		for _, id := range x.ids {
			if _, err := b.accounts.Create(x.ctx, bdAccount{ID: id, Owner: "owner-" + id, Balance: 1000}); err != nil {
				t.Fatalf("create %s: %v", id, err)
			}
		}
	}
	must(t, second3(b.Transfer(branchA, transferReq{Key: "ka", From: "a1", To: "a2", Amount: 100})))
	must(t, second3(b.Transfer(branchB, transferReq{Key: "kb", From: "b1", To: "b2", Amount: 200})))
	return b, branchA, branchB
}

func TestBankDBTenancy_BranchesSeeAndChangeOnlyTheirOwnRows(t *testing.T) {
	b, branchA, branchB := tenantBank(t, 8)
	b.replicate()

	found, err := b.accounts.Find(branchA, pagination.StructuredFilter{SortFields: []pagination.SortField{{Field: "id", Order: pagination.SortOrderAsc}}})
	if err != nil || fmt.Sprint(accountIDs(found)) != "[a1 a2]" {
		t.Fatalf("branch A lists %v, %v; want only a1 a2", accountIDs(found), err)
	}
	if n, err := b.transfers.Count(branchB, pagination.StructuredFilter{}); err != nil || n != 1 {
		t.Fatalf("branch B counts %d transfers, %v; want 1", n, err)
	}

	crossing := map[string]func() error{
		"read": func() error { return second(b.accounts.GetByID(branchA, "b1")) },
		"update": func() error {
			return second(b.accounts.UpdateByID(branchA, "b1", bdAccount{ID: "b1", Owner: "hijacked"}))
		},
		"increment": func() error { return second(b.accounts.IncrementByID(branchA, "b1", "balance", 1_000_000)) },
		"delete":    func() error { return b.accounts.DeleteByID(branchA, "b2") },
		"transfer": func() error {
			return second3(b.Transfer(branchA, transferReq{Key: "steal", From: "b1", To: "a1", Amount: 500}))
		},
	}
	for name, call := range crossing {
		t.Run(name, func(t *testing.T) { requireKind(t, call(), database.ErrNotFound) })
	}
	// Even a hand-written query in branch A's transaction only reaches branch A's rows.
	err = database.RunInTx(branchA, b.svc, nil, func(ctx context.Context, tx bun.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE bank_accounts SET balance = balance + 1`)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 2 {
			return fmt.Errorf("raw UPDATE touched %d rows; want branch A's 2", n)
		}
		return nil
	})
	must(t, err)
	for id, want := range map[string]int64{"b1": 800, "b2": 1200} {
		if got := b.writerBalance(id); got != want {
			t.Errorf("branch B account %s = %d; want %d (changed from branch A)", id, got, want)
		}
	}
}

func TestBankDBTenancy_MissingTenantFailsClosedEvenOnAReusedConnection(t *testing.T) {
	b, branchA, _ := tenantBank(t, 1) // one connection: the no-tenant calls reuse a session that had a tenant
	b.replicate()
	if _, err := b.accounts.GetByID(branchA, "a1"); err != nil {
		t.Fatal(err)
	}

	if found, err := b.accounts.Find(bg, pagination.StructuredFilter{}); err != nil || len(found) != 0 {
		t.Fatalf("no tenant lists %v, %v; want nothing", accountIDs(found), err)
	}
	if n, err := b.audit.Count(bg, pagination.StructuredFilter{}); err != nil || n != 0 {
		t.Fatalf("no tenant counts %d audit rows, %v; want 0", n, err)
	}
	_, err := b.accounts.Create(bg, bdAccount{ID: "orphan", Owner: "nobody", Balance: 1})
	requireKind(t, err, database.ErrForbidden)
	_, _, err = b.Transfer(bg, transferReq{Key: "anon", From: "a1", To: "a2", Amount: 1})
	requireKind(t, err, database.ErrNotFound)
	if n := count(t, b.h.writer, `SELECT count(*) FROM bank_accounts WHERE tenant_id IS NULL OR tenant_id = ''`); n != 0 {
		t.Fatalf("%d rows without a tenant were written", n)
	}
}

func TestBankDBTenancy_WritingIntoAnotherBranchIsForbidden(t *testing.T) {
	b, branchA, _ := tenantBank(t, 4)
	other := "branch-b"
	_, err := b.accounts.Create(branchA, bdAccount{ID: "smuggled", Owner: "x", Balance: 1, TenantID: &other})
	requireKind(t, err, database.ErrForbidden)
	_, err = b.audit.Create(branchA, bdAudit{ID: "fake", TransferID: "t-ka", AccountID: "a1", Delta: 1, TenantID: &other})
	requireKind(t, err, database.ErrForbidden)
}

func TestBankDBTenancy_CursorFromAnotherBranchRevealsNothing(t *testing.T) {
	b, branchA, branchB := tenantBank(t, 4)
	for i := range 6 {
		_, err := b.accounts.Create(branchA, bdAccount{ID: fmt.Sprintf("a%02d", i+10), Owner: fmt.Sprintf("x%d", i), Balance: int64(i)})
		must(t, err)
	}
	b.replicate()
	page := pagination.Pagination{PageSize: 3, Filter: pagination.StructuredFilter{SortFields: []pagination.SortField{{Field: "id", Order: pagination.SortOrderAsc}}}}
	first, err := b.accounts.Paginate(branchA, page)
	must(t, err)
	page.Cursor = first.NextCursor

	stolen, err := b.accounts.Paginate(branchB, page) // branch A's cursor replayed by branch B
	if err != nil {
		return // rejecting it is fine too
	}
	for _, row := range stolen.Data {
		if row.TenantID == nil || *row.TenantID != "branch-b" {
			t.Fatalf("branch B received %s from branch %v through a replayed cursor", row.ID, row.TenantID)
		}
	}
}

func TestBankDBTenancy_TheReplicatorMirrorsEveryBranch(t *testing.T) {
	b, branchA, branchB := tenantBank(t, 4)
	b.replicate() // through the runner, which runs as the replicator on the reader
	for q, want := range map[string]int64{
		`SELECT count(*) FROM bank_accounts WHERE tenant_id = 'branch-a'`: 2,
		`SELECT count(*) FROM bank_accounts WHERE tenant_id = 'branch-b'`: 2,
		`SELECT count(*) FROM bank_audit WHERE tenant_id IS NULL`:         0,
	} {
		if got := count(t, b.h.reader, q); got != want {
			t.Errorf("reader %s = %d; want %d", q, got, want)
		}
	}
	a, err := b.accounts.GetByID(branchA, "a2")
	if err != nil || a.Balance != 1100 {
		t.Fatalf("branch A reads a2 from the reader as %+v, %v", a, err)
	}
	if _, err := b.accounts.GetByID(branchB, "a2"); !errors.Is(database.MapError(err), database.ErrNotFound) {
		t.Fatalf("branch B read branch A's account on the reader: %v", err)
	}
}

func accountIDs(rows []*bdAccount) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.ID
	}
	return out
}
