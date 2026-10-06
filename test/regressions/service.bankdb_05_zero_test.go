package regressions

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/cqrs"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/pagination"
	"github.com/uptrace/bun"
)

// 05 Zero, absence and memory safety: zero values stay distinct from NULL, NULLs scan to nil, and nil
// or zero-value transactions, missing relations and empty collections produce structured errors or
// empty results, never a nil-pointer panic.

func TestBankDBZero_ZeroValuesStayDistinctFromNull(t *testing.T) {
	b := newBDBank(t, bdOpts{})
	ctx := withDeadline(t, 10*time.Second)
	must(t, second(b.accounts.Create(ctx, bdAccount{ID: "zero", Owner: "z", Balance: 0, Profile: map[string]any{}, Signature: []byte{}})))
	must(t, second(b.accounts.Create(ctx, bdAccount{ID: "null", Owner: "n", Balance: 0})))

	for _, c := range []struct {
		query string
		want  int64
	}{
		{`SELECT count(*) FROM bank_accounts WHERE id = 'zero' AND balance = 0 AND profile = '{}'::jsonb AND signature = ''::bytea`, 1},
		{`SELECT count(*) FROM bank_accounts WHERE id = 'null' AND profile IS NULL AND signature IS NULL AND closed_at IS NULL`, 1},
	} {
		if got := count(t, b.h.writer, c.query); got != c.want {
			t.Errorf("%s = %d; want %d", c.query, got, c.want)
		}
	}
	b.replicate()
	zero, err := b.accounts.GetByID(ctx, "zero")
	must(t, err)
	null, err := b.accounts.GetByID(ctx, "null")
	must(t, err)
	if zero.Profile == nil || len(zero.Profile) != 0 || zero.Balance != 0 {
		t.Errorf("zero account read back as %+v; want an empty non-nil profile and balance 0", zero)
	}
	if null.Profile != nil || null.ClosedAt != nil || len(null.Signature) != 0 {
		t.Errorf("NULL account read back as %+v; want nil optional fields", null)
	}
	res, err := b.accounts.GetByIDFormat(ctx, "null")
	if err != nil || res.Closed {
		t.Fatalf("serializer on NULL closed_at = %+v, %v", res, err)
	}
}

func TestBankDBZero_NilAndZeroTransactionsFailGracefully(t *testing.T) {
	b := newBDBank(t, bdOpts{})
	ctx := withDeadline(t, 5*time.Second)
	b.open("a", 10)
	var zero bun.Tx
	var nilTx *bun.Tx
	all := pagination.StructuredFilter{}

	calls := map[string]func() error{
		"CreateWithTx": func() error { return second(b.accounts.CreateWithTx(ctx, zero, bdAccount{ID: "x", Owner: "x"})) },
		"CreateManyWithTx": func() error {
			return second(b.accounts.CreateManyWithTx(ctx, zero, []bdAccount{{ID: "x", Owner: "x"}}))
		},
		"UpdateByIDWithTx": func() error {
			return second(b.accounts.UpdateByIDWithTx(ctx, zero, "a", bdAccount{ID: "a", Owner: "a"}))
		},
		"UpdateManyWithTx": func() error {
			return second(b.accounts.UpdateManyWithTx(ctx, zero, []bdAccount{{ID: "a", Owner: "a"}}))
		},
		"DeleteByIDWithTx":    func() error { return b.accounts.DeleteByIDWithTx(ctx, zero, "a") },
		"DeleteManyWithTx":    func() error { return b.accounts.DeleteManyWithTx(ctx, zero, []string{"a"}) },
		"IncrementByIDWithTx": func() error { return second(b.accounts.IncrementByIDWithTx(ctx, zero, "a", "balance", 1)) },
		"EndTx(commit)":       func() error { return b.accounts.EndTx(ctx, zero, nil) },
		"GetByIDWithTx":       func() error { return second(b.accounts.GetByIDWithTx(ctx, nilTx, "a")) },
		"FindWithTx":          func() error { return second(b.accounts.FindWithTx(ctx, nilTx, all)) },
		"FindOneWithTx":       func() error { return second(b.accounts.FindOneWithTx(ctx, nilTx, all)) },
		"CountWithTx":         func() error { return second(b.accounts.CountWithTx(ctx, nilTx, all)) },
		"ExistsWithTx":        func() error { return second(b.accounts.ExistsWithTx(ctx, nilTx, all)) },
		"MaxWithTx":           func() error { return second(b.accounts.MaxWithTx(ctx, nilTx, "balance", all)) },
		"FilterWithTx":        func() error { return second(b.accounts.FilterWithTx(ctx, nilTx, all)) },
		"GetByIDWithTx(zero)": func() error { return second(b.accounts.GetByIDWithTx(ctx, &zero, "a")) },
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			noPanic(t, name, func() {
				if err := call(); !errors.Is(err, cqrs.ErrNilTx) && !errors.Is(err, pagination.ErrNilTx) {
					t.Errorf("%s with a nil/zero transaction = %v; want ErrNilTx", name, err)
				}
			})
		})
	}
	if got := b.writerBalance("a"); got != 10 {
		t.Fatalf("balance = %d; a call on a nil transaction changed data", got)
	}
}

func TestBankDBZero_MissingRelationsFailWithStructuredErrors(t *testing.T) {
	b := newBDBank(t, bdOpts{})
	ctx := withDeadline(t, 5*time.Second)
	b.open("a", 10)

	cases := map[string]struct {
		call func() (any, error)
		kind error
	}{
		"audit row for a missing transfer": {func() (any, error) {
			return b.audit.Create(ctx, bdAudit{ID: "x", TransferID: "ghost", AccountID: "a", Delta: 1})
		}, database.ErrForeignKey},
		"lookup of a missing account": {func() (any, error) { return b.accounts.GetByID(ctx, "ghost") }, database.ErrNotFound},
		"serialized lookup":           {func() (any, error) { return b.accounts.GetByIDFormat(ctx, "ghost") }, database.ErrNotFound},
		"transfer from a missing account": {func() (any, error) {
			tr, _, err := b.Transfer(ctx, transferReq{"k", "ghost", "a", 1})
			return tr, err
		}, database.ErrNotFound},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			var got any
			var err error
			noPanic(t, name, func() { got, err = c.call() })
			requireKind(t, err, c.kind)
			if v := reflect.ValueOf(got); got != nil && !v.IsNil() {
				t.Fatalf("returned a non-nil model %+v alongside the error", got)
			}
		})
	}
}

func TestBankDBZero_EmptyCollectionsAreSafe(t *testing.T) {
	b := newBDBank(t, bdOpts{})
	ctx := withDeadline(t, 5*time.Second)

	tx, err := b.accounts.StartTx(ctx)
	must(t, err)
	created, err := b.accounts.CreateManyWithTx(ctx, tx, nil)
	if err != nil || created == nil || len(created) != 0 {
		t.Errorf("CreateManyWithTx(nil) = %#v, %v; want an empty non-nil slice", created, err)
	}
	if err := b.accounts.DeleteManyWithTx(ctx, tx, nil); err != nil {
		t.Errorf("DeleteManyWithTx(nil) = %v", err)
	}
	must(t, b.accounts.EndTx(ctx, tx, nil))

	found, err := b.accounts.Find(ctx, eqFilter("owner", "nobody"))
	if err != nil || len(found) != 0 {
		t.Errorf("Find with no matches = %v, %v", found, err)
	}
	if rs := b.accounts.ToModels(found); len(rs) != 0 {
		t.Errorf("ToModels(empty) = %v", rs)
	}
	page, err := b.accounts.PaginateFormat(ctx, pagination.Pagination{PageSize: 10})
	if err != nil || len(page.Data) != 0 || page.NextCursor != nil {
		t.Errorf("PaginateFormat on empty table = %+v, %v", page, err)
	}
}

func TestBankDBZero_CursorOverANullableSortColumnVisitsEveryRow(t *testing.T) {
	b := newBDBank(t, bdOpts{noRun: true})
	for i := range 12 {
		var closed any // every third account is open (closed_at NULL)
		if i%3 != 0 {
			closed = time.Date(2026, 1, 1, i, 0, 0, 0, time.UTC)
		}
		_, err := b.h.reader.Exec(`INSERT INTO bank_accounts (id, owner, balance, closed_at) VALUES ($1, $1, 0, $2)`, fmt.Sprintf("n%02d", i), closed)
		must(t, err)
	}
	ctx := withDeadline(t, 10*time.Second)
	page := pagination.Pagination{PageSize: 5, Filter: pagination.StructuredFilter{
		SortFields: []pagination.SortField{{Field: "closed_at", Order: pagination.SortOrderAsc}}}}
	seen := map[string]int{}
	var pages []pagination.PaginationResult[bdAccount]
	for {
		res, err := b.accounts.Paginate(ctx, page)
		must(t, err)
		pages = append(pages, res)
		for _, a := range res.Data {
			seen[a.ID]++
		}
		if res.NextCursor == nil {
			break
		}
		page.Cursor = res.NextCursor
	}
	if len(seen) != 12 {
		t.Fatalf("visited %d distinct rows; want 12 (rows with NULL lost): %v", len(seen), seen)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("%s visited %d times", id, n)
		}
	}
	page.Cursor = pages[1].PreviousCursor
	back, err := b.accounts.Paginate(ctx, page)
	must(t, err)
	if fmt.Sprint(accountIDs(back.Data)) != fmt.Sprint(accountIDs(pages[0].Data)) {
		t.Fatalf("backward page = %v; want %v", accountIDs(back.Data), accountIDs(pages[0].Data))
	}
}
