//go:build integration

package pagination_test

import (
	"context"
	"errors"
	"testing"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pagination"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// The zero value of a field is a real value to filter on, distinct from
// "absent".
func TestZero_ZeroIsAValue(t *testing.T) {
	fx := need(t)
	t.Run("number zero", func(t *testing.T) {
		t.Parallel()
		wantSet(t, names(t, fx, and(f("balance", pagination.ModeEqual, 0))), "Carol", "Ivan")
	})
	t.Run("false", func(t *testing.T) {
		t.Parallel()
		wantSet(t, names(t, fx, and(f("active", pagination.ModeEqual, false))), "Bob", "O'Brien", "Frank", "Ivan")
	})
	t.Run("empty string equals only empty strings, not NULL", func(t *testing.T) {
		t.Parallel()
		wantSet(t, names(t, fx, and(f("email", pagination.ModeEqual, ""))), "O'Brien", "Ivan")
	})
	t.Run("empty string contains matches every non-null text", func(t *testing.T) {
		t.Parallel()
		got := names(t, fx, and(f("email", pagination.ModeContains, "")))
		if len(got) != 8 { // the two NULL emails never match LIKE
			t.Fatalf("matched %v, want the 8 non-NULL emails", got)
		}
	})
	t.Run("negative number", func(t *testing.T) {
		t.Parallel()
		wantSet(t, names(t, fx, and(f("balance", pagination.ModeLT, 0))), "Frank")
	})
}

// NULL follows SQL three-valued logic: NULL rows match neither a value nor
// its negation. isEmpty / isNotEmpty are how to ask about absence.
func TestZero_NullSemantics(t *testing.T) {
	fx := need(t)
	t.Run("notEqual skips NULL", func(t *testing.T) {
		t.Parallel()
		got := names(t, fx, and(f("email", pagination.ModeNotEqual, "alice@x.com")))
		wantSet(t, got, "Bob", "Dave_100%", "O'Brien", emile, "Grace", "Heidi", "Ivan")
	})
	t.Run("notContains skips NULL", func(t *testing.T) {
		t.Parallel()
		got := names(t, fx, and(f("email", pagination.ModeNotContains, "@x.com")))
		wantSet(t, got, "O'Brien", "Ivan")
	})
	t.Run("outside skips NULL", func(t *testing.T) {
		t.Parallel()
		got := names(t, fx, and(f("email", pagination.ModeOutside, []any{"alice@x.com"})))
		wantSet(t, got, "Bob", "Dave_100%", "O'Brien", emile, "Grace", "Heidi", "Ivan")
	})
	t.Run("isEmpty and isNotEmpty partition the table", func(t *testing.T) {
		t.Parallel()
		empty := names(t, fx, and(f("email", pagination.ModeIsEmpty, nil)))
		full := names(t, fx, and(f("email", pagination.ModeIsNotEmpty, nil)))
		if len(empty)+len(full) != 10 {
			t.Fatalf("%v + %v do not cover the 10 rows", empty, full)
		}
	})
	t.Run("a stray value on isEmpty is ignored", func(t *testing.T) {
		t.Parallel()
		wantSet(t, names(t, fx, and(f("email", pagination.ModeIsEmpty, "whatever"))), "Carol", "O'Brien", "Frank", "Ivan")
	})
}

// isEmpty on a non-text column. The generated SQL compares the column with
// ” (score = ”), which Postgres rejects for integers, so absence can only
// be asked of text columns today. Desired behavior: NULL rows match.
func TestZero_IsEmptyOnNonTextColumn(t *testing.T) {
	fx := need(t)
	got, err := fx.svc.Filter(ctxT(t), and(f("score", pagination.ModeIsEmpty, nil)))
	if err != nil {
		t.Fatalf("isEmpty on an integer column failed: %v", err)
	}
	wantSet(t, nameList(got), "Carol", emile)
}

// Rows scanned with NULL columns come back with nil pointers, each its own
// allocation: no shared or dangling pointer between rows or calls.
func TestZero_NilPointersInResults(t *testing.T) {
	fx := need(t)
	rows, err := fx.svc.Filter(ctxT(t), and(f("name", pagination.ModeInside, []any{"Carol", "Alice", "Frank"})))
	if err != nil || len(rows) != 3 {
		t.Fatalf("rows = %d, %v", len(rows), err)
	}
	seen := map[*Account]bool{}
	for _, r := range rows {
		if r == nil || seen[r] {
			t.Fatalf("nil or aliased row pointer in %v", rows)
		}
		seen[r] = true
		switch r.Name {
		case "Carol", "Frank":
			if r.Email != nil {
				t.Fatalf("%s: email = %q, want nil", r.Name, *r.Email)
			}
		case "Alice":
			if r.Email == nil || *r.Email != "alice@x.com" {
				t.Fatalf("Alice: email = %v", r.Email)
			}
		}
	}
	// Mutating a result must not corrupt the next read.
	for _, r := range rows {
		r.Name, r.Email = "mutated", nil
	}
	again := names(t, fx, and(f("name", pagination.ModeEqual, "Alice")))
	wantSet(t, again, "Alice")
}

// A NULL in the sort column is followed by a working cursor (a nil sort
// pointer must not become the string "").
func TestZero_NullSortValuesPage(t *testing.T) {
	fx := need(t)
	got := walk(t, fx, filterWithSort(and(), asc("score")), 3)
	if len(got) != 10 {
		t.Fatalf("walked %d rows %v, want 10", len(got), got)
	}
	// NULLS LAST: the two NULL scores come at the end.
	if tail := got[8:]; !sameSet(tail, []string{"Carol", emile}) {
		t.Fatalf("tail = %v, want the NULL-score rows last", tail)
	}
}

// Unset / empty inputs fall back to defaults.
func TestZero_DefaultsAndEmptyInputs(t *testing.T) {
	fx := need(t)
	t.Run("zero Pagination", func(t *testing.T) {
		t.Parallel()
		res, err := fx.svc.Paginate(ctxT(t), pagination.Pagination{})
		if err != nil || len(res.Data) != 10 || res.PageSize != 30 {
			t.Fatalf("got %d rows, page size %d, %v; want 10 rows, size 30", len(res.Data), res.PageSize, err)
		}
	})
	t.Run("negative page size", func(t *testing.T) {
		t.Parallel()
		res, err := fx.svc.Paginate(ctxT(t), pagination.Pagination{PageSize: -5})
		if err != nil || res.PageSize != 30 {
			t.Fatalf("page size %d, %v; want 30", res.PageSize, err)
		}
	})
	t.Run("empty cursor string is no cursor", func(t *testing.T) {
		t.Parallel()
		empty := ""
		res := page(t, fx, and(), 4, &empty)
		if len(res.Data) != 4 || res.PreviousCursor != nil {
			t.Fatalf("got %d rows, previous %v; want the first page of 4", len(res.Data), res.PreviousCursor)
		}
	})
	t.Run("nil filters slice and nil sort", func(t *testing.T) {
		t.Parallel()
		res, err := fx.svc.Paginate(ctxT(t), pagination.Pagination{Filter: pagination.StructuredFilter{}})
		if err != nil || len(res.Data) != 10 {
			t.Fatalf("got %d rows, %v", len(res.Data), err)
		}
	})
	t.Run("sort with no order defaults to ascending", func(t *testing.T) {
		t.Parallel()
		res := page(t, fx, filterWithSort(and(), pagination.SortField{Field: "balance"}), 3, nil)
		if res.Data[0].Name != "Frank" {
			t.Fatalf("first = %s, want Frank (lowest balance)", res.Data[0].Name)
		}
	})
}

// A service that was never wired fails with an error, not a nil dereference.
func TestZero_UnwiredService(t *testing.T) {
	ctx := context.Background()
	zero := &pagination.PaginationService[Account, int64]{}
	mustNotPanic(t, "zero service", func() {
		if _, err := zero.Filter(ctx, and()); !errors.Is(err, pagination.ErrReadServiceRequired) {
			t.Fatalf("Filter err = %v, want ErrReadServiceRequired", err)
		}
		if _, err := zero.Paginate(ctx, pagination.Pagination{}); !errors.Is(err, pagination.ErrReadServiceRequired) {
			t.Fatalf("Paginate err = %v", err)
		}
		if _, err := zero.Count(ctx, and()); !errors.Is(err, pagination.ErrReadServiceRequired) {
			t.Fatalf("Count err = %v", err)
		}
		if _, err := zero.Exists(ctx, and()); !errors.Is(err, pagination.ErrReadServiceRequired) {
			t.Fatalf("Exists err = %v", err)
		}
	})
	t.Run("constructor needs a database service", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("expected a panic when no SQL service is given")
			}
		}()
		pagination.NewPaginationService(&pagination.PaginationService[Account, int64]{}, utils.NewInflight())
	})
	t.Run("constructor fills defaults", func(t *testing.T) {
		fx := need(t)
		svc := pagination.NewPaginationService(&pagination.PaginationService[Account, int64]{WriteSQLService: fx.sql}, utils.NewInflight())
		if _, err := svc.Filter(ctxT(t), and()); err != nil {
			t.Fatal(err)
		}
	})
}

// A nil or never-started transaction is an error, not a crash.
func TestZero_NilTransaction(t *testing.T) {
	fx := need(t)
	ctx := ctxT(t)
	for name, tx := range map[string]*bun.Tx{"nil": nil, "empty": {}} {
		mustNotPanic(t, name, func() {
			if _, err := fx.svc.FilterWithTx(ctx, tx, and()); err == nil {
				t.Errorf("%s tx: FilterWithTx returned no error", name)
			}
			if _, err := fx.svc.FindOneWithTx(ctx, tx, and()); err == nil {
				t.Errorf("%s tx: FindOneWithTx returned no error", name)
			}
			if _, err := fx.svc.ExistsWithTx(ctx, tx, and()); err == nil {
				t.Errorf("%s tx: ExistsWithTx returned no error", name)
			}
		})
	}
}
