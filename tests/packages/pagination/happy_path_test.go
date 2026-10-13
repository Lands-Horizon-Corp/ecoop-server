//go:build integration

package pagination_test

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pagination"
)

const emile = "Émile"

// Every filter mode returns exactly the rows it should. The seed table is
// in helpers_test.go.
func TestHappy_FilterModes(t *testing.T) {
	fx := need(t)
	cases := []struct {
		name string
		sf   pagination.StructuredFilter
		want []string
	}{
		{"equal", and(f("name", pagination.ModeEqual, "Alice")), []string{"Alice"}},
		{"equal on tier", and(f("tier", pagination.ModeEqual, "gold")), []string{"Alice", "Carol", emile, "Grace"}},
		{"equal on bool", and(f("active", pagination.ModeEqual, true)), []string{"Alice", "Carol", "Dave_100%", emile, "Grace", "Heidi"}},
		{"notEqual", and(f("tier", pagination.ModeNotEqual, "gold")), []string{"Bob", "Dave_100%", "O'Brien", "Frank", "Heidi", "Ivan"}},
		{"gt", and(f("balance", pagination.ModeGT, 100)), []string{"Bob", emile}},
		{"gte", and(f("balance", pagination.ModeGTE, 100)), []string{"Alice", "Bob", emile, "Grace", "Heidi"}},
		{"lt", and(f("balance", pagination.ModeLT, 50)), []string{"Carol", "Frank", "Ivan"}},
		{"lte", and(f("balance", pagination.ModeLTE, 50)), []string{"Carol", "Dave_100%", "Frank", "Ivan"}},
		{"gt on negative", and(f("balance", pagination.ModeGT, -21)), []string{"Alice", "Bob", "Carol", "Dave_100%", "O'Brien", emile, "Frank", "Grace", "Heidi", "Ivan"}},
		{"contains", and(f("name", pagination.ModeContains, "ra")), []string{"Grace", "Frank"}},
		{"notContains", and(f("name", pagination.ModeNotContains, "a")), []string{"Alice", "Bob", "O'Brien", emile, "Heidi"}},
		{"startsWith", and(f("name", pagination.ModeStartsWith, "Al")), []string{"Alice"}},
		{"endsWith", and(f("name", pagination.ModeEndsWith, "e")), []string{"Alice", "Grace", emile}},
		{"inside", and(f("tier", pagination.ModeInside, []any{"gold", "bronze"})), []string{"Alice", "Carol", emile, "Grace", "Dave_100%", "Frank", "Ivan"}},
		{"inside typed slice", and(f("tier", pagination.ModeInside, []string{"silver"})), []string{"Bob", "O'Brien", "Heidi"}},
		{"outside", and(f("tier", pagination.ModeOutside, []any{"gold", "bronze"})), []string{"Bob", "O'Brien", "Heidi"}},
		{"range from a map", and(f("balance", pagination.ModeRange, map[string]any{"from": 50, "to": 100})), []string{"Alice", "Dave_100%", "O'Brien", "Grace", "Heidi"}},
		{"range from RangeNumber", and(f("balance", pagination.ModeRange, pagination.RangeNumber{From: 0, To: 50})), []string{"Carol", "Dave_100%", "Ivan"}},
		{"isEmpty covers NULL and empty string", and(f("email", pagination.ModeIsEmpty, nil)), []string{"Carol", "O'Brien", "Frank", "Ivan"}},
		{"isNotEmpty", and(f("email", pagination.ModeIsNotEmpty, nil)), []string{"Alice", "Bob", "Dave_100%", emile, "Grace", "Heidi"}},
		{"no filters returns every row", and(), []string{"Alice", "Bob", "Carol", "Dave_100%", "O'Brien", emile, "Frank", "Grace", "Heidi", "Ivan"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wantSet(t, names(t, fx, tc.sf), tc.want...)
		})
	}
}

// Dates and times are given as strings and coerced by DataType.
func TestHappy_DateFilters(t *testing.T) {
	fx := need(t)
	dated := func(mode pagination.Mode, v any) pagination.StructuredFilter {
		return and(pagination.Filter{Field: "created_at", Mode: mode, Value: v, DataType: pagination.DataTypeDate})
	}
	t.Run("after", func(t *testing.T) {
		t.Parallel()
		wantSet(t, names(t, fx, dated(pagination.ModeAfter, "2024-09-01")), "Ivan")
	})
	t.Run("before", func(t *testing.T) {
		t.Parallel()
		wantSet(t, names(t, fx, dated(pagination.ModeBefore, "2024-02-01")), "Alice")
	})
	t.Run("equal", func(t *testing.T) {
		t.Parallel()
		wantSet(t, names(t, fx, dated(pagination.ModeEqual, "2024-03-01")), "Carol")
	})
	t.Run("range", func(t *testing.T) {
		t.Parallel()
		got := names(t, fx, dated(pagination.ModeRange, map[string]any{"from": "2024-02-01", "to": "2024-04-01"}))
		wantSet(t, got, "Bob", "Carol", "Dave_100%")
	})
	t.Run("inside", func(t *testing.T) {
		t.Parallel()
		got := names(t, fx, dated(pagination.ModeInside, []any{"2024-01-01", "2024-10-01"}))
		wantSet(t, got, "Alice", "Ivan")
	})
	t.Run("RFC3339", func(t *testing.T) {
		t.Parallel()
		wantSet(t, names(t, fx, dated(pagination.ModeAfter, "2024-09-01T00:00:00Z")), "Ivan")
	})
	t.Run("camelCase field is normalized to its column", func(t *testing.T) {
		t.Parallel()
		sf := and(pagination.Filter{Field: "createdAt", Mode: pagination.ModeAfter, Value: "2024-09-01", DataType: pagination.DataTypeDate})
		got, err := fx.svc.Paginate(ctxT(t), pagination.Pagination{Filter: sf})
		if err != nil {
			t.Fatal(err)
		}
		wantSet(t, nameList(got.Data), "Ivan")
	})
}

// Several filters combine with AND or OR; a group never leaks into the next.
func TestHappy_Logic(t *testing.T) {
	fx := need(t)
	t.Run("and narrows", func(t *testing.T) {
		t.Parallel()
		sf := and(
			f("tier", pagination.ModeEqual, "gold"),
			f("active", pagination.ModeEqual, true),
			f("balance", pagination.ModeGTE, 100),
		)
		wantSet(t, names(t, fx, sf), "Alice", emile, "Grace")
	})
	t.Run("or widens", func(t *testing.T) {
		t.Parallel()
		sf := or(f("tier", pagination.ModeEqual, "bronze"), f("name", pagination.ModeEqual, "Alice"))
		wantSet(t, names(t, fx, sf), "Alice", "Dave_100%", "Frank", "Ivan")
	})
	t.Run("empty logic behaves as and", func(t *testing.T) {
		t.Parallel()
		sf := pagination.StructuredFilter{Filters: []pagination.Filter{
			f("tier", pagination.ModeEqual, "gold"), f("balance", pagination.ModeEqual, 0),
		}}
		wantSet(t, names(t, fx, sf), "Carol")
	})
	t.Run("hardcoded filter and request filter both apply", func(t *testing.T) {
		t.Parallel()
		res, err := fx.svc.PaginateFilter(ctxT(t), and(f("tier", pagination.ModeEqual, "gold")), pagination.Pagination{
			Filter: and(f("active", pagination.ModeEqual, true), f("balance", pagination.ModeGT, 0)),
		})
		if err != nil {
			t.Fatal(err)
		}
		wantSet(t, nameList(res.Data), "Alice", emile, "Grace")
	})
}

// Unicode is compared in its normalized form, so a decomposed "E + acute"
// finds the precomposed row.
func TestHappy_UnicodeNormalized(t *testing.T) {
	fx := need(t)
	wantSet(t, names(t, fx, and(f("name", pagination.ModeEqual, "Émile"))), emile)
}

// LIKE wildcards in user input are literal text.
func TestHappy_LikeWildcardsAreLiteral(t *testing.T) {
	fx := need(t)
	t.Run("percent", func(t *testing.T) {
		t.Parallel()
		wantSet(t, names(t, fx, and(f("name", pagination.ModeContains, "%"))), "Dave_100%")
	})
	t.Run("underscore", func(t *testing.T) {
		t.Parallel()
		wantSet(t, names(t, fx, and(f("name", pagination.ModeContains, "_"))), "Dave_100%")
	})
	t.Run("backslash", func(t *testing.T) {
		t.Parallel()
		wantSet(t, names(t, fx, and(f("name", pagination.ModeContains, `\`))))
	})
	t.Run("starts with underscore wildcard finds nothing", func(t *testing.T) {
		t.Parallel()
		wantSet(t, names(t, fx, and(f("name", pagination.ModeStartsWith, "_"))))
	})
}

// ModeCustom hands the query to the caller's function.
func TestHappy_CustomFilter(t *testing.T) {
	fx := need(t)
	var seen any
	custom := pagination.Filter{Mode: pagination.ModeCustom, Value: 150, Custom: func(q *bun.SelectQuery, v any) (*bun.SelectQuery, error) {
		seen = v
		return q.Where("balance > ?", v), nil
	}}
	wantSet(t, names(t, fx, and(custom)), "Bob", emile)
	if seen != 150 {
		t.Fatalf("custom filter saw value %v, want 150", seen)
	}
}

// Count, Exists, Find and FindOne agree with Filter on the same filter.
func TestHappy_ReadersAgree(t *testing.T) {
	fx := need(t)
	ctx := ctxT(t)
	sf := and(f("tier", pagination.ModeEqual, "silver"))

	n, err := fx.svc.Count(ctx, sf)
	if err != nil || n != 3 {
		t.Fatalf("Count = %d, %v; want 3", n, err)
	}
	ok, err := fx.svc.Exists(ctx, sf)
	if err != nil || !ok {
		t.Fatalf("Exists = %v, %v; want true", ok, err)
	}
	rows, err := fx.svc.Find(ctx, sf)
	if err != nil || len(rows) != 3 {
		t.Fatalf("Find = %d rows, %v; want 3", len(rows), err)
	}
	one, err := fx.svc.FindOne(ctx, and(f("name", pagination.ModeEqual, "Bob")))
	if err != nil || one.Name != "Bob" {
		t.Fatalf("FindOne = %+v, %v", one, err)
	}
}

func TestHappy_MaxMin(t *testing.T) {
	fx := need(t)
	ctx := ctxT(t)
	hi, err := fx.svc.GetMax(ctx, "balance", and())
	if err != nil || hi.Name != emile {
		t.Fatalf("GetMax = %+v, %v; want %s", hi, err, emile)
	}
	lo, err := fx.svc.GetMin(ctx, "balance", and())
	if err != nil || lo.Name != "Frank" {
		t.Fatalf("GetMin = %+v, %v; want Frank", lo, err)
	}
	hiGold, err := fx.svc.GetMax(ctx, "balance", and(f("tier", pagination.ModeEqual, "silver")))
	if err != nil || hiGold.Name != "Bob" {
		t.Fatalf("GetMax(silver) = %+v, %v; want Bob", hiGold, err)
	}
}

// A filter with no match is an empty page, not an error: no rows, no
// cursors, the page size echoed back.
func TestHappy_EmptyResultIsGraceful(t *testing.T) {
	fx := need(t)
	ctx := ctxT(t)
	sf := and(f("name", pagination.ModeEqual, "nobody"))

	res := page(t, fx, sf, 5, nil)
	if res.Data == nil || len(res.Data) != 0 {
		t.Fatalf("Data = %#v, want an empty non-nil slice (JSON [] not null)", res.Data)
	}
	if res.NextCursor != nil || res.PreviousCursor != nil || res.CurrentCursor != nil {
		t.Fatalf("empty page has cursors: %+v", res)
	}
	if res.PageSize != 5 {
		t.Fatalf("PageSize = %d, want 5", res.PageSize)
	}
	if n, err := fx.svc.Count(ctx, sf); err != nil || n != 0 {
		t.Fatalf("Count = %d, %v; want 0", n, err)
	}
	if ok, err := fx.svc.Exists(ctx, sf); err != nil || ok {
		t.Fatalf("Exists = %v, %v; want false", ok, err)
	}
	if rows, err := fx.svc.Find(ctx, sf); err != nil || len(rows) != 0 {
		t.Fatalf("Find = %d rows, %v; want 0, nil", len(rows), err)
	}
	if _, err := fx.svc.FindOne(ctx, sf); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("FindOne err = %v, want sql.ErrNoRows", err)
	}
	if _, err := fx.svc.GetMax(ctx, "balance", sf); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("GetMax err = %v, want sql.ErrNoRows", err)
	}
}
