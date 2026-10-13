//go:build integration

package pagination_test

import (
	"context"
	"encoding/base64"
	"errors"
	"net/url"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pagination"
)

// Filter, Count... take the hardcoded filter, which is checked strictly:
// a bad one is an error the caller can match with errors.Is.
func TestSad_InvalidFilters(t *testing.T) {
	fx := need(t)
	cases := []struct {
		name string
		f    pagination.Filter
		want error
	}{
		{"unknown field", f("nope", pagination.ModeEqual, 1), pagination.ErrUnknownField},
		{"unknown mode", f("name", pagination.Mode("regex"), "a"), pagination.ErrInvalidFilter},
		{"empty mode", f("name", "", "a"), pagination.ErrInvalidFilter},
		{"equal needs a value", f("name", pagination.ModeEqual, nil), pagination.ErrInvalidFilter},
		{"gt needs a value", f("balance", pagination.ModeGT, nil), pagination.ErrInvalidFilter},
		{"contains needs a value", f("name", pagination.ModeContains, nil), pagination.ErrInvalidFilter},
		{"range needs a value", f("balance", pagination.ModeRange, nil), pagination.ErrInvalidFilter},
		{"inside needs a list", f("tier", pagination.ModeInside, nil), pagination.ErrInvalidFilter},
		{"inside rejects a scalar", f("tier", pagination.ModeInside, "gold"), pagination.ErrInvalidFilter},
		{"outside rejects a scalar", f("tier", pagination.ModeOutside, 7), pagination.ErrInvalidFilter},
		{"range rejects a scalar", f("balance", pagination.ModeRange, 5), pagination.ErrInvalidFilter},
		{"range rejects a list", f("balance", pagination.ModeRange, []any{1, 2}), pagination.ErrInvalidFilter},
		{"range missing to", f("balance", pagination.ModeRange, map[string]any{"from": 1}), pagination.ErrInvalidFilter},
		{"range missing from", f("balance", pagination.ModeRange, map[string]any{"to": 1}), pagination.ErrInvalidFilter},
		{"range null bound", f("balance", pagination.ModeRange, map[string]any{"from": nil, "to": 1}), pagination.ErrInvalidFilter},
		{"custom without a function", pagination.Filter{Mode: pagination.ModeCustom}, pagination.ErrInvalidFilter},
		{"bad date", pagination.Filter{Field: "created_at", Mode: pagination.ModeAfter, Value: "yesterday", DataType: pagination.DataTypeDate}, pagination.ErrInvalidFilter},
		{"bad time", pagination.Filter{Field: "created_at", Mode: pagination.ModeAfter, Value: "25:99", DataType: pagination.DataTypeTime}, pagination.ErrInvalidFilter},
		{"bad date inside a list", pagination.Filter{Field: "created_at", Mode: pagination.ModeInside, Value: []any{"2024-01-01", "nope"}, DataType: pagination.DataTypeDate}, pagination.ErrInvalidFilter},
		{"bad date in a range", pagination.Filter{Field: "created_at", Mode: pagination.ModeRange, Value: map[string]any{"from": "2024-01-01", "to": "nope"}, DataType: pagination.DataTypeDate}, pagination.ErrInvalidFilter},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rows, err := fx.svc.Filter(ctxT(t), and(tc.f))
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v (rows %d), want %v", err, len(rows), tc.want)
			}
			if rows != nil {
				t.Fatalf("returned %d rows alongside an error", len(rows))
			}
		})
	}
}

// A custom filter's own error and a nil query both surface.
func TestSad_CustomFilterFailures(t *testing.T) {
	fx := need(t)
	boom := errors.New("boom")
	t.Run("its error is returned", func(t *testing.T) {
		t.Parallel()
		c := pagination.Filter{Mode: pagination.ModeCustom, Custom: func(*bun.SelectQuery, any) (*bun.SelectQuery, error) { return nil, boom }}
		if _, err := fx.svc.Filter(ctxT(t), and(c)); !errors.Is(err, boom) {
			t.Fatalf("err = %v, want boom", err)
		}
	})
	t.Run("a nil query is rejected", func(t *testing.T) {
		t.Parallel()
		c := pagination.Filter{Mode: pagination.ModeCustom, Custom: func(*bun.SelectQuery, any) (*bun.SelectQuery, error) { return nil, nil }}
		if _, err := fx.svc.Filter(ctxT(t), and(c)); !errors.Is(err, pagination.ErrInvalidFilter) {
			t.Fatalf("err = %v, want ErrInvalidFilter", err)
		}
	})
	t.Run("a failing term fails the whole filter even after good ones", func(t *testing.T) {
		t.Parallel()
		c := pagination.Filter{Mode: pagination.ModeCustom, Custom: func(*bun.SelectQuery, any) (*bun.SelectQuery, error) { return nil, boom }}
		if _, err := fx.svc.Filter(ctxT(t), and(f("tier", pagination.ModeEqual, "gold"), c)); !errors.Is(err, boom) {
			t.Fatalf("err = %v, want boom", err)
		}
	})
}

// The same mistakes through Paginate, whose user-supplied filters go through
// normalization first.
func TestSad_PaginateInvalidFilters(t *testing.T) {
	fx := need(t)
	run := func(t *testing.T, fl pagination.Filter) error {
		t.Helper()
		_, err := fx.svc.Paginate(ctxT(t), pagination.Pagination{Filter: and(fl)})
		return err
	}
	t.Run("bad mode is an error", func(t *testing.T) {
		t.Parallel()
		if err := run(t, f("name", pagination.Mode("regex"), "a")); !errors.Is(err, pagination.ErrInvalidFilter) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("nil value is an error", func(t *testing.T) {
		t.Parallel()
		if err := run(t, f("name", pagination.ModeEqual, nil)); !errors.Is(err, pagination.ErrInvalidFilter) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("unknown sort field", func(t *testing.T) {
		t.Parallel()
		_, err := fx.svc.Paginate(ctxT(t), pagination.Pagination{Filter: filterWithSort(and(), asc("nope"))})
		if !errors.Is(err, pagination.ErrUnknownField) {
			t.Fatalf("err = %v, want ErrUnknownField", err)
		}
	})
}

// Documents current behavior: user-supplied filters on an unknown field are
// dropped silently by normalizeFilters instead of rejected, so a typo in a
// field name widens the result to every row. If this is ever made strict,
// flip the expectation to ErrUnknownField.
func TestSad_PaginateDropsUnknownFieldSilently(t *testing.T) {
	fx := need(t)
	res, err := fx.svc.Paginate(ctxT(t), pagination.Pagination{Filter: and(f("nmae", pagination.ModeEqual, "Alice"))})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if len(res.Data) != 10 {
		t.Fatalf("got %d rows, want all 10 (filter dropped)", len(res.Data))
	}
}

// Documents current behavior: Count swallows every error and reports 0, so
// "no matches" and "the query failed" look the same.
func TestSad_CountSwallowsErrors(t *testing.T) {
	fx := need(t)
	n, err := fx.svc.Count(ctxT(t), and(f("nope", pagination.ModeEqual, 1)))
	if err != nil || n != 0 {
		t.Fatalf("Count = %d, %v; want 0, nil (current behavior)", n, err)
	}
	if _, err := fx.svc.Exists(ctxT(t), and(f("nope", pagination.ModeEqual, 1))); !errors.Is(err, pagination.ErrUnknownField) {
		t.Fatalf("Exists err = %v, want ErrUnknownField", err)
	}
}

// Values of the wrong type reach Postgres, which refuses them: an error, not
// a panic and not a silent empty page.
func TestSad_TypeMismatch(t *testing.T) {
	fx := need(t)
	cases := []struct {
		name string
		f    pagination.Filter
	}{
		{"text for a number", f("balance", pagination.ModeEqual, "abc")},
		{"text for a bool", f("active", pagination.ModeEqual, "maybe")},
		{"bool for a number", f("balance", pagination.ModeGT, true)},
		{"text in a numeric list", f("balance", pagination.ModeInside, []any{1, "x"})},
		{"text for a number range", f("balance", pagination.ModeRange, map[string]any{"from": "a", "to": "b"})},
		{"text for a timestamp", f("created_at", pagination.ModeEqual, "not a time")},
		{"object for a number", f("balance", pagination.ModeEqual, map[string]any{"a": 1})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mustNotPanic(t, tc.name, func() {
				if rows, err := fx.svc.Filter(ctxT(t), and(tc.f)); err == nil {
					t.Fatalf("no error, %d rows", len(rows))
				}
			})
		})
	}
}

// A text operator on a non-text column is refused by the database.
func TestSad_OperatorMismatch(t *testing.T) {
	fx := need(t)
	for _, tc := range []struct {
		name string
		f    pagination.Filter
	}{
		{"contains on a number", f("balance", pagination.ModeContains, "1")},
		{"startsWith on a number", f("balance", pagination.ModeStartsWith, "1")},
		{"endsWith on a bool", f("active", pagination.ModeEndsWith, "t")},
		{"contains on a timestamp", f("created_at", pagination.ModeContains, "2024")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mustNotPanic(t, tc.name, func() {
				if rows, err := fx.svc.Filter(ctxT(t), and(tc.f)); err == nil {
					t.Fatalf("no error, %d rows", len(rows))
				}
			})
		})
	}
}

// Malformed filter / sort query parameters from the HTTP layer.
func TestSad_MalformedQueryParams(t *testing.T) {
	enc := func(s string) string { return url.QueryEscape(base64.StdEncoding.EncodeToString([]byte(s))) }
	parse := func(rawQuery string) (pagination.Pagination, error) {
		c := app.NewContext(0)
		c.Request.SetRequestURI("/accounts?" + rawQuery)
		var p pagination.Pagination
		err := p.Parse(c)
		return p, err
	}
	for _, tc := range []struct {
		name  string
		query string
	}{
		{"filter is not base64", "filter=%%%not-base64"},
		{"filter is base64 of non-JSON", "filter=" + enc("{not json")},
		{"filter is JSON of the wrong shape", "filter=" + enc(`[1,2,3]`)},
		{"filters is not a list", "filter=" + enc(`{"filters":"x"}`)},
		{"filter mode is a number", "filter=" + enc(`{"filters":[{"field":"a","mode":5}]}`)},
		{"sort is not base64", "sort=@@@"},
		{"sort is the wrong shape", "sort=" + enc(`{"field":"name"}`)},
		{"page size is not a number", "pageSize=ten"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mustNotPanic(t, tc.name, func() {
				if _, err := parse(tc.query); err == nil {
					t.Fatal("expected an error")
				}
			})
		})
	}
	t.Run("a good request parses", func(t *testing.T) {
		t.Parallel()
		p, err := parse("pageSize=7&filter=" + enc(`{"filters":[{"field":"tier","mode":"equal","value":"gold"}]}`) +
			"&sort=" + enc(`[{"field":"balance","order":" DESC "},{"field":"name","order":"sideways"}]`))
		if err != nil {
			t.Fatal(err)
		}
		if p.PageSize != 7 || p.Filter.Logic != pagination.LogicAnd || len(p.Filter.Filters) != 1 {
			t.Fatalf("parsed %+v", p)
		}
		if got := p.Filter.SortFields; len(got) != 2 || got[0].Order != pagination.SortOrderDesc || got[1].Order != pagination.SortOrderAsc {
			t.Fatalf("sort = %+v, want desc then (invalid -> asc)", got)
		}
	})
	t.Run("no params means no filter", func(t *testing.T) {
		t.Parallel()
		p, err := parse("")
		if err != nil || len(p.Filter.Filters) != 0 || p.Filter.Logic != pagination.LogicAnd {
			t.Fatalf("parsed %+v, %v", p, err)
		}
	})
}

// A cancelled context is an error, not an empty page.
func TestSad_CancelledContext(t *testing.T) {
	fx := need(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := fx.svc.Filter(ctx, and()); err == nil {
		t.Fatal("expected an error from a cancelled context")
	}
	if _, err := fx.svc.Exists(ctx, and()); err == nil {
		t.Fatal("Exists: expected an error from a cancelled context")
	}
}
