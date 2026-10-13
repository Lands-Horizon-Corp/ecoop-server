//go:build integration

package pagination_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pagination"
)

var injections = []string{
	`'; DROP TABLE pg_accounts; --`,
	`" OR 1=1 --`,
	`' OR '1'='1`,
	`x'); DELETE FROM pg_accounts; --`,
	`\'; TRUNCATE pg_accounts; --`,
	`$1`, `?`, `??`, `?? ?`, `%s%s%s`, `{{.}}`,
	"Robert'); DROP TABLE pg_accounts;--",
	`$$; DROP TABLE pg_accounts; $$`,
}

func tableIntact(t *testing.T, fx *fixture) {
	t.Helper()
	n, err := fx.sql.Client().NewSelect().Model((*Account)(nil)).Count(ctxT(t))
	if err != nil || n != 10 {
		t.Fatalf("accounts table has %d rows, %v; want 10 untouched", n, err)
	}
}

// Hostile text in a value is data: no row matches and the table survives.
func TestPoison_SQLInjectionInValues(t *testing.T) {
	fx := need(t)
	modes := []pagination.Mode{
		pagination.ModeEqual, pagination.ModeNotEqual, pagination.ModeContains, pagination.ModeNotContains,
		pagination.ModeStartsWith, pagination.ModeEndsWith,
	}
	for _, payload := range injections {
		for _, m := range modes {
			rows, err := fx.svc.Filter(ctxT(t), and(f("name", m, payload)))
			if err != nil {
				t.Fatalf("%s %q: %v", m, payload, err)
			}
			if m != pagination.ModeNotEqual && m != pagination.ModeNotContains && len(rows) != 0 {
				t.Fatalf("%s %q matched %v", m, payload, nameList(rows))
			}
			if (m == pagination.ModeNotEqual || m == pagination.ModeNotContains) && len(rows) != 10 {
				t.Fatalf("%s %q matched %d rows, want all 10", m, payload, len(rows))
			}
		}
		if _, err := fx.svc.Filter(ctxT(t), and(f("name", pagination.ModeInside, []any{payload, "Alice"}))); err != nil {
			t.Fatalf("inside %q: %v", payload, err)
		}
	}
	tableIntact(t, fx)
}

// Hostile text as a field or sort name never reaches the SQL.
func TestPoison_SQLInjectionInIdentifiers(t *testing.T) {
	fx := need(t)
	for _, payload := range injections {
		if _, err := fx.svc.Filter(ctxT(t), and(f(payload, pagination.ModeEqual, "x"))); !errors.Is(err, pagination.ErrUnknownField) {
			t.Fatalf("field %q: err = %v, want ErrUnknownField", payload, err)
		}
		_, err := fx.svc.Paginate(ctxT(t), pagination.Pagination{Filter: filterWithSort(and(), asc(payload))})
		if !errors.Is(err, pagination.ErrUnknownField) {
			t.Fatalf("sort %q: err = %v, want ErrUnknownField", payload, err)
		}
		// Through Paginate a bad field is dropped; the rest of the query still runs.
		if _, err := fx.svc.Paginate(ctxT(t), pagination.Pagination{Filter: and(f(payload, pagination.ModeEqual, "x"))}); err != nil {
			t.Fatalf("paginate field %q: %v", payload, err)
		}
	}
	// An identifier that looks valid but smuggles SQL after a real column name.
	for _, field := range []string{`name"; DROP TABLE pg_accounts; --`, "name--", "name OR 1=1", "name;", "id desc, (select 1)"} {
		if _, err := fx.svc.Filter(ctxT(t), and(f(field, pagination.ModeEqual, "x"))); !errors.Is(err, pagination.ErrUnknownField) {
			t.Fatalf("field %q: err = %v, want ErrUnknownField", field, err)
		}
	}
	tableIntact(t, fx)
}

// Text Postgres cannot store is refused up front, before it can truncate the
// statement.
func TestPoison_UnstorableText(t *testing.T) {
	fx := need(t)
	for _, tc := range []struct {
		name, v string
	}{
		{"NUL byte", "a\x00b"},
		{"only a NUL", "\x00"},
		{"invalid UTF-8", "\xff\xfe"},
		{"truncated UTF-8", "\xe2\x82"},
		{"overlong UTF-8", "\xc0\xaf"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, m := range []pagination.Mode{pagination.ModeEqual, pagination.ModeContains} {
				if _, err := fx.svc.Filter(ctxT(t), and(f("name", m, tc.v))); !errors.Is(err, pagination.ErrInvalidFilter) {
					t.Fatalf("%s: err = %v, want ErrInvalidFilter", m, err)
				}
			}
			if _, err := fx.svc.Filter(ctxT(t), and(f("name", pagination.ModeInside, []any{"ok", tc.v}))); !errors.Is(err, pagination.ErrInvalidFilter) {
				t.Fatalf("inside: err = %v, want ErrInvalidFilter", err)
			}
		})
	}
}

// Odd but storable text is accepted and matches nothing instead of crashing.
func TestPoison_UnusualText(t *testing.T) {
	fx := need(t)
	for _, v := range []string{
		"‮evil", "�", "​", "😀😀😀", strings.Repeat("é", 5000), "\t\n\r", "́́́",
		"%", "_", `\`, `\\%`, "'", `"`, "`", "--", "/*", "*/", ";",
	} {
		mustNotPanic(t, "value", func() {
			if _, err := fx.svc.Filter(ctxT(t), and(f("name", pagination.ModeEqual, v))); err != nil {
				t.Fatalf("%q: %v", v, err)
			}
		})
	}
}

// Values of types a JSON decoder never produces must not take the process
// down. (func and chan values are left out: bun itself panics on them, and no
// request can carry one.)
func TestPoison_ExoticValueTypes(t *testing.T) {
	fx := need(t)
	var nilPtr *string
	var nilMap map[string]any
	var nilSlice []any
	for name, v := range map[string]any{
		"nil pointer":     nilPtr,
		"nil map":         nilMap,
		"nil slice":       nilSlice,
		"empty slice":     []any{},
		"nested slice":    []any{[]any{1, 2}, []any{3}},
		"nested map":      map[string]any{"a": map[string]any{"b": []any{1}}},
		"pointer to text": ptr("Alice"),
		"struct":          struct{ A int }{1},
		"NaN-ish float":   0.0 / func() float64 { return 1 }(),
	} {
		for _, m := range []pagination.Mode{pagination.ModeEqual, pagination.ModeContains, pagination.ModeInside, pagination.ModeRange, pagination.ModeGT} {
			mustNotPanic(t, name+"/"+string(m), func() {
				_, _ = fx.svc.Filter(ctxT(t), and(f("name", m, v)))
			})
		}
	}
	tableIntact(t, fx)
}

// An empty list matches nothing for inside and everything for outside; it is
// not a syntax error (IN ()).
func TestPoison_EmptyAndHugeLists(t *testing.T) {
	fx := need(t)
	t.Run("empty inside", func(t *testing.T) {
		t.Parallel()
		got, err := fx.svc.Filter(ctxT(t), and(f("tier", pagination.ModeInside, []any{})))
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("matched %v", nameList(got))
		}
	})
	t.Run("empty outside", func(t *testing.T) {
		t.Parallel()
		got, err := fx.svc.Filter(ctxT(t), and(f("tier", pagination.ModeOutside, []any{})))
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if len(got) != 10 {
			t.Fatalf("matched %d rows, want 10", len(got))
		}
	})
	t.Run("a very long list", func(t *testing.T) {
		t.Parallel()
		list := make([]any, 50_000)
		for i := range list {
			list[i] = "tier-" + strings.Repeat("x", i%7)
		}
		list[49_999] = "gold"
		got, err := fx.svc.Filter(ctxT(t), and(f("tier", pagination.ModeInside, list)))
		if err != nil || len(got) != 4 {
			t.Fatalf("matched %d rows, %v; want the 4 gold ones", len(got), err)
		}
	})
	t.Run("a megabyte value", func(t *testing.T) {
		t.Parallel()
		got, err := fx.svc.Filter(ctxT(t), and(f("name", pagination.ModeContains, strings.Repeat("a", 1<<20))))
		if err != nil || len(got) != 0 {
			t.Fatalf("matched %d rows, %v", len(got), err)
		}
	})
	t.Run("many filters", func(t *testing.T) {
		t.Parallel()
		fs := make([]pagination.Filter, 2000)
		for i := range fs {
			fs[i] = f("tier", pagination.ModeNotEqual, "none")
		}
		got, err := fx.svc.Filter(ctxT(t), and(fs...))
		if err != nil || len(got) != 10 {
			t.Fatalf("matched %d rows, %v", len(got), err)
		}
	})
}

// A user filter can never widen the hardcoded scope, however it is phrased:
// that scope is what tenant and ownership rules ride on.
func TestPoison_UserFilterCannotEscapeScope(t *testing.T) {
	fx := need(t)
	scope := and(f("tier", pagination.ModeEqual, "gold"))
	wideOpen := pagination.Filter{Mode: pagination.ModeCustom, Custom: func(q *bun.SelectQuery, _ any) (*bun.SelectQuery, error) {
		return q.WhereOr("1 = 1").WhereOr("tier = 'silver'"), nil
	}}
	for name, user := range map[string]pagination.StructuredFilter{
		"or with another tier":      or(f("tier", pagination.ModeEqual, "silver"), f("name", pagination.ModeEqual, "Bob")),
		"or with a tautology value": or(f("name", pagination.ModeEqual, "' OR 1=1 --"), f("tier", pagination.ModeNotEqual, "gold")),
		"custom WhereOr":            and(wideOpen),
		"custom WhereOr in an or":   or(wideOpen, f("tier", pagination.ModeEqual, "silver")),
	} {
		res, err := fx.svc.PaginateFilter(ctxT(t), scope, pagination.Pagination{Filter: user})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for _, r := range res.Data {
			if r.Tier != "gold" {
				t.Fatalf("%s: leaked %s (tier %s) out of the gold scope", name, r.Name, r.Tier)
			}
		}
	}
}

// JSON can carry null inside a list (["gold", null]). That reaches the query
// builder, which must refuse or tolerate it, not panic the request.
func TestPoison_NullInsideAList(t *testing.T) {
	fx := need(t)
	for _, mode := range []pagination.Mode{pagination.ModeInside, pagination.ModeOutside} {
		mustNotPanic(t, string(mode), func() {
			_, _ = fx.svc.Filter(ctxT(t), and(f("tier", mode, []any{"gold", nil})))
		})
	}
}
