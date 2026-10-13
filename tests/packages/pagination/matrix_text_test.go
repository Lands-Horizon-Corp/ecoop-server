//go:build integration

package pagination_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pagination"
)

// text reads a string column of a seeded account; ok is false for NULL.
type textCol struct {
	field string
	get   func(Account) (string, bool)
}

var (
	tName  = textCol{"name", func(a Account) (string, bool) { return a.Name, true }}
	tTier  = textCol{"tier", func(a Account) (string, bool) { return a.Tier, true }}
	tEmail = textCol{"email", func(a Account) (string, bool) {
		if a.Email == nil {
			return "", false
		}
		return *a.Email, true
	}}
)

// checkAccounts runs the filter through Filter, Paginate, Count, Exists and
// a cursor walk, and compares each with the oracle over the seed.
func checkAccounts(t *testing.T, flt pagination.Filter, match func(Account) bool) {
	t.Helper()
	fx := need(t)
	checkAccountFilter(t, fx, and(flt), match)
}

func checkAccountFilter(t *testing.T, fx *fixture, sf pagination.StructuredFilter, match func(Account) bool) {
	t.Helper()
	var want []string
	for _, a := range seed() {
		if match(a) {
			want = append(want, a.Name)
		}
	}
	ctx := ctxT(t)
	got, err := fx.svc.Filter(ctx, sf)
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	wantSet(t, nameList(got), want...)
	res, err := fx.svc.Paginate(ctx, pagination.Pagination{Filter: sf, PageSize: 50})
	if err != nil {
		t.Fatalf("Paginate: %v", err)
	}
	wantSet(t, nameList(res.Data), want...)
	if n, err := fx.svc.Count(ctx, sf); err != nil || int(n) != len(want) {
		t.Fatalf("Count = %d, %v; want %d", n, err, len(want))
	}
	if ok, err := fx.svc.Exists(ctx, sf); err != nil || ok != (len(want) > 0) {
		t.Fatalf("Exists = %v, %v; want %v", ok, err, len(want) > 0)
	}
	walkSF := sf
	walkSF.SortFields = []pagination.SortField{asc("balance")}
	walked := walk(t, fx, walkSF, 3)
	if !sameSet(walked, want) || firstDuplicate(walked) != "" {
		t.Fatalf("cursor walk visited %v, want %v once each", walked, want)
	}
}

type likeCase struct {
	col textCol
	val string
}

// Ten values per LIKE mode, across three columns, including wildcards,
// quotes, empty text, accents, NULL-bearing columns and a value that matches
// nothing. LIKE is case-sensitive and NULL never matches, so the oracle is
// strings.Contains / HasPrefix / HasSuffix on non-NULL values only.
func TestMatrix_TextLike(t *testing.T) {
	cases := map[pagination.Mode][]likeCase{
		pagination.ModeContains: {
			{tName, "a"}, {tName, "ra"}, {tName, "Al"}, {tName, "O'"}, {tName, "mile"},
			{tName, "%"}, {tName, "_"}, {tName, "100"}, {tName, ""}, {tName, "zzz"},
			{tEmail, "@x.com"}, {tEmail, "alice"}, {tEmail, "x."}, {tTier, "ol"}, {tTier, "GOLD"},
		},
		pagination.ModeNotContains: {
			{tName, "a"}, {tName, "e"}, {tName, "Al"}, {tName, "O'"}, {tName, "%"},
			{tName, "_"}, {tName, ""}, {tName, "zzz"}, {tName, "i"},
			{tEmail, "@x.com"}, {tEmail, "alice"}, {tEmail, "@"}, {tTier, "ol"}, {tTier, "er"},
		},
		pagination.ModeStartsWith: {
			{tName, "A"}, {tName, "Al"}, {tName, "Bo"}, {tName, "Dave_"}, {tName, "O'B"},
			{tName, "É"}, {tName, "F"}, {tName, "Gr"}, {tName, "_"}, {tName, "zz"}, {tName, ""},
			{tEmail, "a"}, {tEmail, "emile"}, {tEmail, "@"}, {tTier, "s"}, {tTier, "g"},
		},
		pagination.ModeEndsWith: {
			{tName, "e"}, {tName, "ce"}, {tName, "n"}, {tName, "%"}, {tName, "100%"},
			{tName, "k"}, {tName, "di"}, {tName, "'Brien"}, {tName, "_"}, {tName, ""},
			{tEmail, ".com"}, {tEmail, "x.com"}, {tEmail, "e@x.com"}, {tTier, "r"}, {tTier, "ze"},
		},
	}
	holdsLike := func(mode pagination.Mode, s, v string) bool {
		switch mode {
		case pagination.ModeContains:
			return strings.Contains(s, v)
		case pagination.ModeNotContains:
			return !strings.Contains(s, v)
		case pagination.ModeStartsWith:
			return strings.HasPrefix(s, v)
		default:
			return strings.HasSuffix(s, v)
		}
	}
	for mode, list := range cases {
		t.Run(string(mode), func(t *testing.T) {
			for _, tc := range list {
				t.Run(fmt.Sprintf("%s/%q", tc.col.field, tc.val), func(t *testing.T) {
					t.Parallel()
					flt := f(tc.col.field, mode, tc.val)
					checkAccounts(t, flt, func(a Account) bool {
						s, ok := tc.col.get(a)
						return ok && holdsLike(mode, s, tc.val)
					})
				})
			}
		})
	}
}

type emptyCol struct {
	field string
	empty func(Account) bool
}

// Nine columns plus a stray-value case per emptiness mode. "Empty" means NULL or the empty text; a
// number, bool or timestamp is never the empty text, so only NULL counts.
func TestMatrix_IsEmpty(t *testing.T) {
	cols := []emptyCol{
		{"email", func(a Account) bool { return a.Email == nil || *a.Email == "" }},
		{"score", func(a Account) bool { return a.Score == nil }},
		{"balance", func(Account) bool { return false }},
		{"name", func(Account) bool { return false }},
		{"tier", func(Account) bool { return false }},
		{"active", func(Account) bool { return false }},
		{"created_at", func(Account) bool { return false }},
		{"updated_at", func(Account) bool { return false }},
		{"id", func(Account) bool { return false }},
	}
	for _, mode := range []pagination.Mode{pagination.ModeIsEmpty, pagination.ModeIsNotEmpty} {
		t.Run(string(mode), func(t *testing.T) {
			for _, c := range cols {
				t.Run(c.field, func(t *testing.T) {
					t.Parallel()
					fx := need(t)
					flt := f(c.field, mode, nil)
					want := func(a Account) bool { return c.empty(a) == (mode == pagination.ModeIsEmpty) }
					// Through the strict path (Filter) and the normalizing one (Paginate).
					checkAccountFilter(t, fx, and(flt), want)
				})
			}
		})
	}
	t.Run("a stray value is ignored", func(t *testing.T) {
		t.Parallel()
		checkAccounts(t, f("email", pagination.ModeIsEmpty, "ignored"), func(a Account) bool { return a.Email == nil || *a.Email == "" })
	})
}

// Ten ModeCustom filters: the function owns the SQL, receives the value
// unchanged, and composes with built-in filters and OR logic.
func TestMatrix_Custom(t *testing.T) {
	custom := func(value any, fn func(q *bun.SelectQuery, v any) *bun.SelectQuery) pagination.Filter {
		return pagination.Filter{Mode: pagination.ModeCustom, Value: value, Custom: func(q *bun.SelectQuery, v any) (*bun.SelectQuery, error) {
			return fn(q, v), nil
		}}
	}
	cases := []struct {
		name  string
		sf    pagination.StructuredFilter
		match func(Account) bool
	}{
		{"column comparison", and(custom(100, func(q *bun.SelectQuery, v any) *bun.SelectQuery { return q.Where("balance > ?", v) })),
			func(a Account) bool { return a.Balance > 100 }},
		{"ILIKE", and(custom("AL", func(q *bun.SelectQuery, v any) *bun.SelectQuery { return q.Where("name ILIKE ?", "%"+v.(string)+"%") })),
			func(a Account) bool { return strings.Contains(strings.ToLower(a.Name), "al") }},
		{"expression over two columns", and(custom(nil, func(q *bun.SelectQuery, _ any) *bun.SelectQuery { return q.Where("balance > score") })),
			func(a Account) bool { return a.Score != nil && a.Balance > float64(*a.Score) }},
		{"IS NULL", and(custom(nil, func(q *bun.SelectQuery, _ any) *bun.SelectQuery { return q.Where("score IS NULL") })),
			func(a Account) bool { return a.Score == nil }},
		{"IN subquery", and(custom("gold", func(q *bun.SelectQuery, v any) *bun.SelectQuery {
			return q.Where("tier IN (SELECT tier FROM pg_accounts GROUP BY tier HAVING count(*) = ? )", 4)
		})), func(a Account) bool { return a.Tier == "gold" }},
		{"EXISTS subquery", and(custom(nil, func(q *bun.SelectQuery, _ any) *bun.SelectQuery {
			return q.Where("EXISTS (SELECT 1 FROM pg_accounts o WHERE o.tier = account.tier AND o.balance > account.balance)")
		})), func(a Account) bool {
			for _, o := range seed() {
				if o.Tier == a.Tier && o.Balance > a.Balance {
					return true
				}
			}
			return false
		}},
		{"custom AND built-in", and(f("tier", pagination.ModeEqual, "gold"), custom(0, func(q *bun.SelectQuery, v any) *bun.SelectQuery { return q.Where("balance > ?", v) })),
			func(a Account) bool { return a.Tier == "gold" && a.Balance > 0 }},
		{"custom OR built-in", or(f("name", pagination.ModeEqual, "Bob"), custom(nil, func(q *bun.SelectQuery, _ any) *bun.SelectQuery { return q.Where("score IS NULL") })),
			func(a Account) bool { return a.Name == "Bob" || a.Score == nil }},
		{"two customs", and(
			custom(0, func(q *bun.SelectQuery, v any) *bun.SelectQuery { return q.Where("balance >= ?", v) }),
			custom(nil, func(q *bun.SelectQuery, _ any) *bun.SelectQuery { return q.Where("active") })),
			func(a Account) bool { return a.Balance >= 0 && a.Active }},
		{"matches nothing", and(custom(nil, func(q *bun.SelectQuery, _ any) *bun.SelectQuery { return q.Where("1 = 0") })),
			func(Account) bool { return false }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fx := need(t)
			checkAccountFilter(t, fx, tc.sf, tc.match)
		})
	}
}

// ModeSearch needs ParadeDB's @@@ operator. Validation happens before the
// database, so these hold on any Postgres.
func TestMatrix_SearchValidation(t *testing.T) {
	fx := need(t)
	t.Run("nil value is refused", func(t *testing.T) {
		t.Parallel()
		if _, err := fx.svc.Filter(ctxT(t), and(f("name", pagination.ModeSearch, nil))); !errors.Is(err, pagination.ErrInvalidFilter) {
			t.Fatalf("err = %v, want ErrInvalidFilter", err)
		}
	})
	t.Run("whole-index search with nil value is refused", func(t *testing.T) {
		t.Parallel()
		if _, err := fx.svc.Filter(ctxT(t), and(f("", pagination.ModeSearch, nil))); !errors.Is(err, pagination.ErrInvalidFilter) {
			t.Fatalf("err = %v, want ErrInvalidFilter", err)
		}
	})
	t.Run("unknown field is refused", func(t *testing.T) {
		t.Parallel()
		if _, err := fx.svc.Filter(ctxT(t), and(f("nope", pagination.ModeSearch, "x"))); !errors.Is(err, pagination.ErrUnknownField) {
			t.Fatalf("err = %v, want ErrUnknownField", err)
		}
	})
	t.Run("without ParadeDB it is a database error, not a panic or a silent empty page", func(t *testing.T) {
		t.Parallel()
		var has bool
		_ = fx.sql.Client().NewRaw("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_search')").Scan(ctxT(t), &has)
		if has {
			t.Skip("ParadeDB is installed")
		}
		mustNotPanic(t, "search", func() {
			if _, err := fx.svc.Filter(ctxT(t), and(f("name", pagination.ModeSearch, "alice"))); err == nil {
				t.Error("no error without the @@@ operator")
			}
		})
	})
}
