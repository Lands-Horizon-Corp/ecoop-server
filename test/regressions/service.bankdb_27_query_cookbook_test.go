package regressions

import (
	"fmt"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/pagination"
	"github.com/uptrace/bun"
)

// 27 Query cookbook: every query pattern documented in the ecoop-database skill
// (.agents/skills/ecoop-database), executed against real Postgres so the documentation stays true.

func cookbookBank(t *testing.T) *bdLedger {
	t.Helper()
	b := newBDBank(t, bdOpts{})
	ctx := withDeadline(t, 10*time.Second)
	rows := []bdAccount{
		{ID: "a1", Owner: "Ann Cruz", Balance: 5_000, Profile: map[string]any{"tier": "gold", "tags": []any{"vip"}}},
		{ID: "a2", Owner: "ben cruz", Balance: 200, Profile: map[string]any{"tier": "silver"}},
		{ID: "a3", Owner: "Cy Santos", Currency: "USD", Balance: 9_000, Profile: map[string]any{"tier": "gold"}},
		{ID: "a4", Owner: "Di Reyes", Balance: 50},
	}
	for _, a := range rows {
		must(t, second(b.accounts.Create(ctx, a)))
	}
	must(t, second3(b.Transfer(ctx, transferReq{Key: "k1", From: "a1", To: "a2", Amount: 100})))
	b.replicate()
	return b
}

func idsOf(t *testing.T, rows []*bdAccount, err error) string {
	t.Helper()
	must(t, err)
	return fmt.Sprint(accountIDs(rows))
}

var byIDAsc = []pagination.SortField{{Field: "id", Order: pagination.SortOrderAsc}}

func TestBankDBCookbook_BuiltInFilters(t *testing.T) {
	b := cookbookBank(t)
	ctx := withDeadline(t, 10*time.Second)
	for name, c := range map[string]struct {
		f    pagination.StructuredFilter
		want string
	}{
		"AND of two filters": {pagination.StructuredFilter{SortFields: byIDAsc, Filters: []pagination.Filter{
			{Field: "currency", Mode: pagination.ModeEqual, Value: "PHP"},
			{Field: "balance", Mode: pagination.ModeGTE, Value: 1000},
		}}, "[a1]"},
		"OR of two filters": {pagination.StructuredFilter{SortFields: byIDAsc, Logic: pagination.LogicOr, Filters: []pagination.Filter{
			{Field: "currency", Mode: pagination.ModeEqual, Value: "USD"},
			{Field: "balance", Mode: pagination.ModeLT, Value: 100},
		}}, "[a3 a4]"},
		"IN a list": {pagination.StructuredFilter{SortFields: byIDAsc, Filters: []pagination.Filter{
			{Field: "id", Mode: pagination.ModeInside, Value: []string{"a2", "a4"}},
		}}, "[a2 a4]"},
		"number range": {pagination.StructuredFilter{SortFields: byIDAsc, Filters: []pagination.Filter{
			{Field: "balance", Mode: pagination.ModeRange, Value: pagination.RangeNumber{From: 100, To: 5000}},
		}}, "[a1 a2]"},
		"contains is case-sensitive": {pagination.StructuredFilter{SortFields: byIDAsc, Filters: []pagination.Filter{
			{Field: "owner", Mode: pagination.ModeContains, Value: "Cruz"},
		}}, "[a1]"},
		"JSONB column is null": {pagination.StructuredFilter{SortFields: byIDAsc, Filters: []pagination.Filter{
			{Mode: pagination.ModeCustom, Custom: func(q *bun.SelectQuery, _ any) (*bun.SelectQuery, error) {
				return q.Where("?TableAlias.profile IS NULL"), nil
			}},
		}}, "[a4]"},
		"date after, with a time zone": {pagination.StructuredFilter{SortFields: byIDAsc, Filters: []pagination.Filter{
			{Field: "updated_at", Mode: pagination.ModeAfter, DataType: pagination.DataTypeDate, Value: "2000-01-01T08:00:00+08:00"},
		}}, "[a1 a2 a3 a4]"},
	} {
		got, err := b.accounts.Find(ctx, c.f)
		if s := idsOf(t, got, err); s != c.want {
			t.Errorf("%s = %s; want %s", name, s, c.want)
		}
	}
}

func TestBankDBCookbook_CustomFilters(t *testing.T) {
	b := cookbookBank(t)
	ctx := withDeadline(t, 10*time.Second)
	custom := func(fn func(q *bun.SelectQuery, v any) (*bun.SelectQuery, error), v any) pagination.StructuredFilter {
		return pagination.StructuredFilter{SortFields: byIDAsc, Filters: []pagination.Filter{{Mode: pagination.ModeCustom, Custom: fn, Value: v}}}
	}
	for name, c := range map[string]struct {
		f    pagination.StructuredFilter
		want string
	}{
		"case-insensitive contains (ILIKE)": {custom(func(q *bun.SelectQuery, v any) (*bun.SelectQuery, error) {
			return q.Where("?TableAlias.owner ILIKE ?", "%"+v.(string)+"%"), nil
		}, "cruz"), "[a1 a2]"},
		"OR of AND groups": {custom(func(q *bun.SelectQuery, _ any) (*bun.SelectQuery, error) {
			return q.WhereGroup(" AND ", func(q *bun.SelectQuery) *bun.SelectQuery {
				return q.
					WhereGroup(" OR ", func(q *bun.SelectQuery) *bun.SelectQuery {
						return q.Where("?TableAlias.currency = ?", "PHP").Where("?TableAlias.balance >= ?", 1000)
					}).
					WhereGroup(" OR ", func(q *bun.SelectQuery) *bun.SelectQuery {
						return q.Where("?TableAlias.currency = ?", "USD").Where("?TableAlias.balance >= ?", 8000)
					})
			}), nil
		}, nil), "[a1 a3]"},
		"JSONB key equals": {custom(func(q *bun.SelectQuery, v any) (*bun.SelectQuery, error) {
			return q.Where("?TableAlias.profile->>'tier' = ?", v), nil
		}, "gold"), "[a1 a3]"},
		"JSONB containment": {custom(func(q *bun.SelectQuery, _ any) (*bun.SelectQuery, error) {
			return q.Where("?TableAlias.profile @> ?::jsonb", `{"tags":["vip"]}`), nil
		}, nil), "[a1]"},
		"EXISTS subquery on another table": {custom(func(q *bun.SelectQuery, _ any) (*bun.SelectQuery, error) {
			return q.Where("EXISTS (SELECT 1 FROM bank_audit au WHERE au.account_id = ?TableAlias.id AND au.delta < 0)"), nil
		}, nil), "[a1]"},
		"aggregate in a subquery": {custom(func(q *bun.SelectQuery, v any) (*bun.SelectQuery, error) {
			return q.Where("?TableAlias.balance > (SELECT avg(balance) FROM bank_accounts)"), nil
		}, nil), "[a1 a3]"},
	} {
		got, err := b.accounts.Find(ctx, c.f)
		if s := idsOf(t, got, err); s != c.want {
			t.Errorf("%s = %s; want %s", name, s, c.want)
		}
	}
}

func TestBankDBCookbook_ScopeAndClientFilterPaginate(t *testing.T) {
	b := cookbookBank(t)
	ctx := withDeadline(t, 10*time.Second)
	scope := eqFilter("currency", "PHP")                                            // set by code: always applied, unknown fields rejected
	page := pagination.Pagination{PageSize: 2, Filter: pagination.StructuredFilter{ // from the client
		Filters:    []pagination.Filter{{Field: "balance", Mode: pagination.ModeGT, Value: 0}},
		SortFields: []pagination.SortField{{Field: "balance", Order: pagination.SortOrderDesc}},
	}}
	var all []string
	for {
		res, err := b.accounts.PaginateFilter(ctx, scope, page)
		must(t, err)
		all = append(all, accountIDs(res.Data)...)
		if res.NextCursor == nil {
			break
		}
		page.Cursor = res.NextCursor // same filter and sort, next page
	}
	if fmt.Sprint(all) != "[a1 a2 a4]" {
		t.Fatalf("scoped pages = %v; want [a1 a2 a4] (PHP only, balance desc)", all)
	}
}
