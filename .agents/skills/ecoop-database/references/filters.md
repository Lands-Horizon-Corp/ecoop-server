# Filters, sorting, cursors and search

All reads take a `pagination.StructuredFilter`. The filter and custom-filter patterns below are
executed by `test/regressions/service.bankdb_27_query_cookbook_test.go`; search, partitions, preloads,
time zones and Hertz parsing by `service.bankdb_17_pagination_ext_test.go`. When you add a pattern
here, add it to the cookbook test too.

## Contents

- Where a filter comes from (scope vs client)
- Built-in modes
- Values, data types and text
- Combining filters (AND / OR)
- Custom filters: the escape hatch for complex SQL
- Sorting
- Cursor pagination
- HTTP endpoints (Hertz query parameters)
- Preloads (relations)
- Full-text search (pg_search / BM25)
- Partitioned tables
- Performance checklist

## Where a filter comes from

| | Scope (set by code) | Client filter |
|---|---|---|
| Passed as | the `filter` argument of `Find`, `FindOne`, `Count`, `Exists`, `Max`/`Min`, the `scope` of `PaginateFilter` / `PaginateWithHertz` | `Pagination.Filter` (`Paginate`, `PaginateFilter`, query parameters) |
| Unknown field | `ErrUnknownField` (fails loudly) | dropped with a warning log |
| Field names | exact bun column names (`updated_at`) | normalized: `updatedAt` → `updated_at` |
| `ModeCustom` | allowed | impossible (`Custom` is `json:"-"`) |

Both are applied, combined with AND. Put every restriction that must hold (owner, branch, status,
"not deleted") in the scope, never in the client filter.

## Built-in modes

| Mode | SQL | Value |
|---|---|---|
| `ModeEqual` / `ModeNotEqual` | `col = v` / `col != v` | scalar |
| `ModeGT` `ModeGTE` `ModeLT` `ModeLTE` | `> >= < <=` | number, string, time |
| `ModeBefore` / `ModeAfter` | `col < v` / `col > v` | time, or a string with `DataTypeDate` |
| `ModeContains` / `ModeNotContains` | `col LIKE '%v%'` | string; `%` `_` `\` are literal; **case-sensitive** |
| `ModeStartsWith` / `ModeEndsWith` | `LIKE 'v%'` / `LIKE '%v'` | string |
| `ModeInside` / `ModeOutside` | `col IN (…)` / `NOT IN (…)` | a slice (`[]string`, `[]int64`, `[]any`) |
| `ModeRange` | `col BETWEEN from AND to` (inclusive) | `pagination.RangeNumber{From, To}`, `pagination.RangeDate{From, To}`, or `map[string]any{"from": …, "to": …}` |
| `ModeIsEmpty` / `ModeIsNotEmpty` | `(col IS NULL OR col = '')` | none — **text columns only** (on numbers/JSONB use a custom `IS NULL`) |
| `ModeSearch` | `col @@@ v` (BM25) | string; needs a pg_search index |
| `ModeCustom` | whatever the function adds | anything; see below |

A nil `Value` is `ErrInvalidFilter` for every mode except `isEmpty`, `isNotEmpty` and `custom`.

## Values, data types and text

- `DataType: pagination.DataTypeDate` (or `DataTypeTime`) makes string values parse as timestamps
  (RFC 3339 with an offset is the safe format: `"2026-01-01T00:00:00+08:00"`). Comparison modes and
  `ModeRange` use it. Other data types are informational.
- Text values are validated and NFC-normalized exactly like stored text: a NUL byte or invalid UTF-8
  is `ErrInvalidFilter` (mapped to `ErrInvalidEncoding`); `Müller` finds `Müller`.
- Prefer typed Go values (`int64`, `time.Time`) in scopes; strings are fine from clients.

## Combining filters

```go
pagination.StructuredFilter{Logic: pagination.LogicOr, Filters: []pagination.Filter{
	{Field: "currency", Mode: pagination.ModeEqual, Value: "USD"},
	{Field: "balance", Mode: pagination.ModeLT, Value: 100},
}} // currency = 'USD' OR balance < 100
```

`Logic` applies to the whole list (`LogicAnd` is the default). There are no nested groups in the
structure; for `(A AND B) OR (C AND D)` use a custom filter.

## Custom filters: the escape hatch for complex SQL

```go
type CustomFilter func(q *bun.SelectQuery, value any) (*bun.SelectQuery, error)
```

The function receives the query inside its own `WHERE (…)` group and returns it with conditions
added. Rules:

- Refer to the model's table as `?TableAlias` (bun substitutes the alias).
- Pass every value as a `?` argument. Never `fmt.Sprintf` user input into SQL.
- Return an error (wrapped `pagination.ErrInvalidFilter`) for a bad `value` instead of panicking.
- Keep it sargable: compare indexed columns, not expressions over them, where you can.

### Case-insensitive contains

```go
{Mode: pagination.ModeCustom, Value: "cruz", Custom: func(q *bun.SelectQuery, v any) (*bun.SelectQuery, error) {
	s, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("%w: want a string", pagination.ErrInvalidFilter)
	}
	return q.Where("?TableAlias.owner ILIKE ?", "%"+s+"%"), nil
}}
```

### OR of AND groups

```go
// (currency = 'PHP' AND balance >= 1000) OR (currency = 'USD' AND balance >= 8000)
Custom: func(q *bun.SelectQuery, _ any) (*bun.SelectQuery, error) {
	return q.WhereGroup(" AND ", func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.
			WhereGroup(" OR ", func(q *bun.SelectQuery) *bun.SelectQuery {
				return q.Where("?TableAlias.currency = ?", "PHP").Where("?TableAlias.balance >= ?", 1000)
			}).
			WhereGroup(" OR ", func(q *bun.SelectQuery) *bun.SelectQuery {
				return q.Where("?TableAlias.currency = ?", "USD").Where("?TableAlias.balance >= ?", 8000)
			})
	}), nil
}
```

### JSONB

```go
q.Where("?TableAlias.profile->>'tier' = ?", "gold")                // one key
q.Where("?TableAlias.profile @> ?::jsonb", `{"tags":["vip"]}`)     // containment (GIN-indexable)
q.Where("?TableAlias.profile IS NULL")                             // no document
```

### Conditions on another table (EXISTS / NOT EXISTS)

```go
// accounts with at least one debit in the audit trail
q.Where("EXISTS (SELECT 1 FROM bank_audit au WHERE au.account_id = ?TableAlias.id AND au.delta < 0)")
```

Prefer `EXISTS` over joins: it cannot duplicate rows, so cursors and counts stay correct.

### Aggregates and subqueries

```go
q.Where("?TableAlias.balance > (SELECT avg(balance) FROM bank_accounts)")
```

Under row-level security a subquery only sees the current tenant's rows too.

## Sorting

```go
SortFields: []pagination.SortField{{Field: "balance", Order: pagination.SortOrderDesc}}
```

- Default: the model's `ColumnDefaultSort` (`"updated_at DESC"` unless registered otherwise).
- The ID column is appended as a tie-breaker automatically, so ordering is always total.
- `NULLS LAST` in both directions; nullable (pointer) sort fields are supported.
- Unknown sort field → `ErrUnknownField`; an invalid order means ascending.

## Cursor pagination

```go
page := pagination.Pagination{PageSize: 50, Filter: clientFilter}
for {
	res, err := accounts.PaginateFilter(ctx, scope, page) // res.Data, res.NextCursor, res.PreviousCursor
	if err != nil { return err }
	use(res.Data)
	if res.NextCursor == nil { break }
	page.Cursor = res.NextCursor
}
```

- Keyset pagination: no `OFFSET`, stable under concurrent inserts and deletes, a page deep in a
  million rows costs the same as page one (given an index on the sort columns + id).
- Pass a cursor back with the **same filter and sort**; a cursor that does not match is
  `ErrInvalidCursor`. `PreviousCursor` walks backwards.
- `PageSize` 0 means 30. `Find`/`Filter` are a single page of 30 — do not use them for "all rows".
- Cursors are opaque base64 JSON, not signed: they only position the walk, the scope still applies.

## HTTP endpoints (Hertz)

```go
res, err := accounts.PaginateWithHertzFormat(ctx, nil, scope, reqCtx) // nil tx: reads the reader
```

Query parameters: `pageSize`, `cursor`, `filter` and `sort`, where `filter` is
`utils.EncodeQueryParam(StructuredFilter)` and `sort` is `utils.EncodeQueryParam([]SortField)`
(URL-escaped base64 of the JSON). JSON shape:

```json
{"logic":"and","filters":[{"field":"balance","mode":"gte","value":100,"dataType":"number"}],
 "sortFields":[{"field":"balance","order":"desc"}]}
```

A malformed parameter is an error (return 400); clients can never send a custom filter.

## Preloads (relations)

`Find(ctx, filter, "From", "To")` loads bun relations declared on the model
(`bun:"rel:belongs-to,join:from_account=id"`). Unknown names are dropped with a warning.
`Registration.Preloads` sets the default list.

## Full-text search (pg_search / BM25)

Needs a BM25 index on the **reader** (create it in a migration):

```sql
CREATE INDEX accounts_search_idx ON accounts USING bm25 (id, owner, notes) WITH (key_field = 'id');
```

- One column: `{Field: "owner", Mode: pagination.ModeSearch, Value: "cruz"}`.
- Whole index with ParadeDB query syntax: `{Mode: pagination.ModeSearch, Value: "owner:cruz AND notes:loan"}`.
- `(*pagination.PaginationService).EnableSearchIndex(ctx, cols...)` creates the index ad hoc.

## Partitioned tables

`(*pagination.PaginationService).EnablePartitioning(ctx, "occurred_at", "1 day")` creates the table
range-partitioned and registers it with pg_partman. The control column must be part of the primary
key (`bun:"occurred_at,pk"`). Queries are unchanged; filter on the control column to prune partitions.

## Performance checklist

- Index `(sort columns…, id)` for every list you paginate; check with `EXPLAIN` that a deep page is an
  index scan without a `Sort` node (`TestBankDBPaginationExt_KeysetPagingOverAMillionRowsUsesTheIndex`).
- `contains` is `LIKE '%…%'`: fine for small tables; use `ModeSearch` or a trigram index for large ones.
- Run `Count` only when the UI needs a total; cursors do not.
- `Exists` is cheaper than `Count > 0`.
