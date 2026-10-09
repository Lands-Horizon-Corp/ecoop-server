---
name: ecoop-database
description: How to use this repo's database service (pkg/services/database) — defining and registering models, full CRUD (create, read, list, PATCH/PUT, optimistic concurrency, soft delete, bulk) with Hertz handlers and errors mapped to HTTP status, transactions, filtering, sorting and cursor pagination, complex SQL filters (OR-of-AND groups, EXISTS subqueries, JSONB, ILIKE, aggregates) through ModeCustom, BM25 search, tenancy with row-level security, error mapping, and the CQRS writer/reader model. Use whenever writing or reviewing Go code in ecoop-server that reads or writes the database, builds a list/search endpoint, adds a model, or debugs a database error. Not for designing table schemas (see postgresql-table-design) or tuning raw SQL (see sql-optimization).
---

# ecoop database service

`pkg/services/database` wraps three packages: `sql` (connection pools + goose migrations), `cqrs`
(writes, and the runner that copies changes to the read model) and `pagination` (reads, filters,
cursors). Application code talks to one typed service per model: `cqrs.CQRSServices[TData,
TResponse, TRequest, TID]`.

## The model you must keep in mind

- **Two databases.** Writes (`Create`, `Update…`, `Delete…`, `Increment…`, anything `…WithTx`) go
  to the **writer**. Plain reads (`Find`, `GetByID`, `Count`, `Paginate`, …) go to the **reader**.
- **The reader lags.** Changes reach it through CDC (Debezium → Kafka → runner). Right after
  `Create`, `GetByID` may return `ErrNotFound`. Use the value `Create` returned, or read inside a
  transaction (`GetByIDWithTx` reads the writer).
- **Reads inside a transaction lock rows** (`SELECT … FOR UPDATE`) unless the context is
  `database.WithoutRowLocks(ctx)`.
- **`Find` / `Filter` return at most 30 rows** (the default page size). For full listings, page with
  `Paginate` / `PaginateFilter` and follow `NextCursor`.

## Setup (once per process)

```go
db := database.NewDatabaseService(writerDSN, readerDSN, maxIdle, maxOpen,
	readerLog, writerLog, cqrsLog, migrationsDir, autoMigrate, os.Stdout, nil,
	broadcaster, kafka /* broker.BatchBrokerServices → at-least-once */, validator.New(),
	100, 5*time.Second,
	database.WithServerTimeouts(database.ServerTimeouts{Statement: 30 * time.Second, Lock: 5 * time.Second, IdleInTransaction: time.Minute}),
	database.WithConnMaxLifetime(30*time.Minute),
)
// Register every model BEFORE Start.
err := database.Register(db, database.Registration[Account, AccountResponse, AccountRequest, string]{
	Channel:       "cqrs.public.accounts", // Debezium topic for the table
	ColumnVersion: "version",              // keep the newest row under reordering
	ToResource:    toAccountResponse,      // used by the …Format methods and change hooks
	FromRequest:   fromAccountRequest,     // used by the …WithValidation methods
})
err = db.Start(ctx) // opens pools, migrates, checks writer/reader schema versions
db.Run(ctx)         // starts the CDC runners; they live until Stop (safe with fx OnStart)
defer db.Stop(ctx)  // stops runners, drains hooks, closes pools

accounts, err := database.Get[Account, AccountResponse, AccountRequest, string](db) // after Start
```

Call `Get` once at wiring time and keep the handle; it is lock-free but there is no reason to repeat
it per request.

## Core API (per model)

| Need | Call |
|---|---|
| Insert one / many | `Create`, `CreateMany` (validated with `validate` tags) |
| Insert from a request DTO | `CreateWithValidation(ctx, req)` (needs `FromRequest`) |
| Update | `UpdateByID(ctx, id, data)` — writes **all** columns of `data` |
| Atomic counter | `IncrementByID(ctx, id, "balance", 25)` — exact for integers |
| Delete | `DeleteByID`, `DeleteMany` |
| One row | `GetByID`, `FindOne(ctx, filter)` → `ErrNotFound` when absent |
| Rows (≤ 30) | `Find(ctx, filter, preloads...)` |
| A page | `Paginate(ctx, page)`, `PaginateFilter(ctx, scope, page)` |
| HTTP list endpoint | `PaginateWithHertz(ctx, nil, scope, reqCtx)` |
| Aggregates | `Count`, `Exists`, `Max(ctx, "field", filter)`, `Min` |
| API shape | any read/write with a `Format` suffix returns `*TResponse` via `ToResource` |
| Transaction | `StartTx` + `…WithTx` + `EndTx(ctx, tx, err)`, or `database.RunInTx` |

## Filters in 30 seconds

```go
f := pagination.StructuredFilter{
	Logic: pagination.LogicAnd, // or LogicOr: how the Filters combine
	Filters: []pagination.Filter{
		{Field: "currency", Mode: pagination.ModeEqual, Value: "PHP"},
		{Field: "balance", Mode: pagination.ModeGTE, Value: 1000},
		{Field: "updated_at", Mode: pagination.ModeAfter, DataType: pagination.DataTypeDate, Value: "2026-01-01T00:00:00+08:00"},
	},
	SortFields: []pagination.SortField{{Field: "balance", Order: pagination.SortOrderDesc}},
}
rows, err := accounts.Find(ctx, f)
```

- Built-in modes: `equal notEqual gt gte lt lte contains notContains startsWith endsWith inside
  outside range before after isEmpty isNotEmpty search custom`. Field names are **bun column names**.
- **Anything more complex** — OR of AND groups, `EXISTS`, JSONB, `ILIKE`, aggregates, joins — is a
  `ModeCustom` filter that edits the bun query. Always pass values as `?` arguments and refer to the
  table as `?TableAlias`:

```go
{Mode: pagination.ModeCustom, Value: "cruz", Custom: func(q *bun.SelectQuery, v any) (*bun.SelectQuery, error) {
	return q.Where("?TableAlias.owner ILIKE ?", "%"+v.(string)+"%"), nil
}}
```

Full mode table, custom-filter cookbook, cursors, Hertz query parameters, search and partitions:
[references/filters.md](references/filters.md). Every pattern there is executed by
`test/regressions/service.bankdb_27_query_cookbook_test.go`.

## Rules that prevent real bugs

1. **Scope with code, not with the client.** Put the restriction that must always hold (owner, branch,
   status) in the `scope` argument of `PaginateFilter`/`Find`; unknown fields there fail with
   `ErrUnknownField`. `Pagination.Filter` comes from the client and silently drops unknown fields.
2. **Never build SQL by string concatenation**, in custom filters or anywhere: `?` arguments only.
3. **Map errors before returning them**: `database.MapError(err)` gives `ErrNotFound`,
   `ErrDuplicate`, `ErrForeignKey`, `ErrConstraint`, `ErrInvalidInput`/`ErrInvalidEncoding`,
   `ErrSerialization` (retry), `ErrTimeout`, `ErrUnavailable` (retry later), `ErrForbidden`, …
   Branch with `errors.Is`; log `MappedError.SafeFields()`, never the raw error (it can quote data).
4. **Money and counters**: integer minor units (`bigint`), `IncrementByID` or a locked
   read-modify-write inside one transaction; never read on the reader and write back.
5. **Tenant data**: pass `database.WithTenant(ctx, branchID)`; tables need the RLS policy.
6. **Retry only what is safe**: whole transactions on `ErrSerialization`/`ErrUnavailable` with an
   idempotency key; a failed `COMMIT` may have committed.

A complete model end to end (tags, DTOs, registration, create, read, list, PATCH/PUT, optimistic
concurrency, soft/hard delete, bulk, errors → HTTP status, Hertz handlers, new-model checklist):
[references/crud.md](references/crud.md).
Writes, validation, text rules, transactions, locking, idempotency, tenancy and error handling:
[references/writes-and-transactions.md](references/writes-and-transactions.md).
CDC, dead letters, options, PgBouncer, migrations and the test harness:
[references/operations.md](references/operations.md).
