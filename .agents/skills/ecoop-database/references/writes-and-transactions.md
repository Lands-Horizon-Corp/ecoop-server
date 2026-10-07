# Writes, transactions, tenancy and errors

## Contents

- Writing rows
- Validation and text rules
- Transactions
- Locking reads and read-modify-write
- Idempotent operations (exactly once)
- Tenancy (row-level security)
- Errors: mapping, retrying, logging

## Writing rows

```go
acc, err := accounts.Create(ctx, Account{ID: id, Owner: name, Balance: 0})   // RETURNING *: defaults filled in
res, err := accounts.CreateFormat(ctx, a)                                   // *AccountResponse via ToResource
rows, err := accounts.CreateMany(ctx, batch)                               // one INSERT; 50k rows is fine
acc, err  = accounts.CreateWithValidation(ctx, req)                        // validates req, then FromRequest
acc, err  = accounts.UpdateByID(ctx, id, a)                                // sets ALL columns from a
acc, err  = accounts.IncrementByID(ctx, id, "balance", -250)               // SET balance = balance + (-250)
err       = accounts.DeleteByID(ctx, id)                                   // ErrNotFound when absent
err       = accounts.DeleteMany(ctx, ids)
```

- `UpdateByID` writes every column of the struct you pass, zero values included. Load the row (in a
  transaction), change it, write it back — never update from a partial struct.
- `IncrementByID` is a single atomic `UPDATE … SET col = col + delta RETURNING *`: concurrent
  increments never lose updates, and whole deltas stay exact on `bigint` (no float rounding).
- Struct tags: `bun:"col,nullzero,default:…"` lets the database default apply on zero values;
  nullable JSONB needs `nullzero` (`bun:"profile,type:jsonb,nullzero"`), otherwise a nil map is stored
  as JSON `null`, not SQL `NULL`.
- Bulk writes copy your slice before cleaning it; your data is never modified in place.

## Validation and text rules

- `Create*`/`Update*` run `validator` on the struct (`validate:"required"` tags); `…WithValidation`
  validates the request DTO then maps it with `FromRequest`.
- Every string, `*string`, `[]string` and JSON map is checked before SQL runs:
  - invalid UTF-8 (truncated or overlong sequences) or a NUL byte → `ErrInvalidEncoding`, always;
  - by default also: NFC normalization (`Müller` is stored as `Müller`, so unique indexes see
    duplicates), and U+FFFD, control characters (except tab/newline/CR) and BiDi overrides are
    rejected (`ErrInvalidInput`). Emoji, ZWJ sequences and RTL scripts are fine.
  - `Registration.RawText = true` opts a model out of normalization and the unsafe-character checks
    (for fields that must keep exact bytes); encoding checks still apply.

## Transactions

```go
err := database.RunInTx(ctx, db, nil, func(ctx context.Context, tx bun.Tx) error {
	from, err := accounts.GetByIDWithTx(ctx, &tx, fromID) // SELECT … FOR UPDATE on the writer
	if err != nil { return err }
	if from.Balance < amount { return ErrInsufficientFunds }
	if _, err := accounts.IncrementByIDWithTx(ctx, tx, fromID, "balance", float64(-amount)); err != nil { return err }
	_, err = accounts.IncrementByIDWithTx(ctx, tx, toID, "balance", float64(amount))
	return err // nil commits; an error or a panic rolls back
})
```

- `database.RunInTx` is the default: commit/rollback/panic handled, tenant stamped, and it returns
  `ErrUnavailable` instead of crashing while the service is stopped.
- `accounts.StartTx(ctx)` + `EndTx(ctx, tx, err)` does the same by hand (`EndTx` rolls back when
  `err != nil` and returns `err`).
- Inside, use only `…WithTx` methods with the same `tx`; a plain `Create` would run outside it.
- Isolation: `&sql.TxOptions{Isolation: sql.LevelSerializable}` for invariants across rows without
  locks; retry the whole function on `ErrSerialization`.
- Savepoints: `tx.RunInTx(ctx, nil, func(ctx, sp bun.Tx) error {...})` rolls back only the inner step.

## Locking reads and read-modify-write

- `GetByIDWithTx`, `FindWithTx`, `FindOneWithTx`, `MaxWithTx`, … read the **writer** with
  `FOR UPDATE`: other writers wait until you commit. That is what prevents lost updates and double
  spending.
- Lock several rows in a fixed order to avoid deadlocks: one `FindWithTx` with
  `ModeInside` on the IDs and `SortFields: id ASC`.
- Reports that must not block writers: `database.WithoutRowLocks(ctx)` makes those reads plain
  snapshots (and allows `&sql.TxOptions{ReadOnly: true}`). Never write back what such a read returned.

## Idempotent operations (exactly once)

Clients retry. A payment must move money once per request:

1. Give the operation an idempotency key with a `UNIQUE` constraint.
2. In one transaction: look the key up (`FindOneWithTx`); if found, return the stored result.
3. Otherwise do the work and insert the key row in the same transaction.
4. If the insert fails with `ErrDuplicate`, a concurrent request won: read and return its result.
5. Store a hash of the request; reusing a key with a different request is an error.

This also settles "commit outcome unknown": if the connection dies during `COMMIT`, the client
retries with the same key and gets either the committed result or a first execution.
Reference implementation: `bdLedger.Transfer` in `test/regressions/service.bankdb_harness_test.go`.

## Tenancy (row-level security)

```go
ctx = database.WithTenant(ctx, branchID) // every read, write and transaction is confined to it
```

- Each tenant table needs `tenant_id` (default `NULLIF(current_setting('app.tenant_id', true), '')`),
  `ENABLE` + `FORCE ROW LEVEL SECURITY`, and the policy shown in `pkg/services/database/sql/sql.tenant.go`.
- The application must connect as a non-superuser (superusers bypass RLS).
- No tenant in the context: nothing visible, every write `ErrForbidden` (fails closed).
- Cross-tenant IDs look like missing rows (`ErrNotFound`), not like a permission error.
- The CDC runner writes the read model as the replicator role, so all tenants are mirrored.

## Errors: mapping, retrying, logging

```go
if err != nil {
	err = database.MapError(err)
	switch {
	case errors.Is(err, database.ErrNotFound):       // 404
	case errors.Is(err, database.ErrDuplicate):      // 409
	case errors.Is(err, database.ErrInvalidInput),   // 400 (includes ErrInvalidEncoding)
		errors.Is(err, database.ErrConstraint),
		errors.Is(err, database.ErrForeignKey),
		errors.Is(err, database.ErrOutOfRange):
	case errors.Is(err, database.ErrForbidden):      // 403
	case errors.Is(err, database.ErrSerialization):  // retry the whole transaction
	case errors.Is(err, database.ErrTimeout),
		errors.Is(err, database.ErrUnavailable):     // 503 / retry with the idempotency key
	case errors.Is(err, database.ErrRejected):       // a database rule (trigger) refused it
	}
	if m, ok := errors.AsType[*database.MappedError](err); ok {
		log.Error(m, "account update failed", m.SafeFields()...) // sqlstate, table, constraint; no values
	}
}
```

- `MappedError.Error()` is only the kind; schema names and values never reach a client.
- `SafeFields()` is safe to log; the raw error and Postgres `Detail` can contain customer data.
- Single statements outside a transaction already retry once on a dead pooled connection; never add
  your own retry around a transaction without an idempotency key.
