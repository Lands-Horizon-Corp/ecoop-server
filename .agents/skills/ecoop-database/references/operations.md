# Operations: CDC, options, PgBouncer, migrations, testing

## Contents

- How the read model is fed
- Registration options
- Service options
- PgBouncer
- Migrations
- Testing

## How the read model is fed

Postgres writer → Debezium (logical replication) → Kafka topic per table → `db.Run` runners →
reader. Each registered model's `Channel` is its topic (`<topic.prefix>.public.<table>`).

- Message formats: Debezium change events (schemas on or off) and the native
  `{"event_id","change_type","payload"}` envelope. Columns map to struct fields by **bun column
  name**; unknown columns are ignored (the writer can gain a column before the reader's model).
- Debezium connector settings that matter: `decimal.handling.mode=string` if you have `numeric`
  columns; `tombstones.on.delete=false`.
- With a batch broker (`broker.BatchBrokerServices`, i.e. Kafka) delivery is **at-least-once**: a
  batch is committed only after it is applied. Transient failures (database down, timeout,
  serialization, a parent row not replicated yet) are retried in place with backoff; permanent ones
  (bad data, a constraint the reader enforces) go to `<Channel>.dlq` with the original message and the
  reason. Duplicates are skipped through `processed_events`.
- A non-batch broker acknowledges on receipt (at-most-once); the runner logs a warning.
- `ColumnVersion` (e.g. `"version"` or `"updated_at"`) keeps the newest row when events arrive out
  of order. Deletes are not versioned: a stale update after a delete re-creates the row.
- Several app replicas can run the runners at once (same consumer group); writes are ordered so
  replicas never deadlock.
- Change hooks (`Created`/`Updated`/`Deleted` + `Dispatch`/broadcast) run on a bounded worker pool
  (`HookWorkers`, default 64) after a change is applied; `Stop` drains them.

## Registration options

| Field | Purpose |
|---|---|
| `Channel` | broker topic of the table's change stream |
| `ColumnDefaultID` / `ColumnDefaultSort` | ID column (default `id`), default sort (`updated_at DESC`) |
| `ColumnVersion` | out-of-order protection on the read model |
| `Preloads` | default relations for reads |
| `ToResource`, `FromRequest` | DTO mapping for `…Format` and `…WithValidation` |
| `Created`/`Updated`/`Deleted`, `Dispatch` | change events and their delivery |
| `MaxRetries` | 0 = retry transient failures until stopped; >0 = stop the runner unacknowledged |
| `DLQTopic` | default `<Channel>.dlq`; `"-"` disables (messages are then dropped) |
| `HookWorkers` | concurrency of change hooks |
| `RawText` | keep text bytes exactly (no NFC / unsafe-character checks) |

## Service options

```go
database.WithServerTimeouts(database.ServerTimeouts{Statement: 30*time.Second, Lock: 5*time.Second, IdleInTransaction: time.Minute})
database.WithConnMaxLifetime(30*time.Minute) // recycle connections (failover, load balancers)
database.WithConnMaxIdleTime(5*time.Minute)
database.WithPgBouncer()                      // transaction-pooling mode
```

Server timeouts are stored on the database (`ALTER DATABASE … SET`), so they hold for callers that
forgot a context deadline and work through PgBouncer.

## PgBouncer

- `WithPgBouncer()` switches pgx to `exec` mode (no named prepared statements) and makes `Start`
  refuse auto-migration (`ErrMigrateThroughPooler`): goose's session lock cannot work through a
  transaction pooler. Run migrations against Postgres directly, then start the app through PgBouncer.
- Everything session-scoped is avoided by design: tenants and timeouts use transaction-local or
  database-level settings.

## Migrations

- goose SQL files in the migrations directory; `Start` applies them to writer **and** reader when
  `autoMigrate` is on (concurrent instances serialize on a session lock).
- `Start` refuses to run when writer and reader are on different versions (`ErrSchemaMismatch`).
- A `DO $$ … $$` block needs `-- +goose StatementBegin` / `-- +goose StatementEnd` around it.
- Put invariants in the schema, not only in Go: `CHECK`, foreign keys, unique keys, triggers for
  append-only and frozen rows, RLS policies. The service maps their violations to error kinds.

## Testing

- Tests live in `test/regressions` and run against docker-compose services (`make test-up`).
- Use the banking harness (`newBDBank(t, bdOpts{…})` in `service.bankdb_harness_test.go`): throwaway
  writer/reader databases, `b.replicate()` to sync the reader, a fault proxy, and automatic
  pool-drain checks. Options select Postgres 16, PgBouncer, tenancy, real Kafka or a recording logger.
- `make test-cdc` (real Debezium), `make test-pg16`, `make test-pgbouncer`, `make test-chaos`,
  `make test-soak`, `make fuzz`.
