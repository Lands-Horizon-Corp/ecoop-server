# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Status

Go service (`github.com/Lands-Horizon-Corp/ecoop-server`, Go 1.27). [main.go](main.go) and [cmd/actions/main.go](cmd/actions/main.go) are stubs; the app is not wired up yet. `src/` holds mostly `*.sample.go` placeholders for the intended layering (controller, usecase, core, middleware, events, cron, reports, dashboard); `src/models` and `cmd/migrate` drive migrations. Real code lives in `pkg/services` and `test/regressions`.

## Commands

```sh
go build ./...
go vet ./...
make test-up                                   # Redis, Sentinel, Postgres 17, Kafka, PgBouncer (needed by the tests)
go test -race ./test/regressions/...           # race build tag flips raceEnabled (timing-sensitive tests relax under -race)
go test ./test/regressions/ -run TestBankDBHappy_TransferIsAtomicAndBalanced   # single test
go test ./test/regressions/ -bench SecurityService -run '^$'   # benchmarks

make test-up-cdc && make test-cdc              # Postgres 16 + pg_search/pg_partman + Debezium + Kafka: real CDC, pagination extensions
make test-pg16                                 # the banking suite against Postgres 16 (production major version)
make test-pgbouncer                            # through PgBouncer in transaction mode
make test-chaos                                # DOCKER_CHAOS=1: restarts Postgres, pg_dump/pg_restore
make test-soak SOAK_DURATION=10m               # long mixed workload (default run is a 5s smoke)
make fuzz FUZZTIME=30s                         # fuzz targets in test/regressions/fuzz_database_test.go

docker compose up -d     # local observability stack: Loki, Tempo, Alloy (OTLP :4317/:4318), Grafana (:3000)
```

Copy `.env.example` to `.env` (Grafana creds + `OTEL_*`, `LOG_LEVEL`, `LOG_FORMAT`). With `OTEL_EXPORTER_OTLP_ENDPOINT` unset, the app logs to stderr only.

## Architecture

- **Dependency injection with `go.uber.org/fx`.** Services take `fx.Lifecycle` and register start/stop hooks. Constructors are `NewXxx`.
- **`pkg/services/`**, one package per infrastructure service: `broadcast`, `broker` (Kafka via franz-go; `BatchBrokerServices.SubscribeBatch` commits only after the handler succeeds), `cache` (Redis), `logger` (zap + OpenTelemetry; writes to stderr, does **not** redact fields: log `database.MappedError.SafeFields()` rather than raw database errors), `qr`, `security` (PASETO, crypto, password hashing), `database`. Each splits into `<name>.go` and `<name>.types.go`; consumers depend on the interface.
- **`pkg/services/database`** combines three subpackages:
  - `sql`: one connection pool + goose migrations (session-locked). Options: `WithConnMaxLifetime`, `WithDatabaseSettings`. Also tenant/row-lock context helpers, `IsStaleSession`/`RetryStale`, log-only `Redact`.
  - `cqrs`: writes go to the writer; `Run` applies the change stream (native `{event_id, change_type, payload}` or Debezium envelopes) to the reader. With a batch broker delivery is at-least-once: transient failures retry in place, permanent ones go to `<channel>.dlq`. `processed_events` deduplicates; `ColumnVersion` keeps the newest version under reordering. Text is validated before SQL (invalid UTF-8/NUL rejected; NFC-normalized, U+FFFD/control/BiDi rejected unless `RawText`).
  - `pagination`: reads from the reader; keyset cursors. Filters set by code reject unknown fields; client filters (`Pagination.Filter`) drop them with a warning. In-transaction reads are `FOR UPDATE` unless the context is `WithoutRowLocks`.
  - `DatabaseService` (singleton) owns the writer/reader pair. `database.Register[TData, TResponse, TRequest, TID]` before `Start`, `database.Get[...]` after. Lock-free: lifecycle state is atomic. `Start` refuses a writer/reader schema-version mismatch and refuses auto-migration through PgBouncer (`WithPgBouncer`). Use `database.RunInTx` (safe while stopped, stamps the tenant), `database.WithTenant` (Postgres row-level security), `database.MapError` (maps driver/Postgres errors to `ErrNotFound`, `ErrDuplicate`, `ErrUnavailable`, ... without leaking schema details), `WithServerTimeouts`.
- **`test/regressions/`** is a single external test package against real services from docker-compose (tests fail, not skip, when a service is missing; only the chaos and soak tests are opt-in). Files: `service.<area>_test.go`; the banking suite is `service.bankdb_<nn>_<category>_test.go` on the shared harness in `service.bankdb_harness_test.go` (throwaway writer/reader databases per test, a transfer ledger built on the three database packages, a CDC simulator, a TCP fault proxy, pool-drain and goleak checks).
- Config is read from environment variables directly (`os.Getenv`), not a config file.

## Agent skills

`.claude/skills/` and `.agents/skills/` contain Go (style, naming, concurrency, context, safety, security, performance, OpenTelemetry, slog) and PostgreSQL/SQL skills; consult the relevant ones when writing Go or SQL here.
