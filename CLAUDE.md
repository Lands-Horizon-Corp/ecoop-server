# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Status

Early scaffold of a Go service (`github.com/Lands-Horizon-Corp/ecoop-server`, Go 1.27). [main.go](main.go) and [cmd/actions/main.go](cmd/actions/main.go) are stubs; the app is not wired up yet. Most of `src/` contains only `*.sample.go` placeholders (controller, usecase, core, middleware, events, cron, reports, dashboard, database migrations/seeders, templates) that define the intended layering. Real code currently lives in `pkg/services` and `test/regressions`.

## Commands

```sh
go build ./...
go vet ./...
go test ./test/regressions/...                 # all tests
go test -race ./test/regressions/...           # race build tag flips raceEnabled (timing-sensitive tests relax under -race)
go test ./test/regressions/ -run TestLogRedactsSensitiveKeys   # single test
go test ./test/regressions/ -bench SecurityService -run '^$'   # benchmarks

docker compose up -d     # local observability stack: Loki, Tempo, Alloy (OTLP :4317/:4318), Grafana (:3000)
```

Copy `.env.example` to `.env` (Grafana creds + `OTEL_*`, `LOG_LEVEL`, `LOG_FORMAT`). With `OTEL_EXPORTER_OTLP_ENDPOINT` unset, the app logs to stderr only and no exporters are created, so docker is optional locally.

## Architecture

- **Dependency injection with `go.uber.org/fx`.** Services take `fx.Lifecycle` and register start/stop hooks (e.g. log flushing, telemetry shutdown). Constructors are `NewXxx`.
- **`pkg/services/`** holds infrastructure services.
  - Top-level package `services` (`service.logs.go`, `service.telemetry.go`, `service.smtp.go`, `service.storage.go`): `Telemetry` builds OTel trace and log providers (nil when no OTLP endpoint) and `LogService` wraps zap, fanning out to stderr plus the otelzap bridge. `LogService` redacts sensitive keys (`password`, `token`, `email`, `otp`, …), replaces raw context with trace IDs, and marks spans failed on error logs.
  - Subpackages `cache` (Redis, interface `CacheServices`, currently mostly `panic("unimplemented")` stubs), `logger`, `qr`, `security` (PASETO, crypto, password hashing). Each subpackage is split into `<name>.go` (impl) and `<name>.types.go` (interfaces and types); consumers depend on the interface (e.g. `security.SecurityServices`, `qr.QRServices`).
- **`src/`** is the planned application layer (controller/v1 → usecase → core, with middleware, events, cron, reports, dashboard, and `database/{migrations,seeders}`).
- **`test/regressions/`** is a single external test package that exercises services through their public API (no tests live beside the source). Test files are named `regression.<area>_test.go` or `service.<name>_test.go`.
- Config is read from environment variables directly (`os.Getenv`), not a config file.

## Agent skills

`.claude/skills/` and `.agents/skills/` contain Go (style, naming, concurrency, context, safety, security, performance, OpenTelemetry, slog) and PostgreSQL/SQL skills; consult the relevant ones when writing Go or SQL here.
