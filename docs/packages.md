# The packages: what each one guarantees, and how that is proven

This page is the overview of `internal/pkg`: twenty packages that a cooperative banking backend needs, each one small enough to read, each one with its failure behaviour written down and tested against real infrastructure.

It is written to be checked. Every claim below names the test suite that backs it, and every package says what it does **not** do. A claim with no test is listed as a limit, not a feature.

## The short version

| | |
|---|---|
| **Packages** | 20 packages, about 18,000 lines of Go in `internal/pkg`, plus shared helpers in `internal/utils` |
| **Tests** | 1,483 top-level tests in 31 suites; with generated subtests the heavy suites run far more (cqrs 1,655, pagination 2,645, sql 175, and 1,388 across the 18 light integration suites) |
| **How they run** | `-race`, `-count=1`, against a real Postgres, Kafka, Debezium, Valkey, S3-compatible storage, SMTP and the telemetry stack, one heavy suite at a time |
| **Static checks** | `golangci-lint` with 41 linters, `staticcheck`, `go vet` with and without the integration tag, `gofmt`: all at zero findings |
| **Last verification** | unit 19/19 packages; light integration suites 18/18 (1,388 tests); cqrs 1,655; sql 175/175; pagination 2,645; 0 failures, 0 data races |

The counts include generated subtests (a matrix of patterns, faults and partition counts). They measure how much of a grid is covered, not the number of distinct ideas, and they are stated that way on purpose.

## What makes them solid: six rules every package follows

1. **The same lifecycle for every service that holds a connection.** Services have `Start` or `Run`, and `Stop`. `Stop` refuses new work, waits for the work in flight, and only then releases its connections. Stop behaviour has its own tests in seventeen of the twenty packages (the API has eight, sql nine, cdc eight). The three without are `auth` and `security`, which have no `Stop` because they hold no connection, and `sms`, which has a `Stop` and no test for it.
2. **Never skip data silently.** What cannot be applied is retried, or parked where it can be seen and repaired. It is not dropped to keep things moving (cdc, cqrs, broker).
3. **The database decides, not the application.** Ordering, uniqueness and "is this change newer" are enforced by SQL, so any number of replicas agree without talking to each other.
4. **Errors never quote data.** Logs, dead-letter headers, readiness probes and API errors carry a code and a location, not row values, credentials or request bodies. The API returns a generic body and the real error stays in the server log.
5. **Every package is injectable.** Each has `Config`, `FromEnv`, `Module`. One line in `app.go` adds a service; a wrong or missing setting stops startup with every mistake named at once, never with the value. See [modules.md](modules.md).
6. **Every package is observable.** Each records the same operation metrics, traces and logs through one observer, which can never block or fail the application. See [logs.md](logs.md).

## Package by package

Each row says what the package does, the strongest thing it guarantees, the suite that proves it, and the limit you should know about. Larger packages have their own page; those are linked.

### Data

| Package | Does | Guarantee | Proof | Limit |
|---|---|---|---|---|
| `sql` | Postgres pool; model-driven, reviewed, reversible, lock-safe migrations | A transactional migration applies completely or not at all; every migration has a rollback that the suite runs; transactional DDL carries a lock timeout so it cannot queue the application behind it. Index changes that must run `CONCURRENTLY` run outside a transaction by design (`-- +goose NO TRANSACTION`), so writes continue | `tests/packages/sql`: 152 tests (175 with subtests), including a 100-release upgrade replay over seeded data, zero-downtime-under-load and rollback of every migration | Needs `CREATEDB` where `Migrate` runs; generated `.review` files need a human. Details in [database.md](database.md) |
| `cdc` | Delivers Debezium change events from Kafka | At least once, ordered per partition, fails toward redelivery rather than loss | `tests/packages/cdc`: 103 tests against a real broker, restarts and rebalances | Not exactly-once on its own (that is built one layer up in cqrs); needs an idempotent consumer. [database.md](database.md) |
| `cqrs` | Keeps a read database equal to the write database | Exactly-once in effect: a batch of rows and the position that says "applied up to here" commit in one transaction | `tests/packages/cqrs`: 231 tests, 1,655 with subtests, under outage, failover, crash, redelivery, replay and random fault schedules | Read-your-writes is deliberately not provided; one model is one table and one primary key. [cdc-cqrs-architecture.md](cdc-cqrs-architecture.md) |
| `pagination` | Structured filters, cursor pages, timezone-correct dates | No skipped or repeated rows across pages, correct day boundaries across timezones and DST, LIKE injection closed | `tests/packages/pagination`: 118 tests, 2,645 with subtests, checked against an oracle on a real Postgres | Index what you filter and sort on; `search` needs ParadeDB. [database.md](database.md) |
| `cache` | Valkey client with typed operations | Keeps working through connection drops in the middle of traffic, including as the backing store for a cache-aside read path | `tests/packages/cache`: 63 tests including `ConnectionDropsMidTraffic` and `CacheAsideUnderDrops` | The `Flush` test wipes a database, so it runs only with `CACHE_TEST_ALLOW_FLUSH=1`. The bounded wait on a slow cache is in the API's rate limiter (250 ms), not here |
| `storage` | S3-compatible object storage (S3, MinIO, RustFS) | An upload larger than the configured limit fails with `ErrTooLarge`; uploads from a URL refuse private addresses, and a redirect to a private address too, unless allowed | `tests/packages/storage`: 54 tests against a real MinIO, including `URLsToPrivateAddressesAreBlocked` and `RedirectToAPrivateAddressIsBlocked` | The bucket is private by default; public access is a setting you choose |

### Messaging and jobs

| Package | Does | Guarantee | Proof | Limit |
|---|---|---|---|---|
| `broker` | Kafka publish and consume with a dead-letter topic | A poison message is parked on the dead-letter topic and does not block the topic; the trace continues from publisher to handler | `tests/packages/broker`: 25 tests against a real Kafka | A record is tried 3 times (`HandlerAttempts`) and then parked, so a handler must be idempotent |
| `queue` | Background jobs on Valkey | One scheduler leader per namespace, so each tick is enqueued once; a rate-limited job type waits for the next window instead of failing | `tests/packages/queue`: 57 tests | Jobs are at least once; handlers must be idempotent |
| `cron` | Scheduled jobs | The schedule survives a load balancer: every instance knows every job, the leader enqueues each tick once, any instance can run it | `tests/packages/cron`: 39 tests | Built on the queue, so it needs the queue's Valkey |
| `broadcast` | Real-time events to browsers | An unreachable server is reported as an error at start, and a nil context fails fast instead of hanging | `tests/packages/broadcast`: 41 tests against a real Sockudo | Delivery to a client that is offline is not queued |

### Security and identity

| Package | Does | Guarantee | Proof | Limit |
|---|---|---|---|---|
| `security` | Password hashing, token and data encryption | Passwords use Argon2id with a stored cost and are compared in constant time; data uses AES-GCM with a fresh nonce; tampered ciphertext is rejected | `tests/packages/security`: 59 tests including tampering, truncation and wrong keys | Key management (where the key lives, how it rotates) is the deployment's job |
| `otp` | One-time codes by mail or SMS | The code is stored only as a peppered Argon2id hash, expires after `CodeTTL`, and is revoked after `MaxAttempts` wrong tries; a cache failure is returned as an error and is never counted as a match | `tests/packages/otp`: 48 tests, including random operations checked against a model | Delivery depends on the mail or SMS provider |
| `qr` | Encrypted, compressed QR payloads | XChaCha20-Poly1305 authenticated encryption; the decompressor has a hard memory ceiling, so a hostile payload cannot exhaust memory | `tests/packages/qr`: 48 tests | The key is supplied by configuration |
| `auth` | Cookie sessions | Cookie flags, expiry and sign-out-everywhere are tested end to end | `tests/packages/auth`: 26 tests | Authorization (who may do what) is route middleware, not part of this package |
| `api` | The HTTP server and its security middleware | A fixed middleware order on every route; request-size, header, timeout and rate limits; hosts and origins allowlisted; no stack trace or internal error ever reaches a client; `Stop` leaves nothing accepting | `tests/packages/api`: 100 tests, 2 fuzz targets, hostile and chaos suites, a real-socket suite and a 300-round start/stop test | No authentication yet; it is route middleware to be added. [api.md](api.md) |

### Communication and documents

| Package | Does | Guarantee | Proof | Limit |
|---|---|---|---|---|
| `smtp` | Sending mail | Header injection and recipient injection are rejected; invalid recipients are refused before sending | `tests/packages/smtp`: 59 tests against a real Mailpit, including `HeaderInjection` and `RecipientInjectionRejected` | No retry policy lives in this package |
| `sms` | Sending SMS | The sender number is validated at start; with security off it runs in mock mode that validates requests; the number and message body are never recorded in telemetry | `tests/packages/sms`: 3 tests (they cover the observer, including that no number or body is recorded) | Thinnest package: sending, formatting and the live Twilio path are not covered by tests |
| `pdf` | PDF rendering through an engine | Survives the engine going away and coming back; concurrent renders from several instances | `tests/packages/pdf`: 15 tests against the real engine, including `EngineGoneAndBack` | Rendering is only as available as the engine |

### Platform

| Package | Does | Guarantee | Proof | Limit |
|---|---|---|---|---|
| `observability` | Metrics, traces, logs | Telemetry never blocks or fails the application; spans carry no secrets; trace context is not trusted from the internet; shutdown is bounded | `tests/packages/observability`: 68 tests plus `otelobs` 36, and an end-to-end test against a real collector, Prometheus, Tempo and Grafana | Logs go to stderr as JSON, not over OTLP; no alert rules yet. [logs.md](logs.md) |
| `config` | Loads settings from files and environment | A malformed or hostile file is rejected with an error, never a panic | `tests/packages/config`: 41 tests including poison files and a randomised edit test | Used on its own, it is not an fx module |

## Shared helpers in `internal/utils`

The reusable parts are small and tested on their own, so that a package is not the only thing standing between a bug and production:

| Helper | Used for | Proof |
|---|---|---|
| `Gate`, `Inflight` | "Refuse new work, wait for what is running" in every `Stop` | `tests/utils/gate`, `tests/utils/inflight` (31 tests) |
| `Limiter` | Per-key rate limits with a bounded table | `tests/utils/limiter` |
| `Env` | Typed environment reads that collect every error and never print a value | `tests/utils/env` |
| `OnLifecycle`, `Sequence` | Start and stop wiring for fx | `tests/utils/lifecycle` |
| `CleanText` and friends | Input normalisation and control-character stripping | `tests/utils/text`, with a fuzz target |

## How this is verified

```sh
make check          # build, vet (both tag sets), gofmt, golangci-lint
make test-unit      # no Docker
make test-light     # every integration suite except cqrs, sql and pagination
make test-heavy     # cqrs, sql, pagination, one at a time
```

- **Real infrastructure, not mocks.** The integration suites start Postgres, Kafka, Debezium, Valkey, MinIO, Mailpit and the telemetry stack from `docker-compose.yml`. They skip with a clear message when the stack is down.
- **The race detector is always on.** `-race` is in every target.
- **Tests that cannot fail are not trusted.** The tests added in the latest pass (environment, lifecycle, limiter, text and the module tests) were mutation-checked: the production code was broken on purpose to see the test fail.
- **Failures are investigated, not retried until green.** During the last pass, a start/stop race in the API that a standalone run hid was reproduced under CPU load, fixed in `api`, and given a 300-round regression test. A lint auto-fix that had silently altered a test's deliberate typo was caught by the suite and is now guarded.

## What is not done, plainly

Investors and engineers will ask. These are the open items, stated here first:

- **The API has no authentication or tenant checks yet.** The server, its security middleware and its tests exist; deciding who may call what is route-level work still to be written. See [api.md](api.md), section 3.1.
- **Some timing-sensitive migration tests need an idle machine.** `TestPG100_*` take 80-115 seconds against a 2-minute limit and fail when the machine is loaded. Measured: the committed code and the current code are the same speed, so this is machine variance, but the limit has little headroom.
- **`sms` has three tests** and no live-provider test; it is the thinnest package.
- **No tail sampling, alert rules or log shipping** in the observability stack; the dashboards exist, the thresholds do not. See [logs.md](logs.md).
- **Kafka in `docker-compose.yml` runs with replication factor 1** for development. Production needs a factor above 1 and `min.insync.replicas` of 2; this is a deployment requirement, listed in [database.md](database.md).
- **Read-your-writes is not provided** by the read database; requests that write and then read must use the write database.

## Where to read more

| Topic | Page |
|---|---|
| Adding a package to the application, settings, start and stop order | [modules.md](modules.md) |
| CDC, CQRS, SQL migrations and pagination in depth, with verdicts | [database.md](database.md) |
| Kafka, Debezium and CQRS failure handling | [cdc-cqrs-architecture.md](cdc-cqrs-architecture.md) |
| The HTTP API's security controls and what is built | [api.md](api.md) |
| Metrics, traces and logs | [logs.md](logs.md) |
