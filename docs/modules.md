# Modules: injecting a package

Every service in `internal/pkg` can be added to the application with one line. This page is how that works and how to change it. For what each package guarantees and how that is tested, see [packages.md](packages.md).

## The one line

[main.go](../main.go) is `fx.New(Modules()).Run()`. [app.go](../app.go) holds `Modules()`, the list of services the application runs:

```go
cache.Module,
sql.Module,
queue.Module,
smtp.Module,
// ...
api.Module,
```

Remove a line to run without that service, as long as nothing else in the list needs it. `go test .` checks the whole list: every service's dependencies must be provided by another line, without starting anything.

## What a module is

Each package has a `<package>.module.go` with the same five parts:

| Part | What it is |
|---|---|
| `Config` | The settings, as a plain struct. |
| `FromEnv(lookup)` | Builds the `Config` from environment variables. `lookup` is `os.LookupEnv`, or a map in a test. |
| `Module` | The fx module: provides the `Config` (from the process environment) and the service. |
| `Params` | What the service needs from the application, as an `fx.In` struct. |
| `New(Params)` | Builds the service and registers its start and stop. |

The existing constructors (`cache.NewCacheService(...)` and the rest) are unchanged. `New` calls them. Tests and any code that builds a service by hand keep working.

## Settings

Every setting is an environment variable, listed with its default in [.env.example](../.env.example). Each `FromEnv` documents its own variables in its comment.

- A blank variable counts as unset.
- A required variable that is missing, or a value that does not parse, **stops the application at startup**. All the mistakes are reported at once, not one per run.
- An error names the variable and what was expected. It never contains the value, so a malformed password cannot reach a log.
- A flag must be `true` or `false` (`1` and `0` also work). A typo such as `SMTP_SECURED=yes` is an error and not quietly `false`.

## Start and stop order

fx builds a service after the ones it depends on, so their start hooks are registered first. The result, with no ordering code of our own:

- **Start:** dependencies first (the cache, then the queue built on it, then mail and OTP built on the queue).
- **Stop:** the reverse (OTP, mail, the queue, then the cache).

Observability is listed first so it stops last, and still receives what the other services record while they shut down. The API is listed last so it is the first to refuse new work.

Services differ in which call opens their connection, and the module uses the right one: `Run` for cache, broker, CDC, storage and broadcast; `Start` for queue, mail, SMS, OTP, QR, PDF and cron; `Start` then `Run` for the database (open, then apply pending migrations when `POSTGRES_AUTO_MIGRATE` is on).

## Changing something

- **A different setting:** change the environment variable.
- **A setting that does not exist yet:** add a field to the package's `Config`, read it in `FromEnv`, pass it in `New`, and add the variable to `.env.example`. The tests in `tests/internal/fromenv` show what to cover.
- **A custom service:** provide your own `Config` and leave the package's `Module` out, or build the service with `New(Params{...})` directly.
- **An observer, an in-flight tracker:** optional. If the application provides an `observability.IObserver` (the observability module does), every service uses it; without one they observe nothing.

## Two databases

`sql.Module` provides the main database. The CQRS read model is a second Postgres, built from the same package:

```go
read, err := sql.ReadFromEnv(os.LookupEnv)        // POSTGRES_READ_*
svc, err := sql.NewFromConfig(lc, read, nil, nil, observer, nil)
```

The read database is never migrated by the service, so `ReadFromEnv` has no migrations directory or auto-migrate setting. fx tells services apart by type, and both are an `ISQLService`, so give each its own type:

```go
type Reader struct{ sql.ISQLService }

fx.Provide(func(lc fx.Lifecycle) (Reader, error) {
	cfg, err := sql.ReadFromEnv(os.LookupEnv)
	if err != nil {
		return Reader{}, err
	}
	svc, err := sql.NewFromConfig(lc, cfg, nil, nil, nil, nil)
	return Reader{svc}, err
})
```

`TestAReadDatabaseCanBeBuiltBesideTheMainOne` in [tests/internal/fromenv](../tests/internal/fromenv/sqlmodule_test.go) builds exactly this next to `sql.Module`.

## What is not a module

`cqrs`, `pagination` and `auth` are generic over a model or a claim type, so one module cannot build them: you build them for each model, from the services above. `config` has its own `Start`, `Run` and `Get` and is used on its own.

## Models for migrations

The database diffs its migrations against your Bun models. Supply them once:

```go
fx.Supply(sql.Models{&User{}, &Member{}})
```

Without models the service still runs; it cannot generate new migrations.
