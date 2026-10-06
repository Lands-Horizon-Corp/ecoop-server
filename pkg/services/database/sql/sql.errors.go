package sql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/uptrace/bun"
)

var (
	ErrNotInitialized       = errors.New("sql: database connection is not initialized")
	ErrNoMigrationsDir      = errors.New("sql: no migrations directory was provided")
	ErrInvalidMigrationsDir = errors.New("sql: migrations directory is not usable")
	ErrInvalidSteps         = errors.New("sql: steps must be greater than zero")
	ErrTooManySteps         = errors.New("sql: more steps requested than migrations available")
	ErrNoModels             = errors.New("sql: at least one model is required")
	ErrInvalidName          = errors.New("sql: migration name must contain a letter or digit")
	ErrPendingMigrations    = errors.New("sql: apply pending migrations before generating a new one")
	ErrInvalidMigration     = errors.New("sql: generated migration is not valid")

	ErrNilTx       = errors.New("sql: transaction is nil or was never started")
	ErrInvalidText = errors.New("sql: text is not valid UTF-8 or contains a NUL byte")
	ErrUnsafeText  = errors.New("sql: text contains a replacement, control or BiDi override character")
)

// CheckDB rejects a nil or zero-value connection or transaction before it is dereferenced;
// notInit is returned for a missing pool.
func CheckDB(db bun.IDB, notInit error) error {
	switch v := db.(type) {
	case nil:
		return notInit
	case *bun.DB:
		if v == nil {
			return notInit
		}
	case bun.Tx:
		if v.Tx == nil {
			return ErrNilTx
		}
	case *bun.Tx:
		if v == nil || v.Tx == nil {
			return ErrNilTx
		}
	}
	return nil
}

// IsStaleSession reports whether err means the pooled connection's server session was already gone
// when the statement was sent (failover, restart, pg_terminate_backend, a pooler restart, a closed
// socket). A statement outside a transaction that fails this way was never committed, because
// terminating a backend aborts whatever it was running, so it is safe to run it once more. Never
// retry a transaction on this basis: a COMMIT whose reply was lost may have committed.
func IsStaleSession(err error) bool {
	if err == nil {
		return false
	}
	if pg, ok := errors.AsType[*pgconn.PgError](err); ok {
		return pg.Code == "57P01" || pg.Code == "57P02" || pg.Code == "57P03"
	}
	return pgconn.SafeToRetry(err) || errors.Is(err, driver.ErrBadConn) || strings.Contains(err.Error(), "conn closed")
}

// RetryStale runs fn and, when db is the pool itself (not a transaction) and fn failed on a stale
// session, runs it once more.
func RetryStale[T any](db bun.IDB, fn func() (T, error)) (T, error) {
	v, err := fn()
	if _, pooled := db.(*bun.DB); pooled && IsStaleSession(err) {
		return fn()
	}
	return v, err
}

// IsTransient reports whether err may succeed on retry: failing to connect, connectivity, timeouts,
// server shutdown, resource exhaustion, lock and serialization conflicts, read-only (a demoted
// primary), and foreign key violations, which on a read model only mean a parent row has not been
// replicated yet. Anything else (bad data, other constraint violations) is permanent.
func IsTransient(err error) bool {
	if _, ok := errors.AsType[*pgconn.ConnectError](err); ok || pgconn.SafeToRetry(err) {
		return true
	}
	if pg, ok := errors.AsType[*pgconn.PgError](err); ok {
		switch {
		case strings.HasPrefix(pg.Code, "08"), strings.HasPrefix(pg.Code, "53"), strings.HasPrefix(pg.Code, "57P"):
			return true
		}
		switch pg.Code {
		case "40001", "40P01", "55P03", "57014", "25006", "23503":
			return true
		}
		return false
	}
	if _, ok := errors.AsType[net.Error](err); ok {
		return true
	}
	return errors.Is(err, ErrNotInitialized) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, driver.ErrBadConn) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		strings.Contains(err.Error(), "sql: connection is already closed")
}

// Redact returns err in a form that is safe to log. Postgres messages for data exceptions and syntax
// errors quote the offending input (`invalid input syntax for type bigint: "123-45-6789"`), and bun
// inlines values into SQL text, so only messages from classes that name schema objects, never values,
// are kept: integrity violations, conflicts, connectivity, resources and operator intervention.
// Everything else is reduced to its SQLSTATE and column. The result still unwraps to err; use it for
// logs only.
func Redact(err error) error {
	pg, ok := errors.AsType[*pgconn.PgError](err)
	if !ok {
		return err
	}
	class := pg.Code
	if len(class) > 2 {
		class = class[:2]
	}
	switch class {
	case "23", "40", "08", "53", "55", "57":
		return err
	}
	msg := fmt.Sprintf("postgres error (SQLSTATE %s)", pg.Code)
	if pg.ColumnName != "" {
		msg += " on column " + pg.ColumnName
	}
	return &redactedError{msg: msg, cause: err}
}

type redactedError struct {
	msg   string
	cause error
}

func (e *redactedError) Error() string { return e.msg }
func (e *redactedError) Unwrap() error { return e.cause }
