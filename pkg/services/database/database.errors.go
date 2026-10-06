package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"net"
	"strings"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/cqrs"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/pagination"
	"github.com/jackc/pgx/v5/pgconn"
)

// Application-level database errors. MapError turns a raw driver or Postgres error into one of
// these so callers branch on errors.Is and never show table, column or constraint names to clients.
var (
	ErrNotFound      = errors.New("database: record not found")
	ErrDuplicate     = errors.New("database: record already exists")
	ErrForeignKey    = errors.New("database: referenced record does not exist or is still referenced")
	ErrConstraint    = errors.New("database: value violates a data rule")
	ErrOutOfRange    = errors.New("database: numeric value out of range")
	ErrInvalidInput  = errors.New("database: invalid input value")
	ErrSerialization = errors.New("database: concurrent update conflict, retry the transaction")
	ErrRejected      = errors.New("database: operation rejected by the database")
	ErrTimeout       = errors.New("database: operation timed out")
	ErrCanceled      = errors.New("database: operation canceled")
	ErrUnavailable   = errors.New("database: database unavailable")
	ErrInternal      = errors.New("database: internal database error")
)

// MappedError is a mapped database error. Its message is only the application kind; the raw
// cause stays reachable through errors.As / errors.Is for logging, never through Error().
type MappedError struct {
	Kind  error
	Cause error
}

func (e *MappedError) Error() string   { return e.Kind.Error() }
func (e *MappedError) Unwrap() []error { return []error{e.Kind, e.Cause} }

// SQLState returns the Postgres error code behind a mapped error, or "" when there is none.
func (e *MappedError) SQLState() string {
	if pg, ok := errors.AsType[*pgconn.PgError](e.Cause); ok {
		return pg.Code
	}
	return ""
}

// MapError maps err to a *MappedError carrying one of the Err* kinds above. nil stays nil and an
// already mapped error is returned unchanged.
func MapError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := errors.AsType[*MappedError](err); ok {
		return err
	}
	return &MappedError{Kind: classify(err), Cause: err}
}

func classify(err error) error {
	if pg, ok := errors.AsType[*pgconn.PgError](err); ok {
		return classifySQLState(pg.Code)
	}
	switch {
	case errors.Is(err, cqrs.ErrInvalidText):
		return ErrInvalidInput
	case errors.Is(err, cqrs.ErrNilTx), errors.Is(err, pagination.ErrNilTx):
		return ErrInternal
	case errors.Is(err, cqrs.ErrWriteDBNotInitialized), errors.Is(err, pagination.ErrReadDBNotInitialized):
		return ErrUnavailable
	case errors.Is(err, sql.ErrNoRows):
		return ErrNotFound
	case errors.Is(err, context.DeadlineExceeded):
		return ErrTimeout
	case errors.Is(err, context.Canceled):
		return ErrCanceled
	case errors.Is(err, sql.ErrConnDone), errors.Is(err, sql.ErrTxDone), errors.Is(err, driver.ErrBadConn),
		errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), isNetError(err):
		return ErrUnavailable
	}
	return ErrInternal
}

func classifySQLState(code string) error {
	switch code {
	case "23505":
		return ErrDuplicate
	case "23503":
		return ErrForeignKey
	case "23502", "23514", "23P01":
		return ErrConstraint
	case "22003":
		return ErrOutOfRange
	case "40001", "40P01":
		return ErrSerialization
	case "57014":
		return ErrTimeout
	case "P0001":
		return ErrRejected
	}
	switch {
	case strings.HasPrefix(code, "22"):
		return ErrInvalidInput
	case strings.HasPrefix(code, "08"), code == "57P01", code == "57P02", code == "57P03":
		return ErrUnavailable
	}
	return ErrInternal
}

func isNetError(err error) bool {
	_, ok := errors.AsType[net.Error](err)
	return ok
}
