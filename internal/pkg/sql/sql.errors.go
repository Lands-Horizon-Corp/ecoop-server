package sql

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/uptrace/bun/driver/pgdriver"
)

// IsPermanent reports whether err is the database rejecting the data itself:
// running the same statement with the same data again can never succeed.
//
// That is SQLSTATE class 22 (data exception: bad text encoding, numeric out of
// range, bad date, ...), class 54 (a row or statement beyond a hard limit) and
// the two constraints no later change can satisfy: 23502 (NOT NULL) and 23514
// (CHECK). Everything else is not permanent, so a caller that retries what is
// not permanent never drops data by mistake:
//
//   - connection loss, shutdown, failover and the like (see IsTransient),
//   - 23503 and 23505 (foreign key, unique): a change that is still on its way,
//     or the order the changes are applied in, can resolve them,
//   - 42xxx (a column or table the database does not have yet): a migration
//     that is still to run resolves it.
//
// Such errors are retried, which blocks the stream until the cause is fixed
// instead of skipping a change.
func IsPermanent(err error) bool {
	if err == nil {
		return false
	}
	code := sqlState(err)
	if len(code) != 5 {
		return false
	}
	switch code {
	case "23502", "23514":
		return true
	}
	return code[:2] == "22" || code[:2] == "54"
}

// IsTransient reports whether err says nothing about the data: the database
// or the connection is unavailable, so the same statement may succeed a
// moment later and the caller should retry it unchanged.
//
// That is no SQLSTATE at all (a network error, EOF, a canceled or timed-out
// context, a full connection pool) and the classes of a server that is down,
// restarting, read-only after a failover, overloaded, or that aborted this
// transaction for a reason that does not recur: 08 (connection), 25 (invalid
// transaction state, e.g. 25006 read-only after a promotion), 40
// (serialization failure, deadlock), 53 (insufficient resources), 55 (lock
// not available), 57 (shutdown, cancel, statement timeout) and 58 (I/O).
//
// A database rejection (classes 22, 23, 42, ...) is not transient: retrying it
// unchanged repeats it, so the caller must look at the statement instead.
func IsTransient(err error) bool {
	if err == nil {
		return false
	}
	code := sqlState(err)
	if len(code) != 5 {
		return true
	}
	switch code[:2] {
	case "08", "25", "40", "53", "55", "57", "58", "XX":
		return true
	}
	return false
}

// ErrorColumn is the column a PostgreSQL error is about (a NOT NULL or type
// violation names one), or "" when there is none.
func ErrorColumn(err error) string {
	if pg, ok := errors.AsType[pgdriver.Error](err); ok {
		return pg.Field('c')
	}
	if pg, ok := errors.AsType[*pgconn.PgError](err); ok && pg != nil {
		return pg.ColumnName
	}
	return ""
}

// SQLState returns the five-character SQLSTATE of the PostgreSQL error in
// err's chain, or "" when there is none (a network error, a context error).
func SQLState(err error) string { return sqlState(err) }

// Describe is err as text that is safe to log or to put on a dead-letter
// record: for a PostgreSQL error only its SQLSTATE and where it happened
// (table, column, constraint), never its message or detail, which can quote
// the offending row. Any other error is its own text. Keep the original err
// for errors.Is and IsPermanent; Describe is only for people.
func Describe(err error) string {
	if err == nil {
		return ""
	}
	if pg, ok := errors.AsType[pgdriver.Error](err); ok {
		return fmt.Sprintf("postgres %s SQLSTATE %s table=%q column=%q constraint=%q",
			pg.Field('S'), pg.Field('C'), pg.Field('t'), pg.Field('c'), pg.Field('n'))
	}
	if pg, ok := errors.AsType[*pgconn.PgError](err); ok && pg != nil {
		return fmt.Sprintf("postgres %s SQLSTATE %s table=%q column=%q constraint=%q",
			pg.Severity, pg.Code, pg.TableName, pg.ColumnName, pg.ConstraintName)
	}
	return err.Error()
}

func sqlState(err error) string {
	if pg, ok := errors.AsType[pgdriver.Error](err); ok {
		return pg.Field('C')
	}
	if pg, ok := errors.AsType[*pgconn.PgError](err); ok && pg != nil {
		return pg.Code
	}
	return ""
}
