package utils

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/uptrace/bun/driver/pgdriver"
)

// SQLState returns the five-character SQLSTATE of the PostgreSQL error in
// err's chain, or "" when there is none (a network error, a context error).
// It reads the errors of both drivers: bun's pgdriver and pgx's pgconn.
func SQLState(err error) string {
	if pg, ok := errors.AsType[pgdriver.Error](err); ok {
		return pg.Field('C')
	}
	if pg, ok := errors.AsType[*pgconn.PgError](err); ok && pg != nil {
		return pg.Code
	}
	return ""
}

// SQLErrorColumn is the column a PostgreSQL error is about (a NOT NULL or type
// violation names one), or "" when there is none.
func SQLErrorColumn(err error) string {
	if pg, ok := errors.AsType[pgdriver.Error](err); ok {
		return pg.Field('c')
	}
	if pg, ok := errors.AsType[*pgconn.PgError](err); ok && pg != nil {
		return pg.ColumnName
	}
	return ""
}

// DescribeSQLError is err as text that is safe to log or to put on a
// dead-letter record: for a PostgreSQL error only its SQLSTATE and where it
// happened (table, column, constraint), never its message or detail, which can
// quote the offending row. Any other error is its own text. Keep the original
// err for errors.Is; this is only for people.
func DescribeSQLError(err error) string {
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
