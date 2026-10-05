package sql

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"os"

	"github.com/pressly/goose/v3"
	"github.com/uptrace/bun"
)

// migrationsDir is relative to the working directory the service runs from.
const migrationsDir = "src/database/migrations"

var errNotInitialized = errors.New("database connection is not initialized")

type SQLService struct {
	dsn         string
	maxIdleConn int
	maxOpenConn int
	db          *bun.DB
	sqldb       *sql.DB
	migrator    *goose.Provider
	file        *os.File
}

func NewSQLService(
	dsn string,
	maxIdleConn int,
	maxOpenConn int,
	file *os.File,
) SQLServices {
	return &SQLService{
		dsn:         dsn,
		maxIdleConn: maxIdleConn,
		maxOpenConn: maxOpenConn,
		file:        file,
	}
}

func (s *SQLService) Client() *bun.DB {
	return s.db
}

func (s *SQLService) Ping(ctx context.Context) error {
	if s.db == nil {
		return errNotInitialized
	}
	return s.db.PingContext(ctx)
}

// provider returns the goose provider created by Run.
func (s *SQLService) provider() (*goose.Provider, error) {
	if s.migrator == nil {
		return nil, errNotInitialized
	}
	return s.migrator, nil
}

// out is where Status and Version print. Falls back to stdout when no file was given.
func (s *SQLService) out() io.Writer {
	if s.file != nil {
		return s.file
	}
	return os.Stdout
}
