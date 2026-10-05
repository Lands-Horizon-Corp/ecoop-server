package sql

import (
	"context"
	"database/sql"
	"io"
	"os"

	"github.com/pressly/goose/v3"
	"github.com/uptrace/bun"
)

// migrationsDir is relative to the working directory the service runs from.
const migrationsDir = "src/database/migrations"

type SQLService struct {
	dsn         string
	maxIdleConn int
	maxOpenConn int
	db          *bun.DB
	sqldb       *sql.DB
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
		return ErrNotInitialized
	}
	return s.db.PingContext(ctx)
}

// provider builds a goose provider from the migrations directory. It is built per call so
// migration files created after Run (see Create) are picked up.
func (s *SQLService) provider() (*goose.Provider, error) {
	if s.sqldb == nil {
		return nil, ErrNotInitialized
	}
	return goose.NewProvider(goose.DialectPostgres, s.sqldb, os.DirFS(migrationsDir))
}

// out is where Status and Version print. Falls back to stdout when no file was given.
func (s *SQLService) out() io.Writer {
	if s.file != nil {
		return s.file
	}
	return os.Stdout
}
