package sql

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
	"github.com/uptrace/bun"
)

// migrationsDir is relative to the working directory the service runs from.
const migrationsDir = "src/database/migrations"

type SQLService struct {
	dsn         string
	maxIdleConn int
	maxOpenConn int
	autoMigrate bool
	db          *bun.DB
	sqldb       *sql.DB
	file        *os.File
}

// Option customizes a SQLService.
type Option func(*SQLService)

// WithAutoMigrate controls whether Run applies pending migrations (default true).
// Tools such as the migration CLI turn it off so they can inspect or step migrations themselves.
func WithAutoMigrate(enabled bool) Option {
	return func(s *SQLService) { s.autoMigrate = enabled }
}

func NewSQLService(
	dsn string,
	maxIdleConn int,
	maxOpenConn int,
	file *os.File,
	opts ...Option,
) SQLServices {
	s := &SQLService{
		dsn:         dsn,
		maxIdleConn: maxIdleConn,
		maxOpenConn: maxOpenConn,
		autoMigrate: true,
		file:        file,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
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

// provider builds a goose provider per call so migration files created after Run are picked up.
// A Postgres advisory lock serializes migration runs, so replicas starting together cannot race.
func (s *SQLService) provider() (*goose.Provider, error) {
	if s.sqldb == nil {
		return nil, ErrNotInitialized
	}
	locker, err := lock.NewPostgresSessionLocker(lock.WithLockTimeout(1, 300))
	if err != nil {
		return nil, fmt.Errorf("creating migration lock: %w", err)
	}
	return goose.NewProvider(goose.DialectPostgres, s.sqldb, os.DirFS(migrationsDir), goose.WithSessionLocker(locker))
}

func (s *SQLService) out() io.Writer {
	if s.file != nil {
		return s.file
	}
	return os.Stdout
}
