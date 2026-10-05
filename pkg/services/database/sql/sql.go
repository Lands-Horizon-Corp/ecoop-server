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

type SQLService struct {
	dsn         string
	maxIdleConn int
	maxOpenConn int
	autoMigrate bool
	db          *bun.DB
	sqldb       *sql.DB
	migrations  *os.File
	output      io.Writer
	models      []any
}

func NewSQLService(
	dsn string,
	maxIdleConn int,
	maxOpenConn int,
	migrations *os.File,
	autoMigrate bool,
	output io.Writer,
	models []any,
) SQLServices {
	return &SQLService{
		dsn:         dsn,
		maxIdleConn: maxIdleConn,
		maxOpenConn: maxOpenConn,
		autoMigrate: autoMigrate,
		migrations:  migrations,
		output:      output,
		models:      models,
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

func (s *SQLService) provider() (*goose.Provider, error) {
	if s.sqldb == nil {
		return nil, ErrNotInitialized
	}
	dir, err := s.migrationsPath()
	if err != nil {
		return nil, err
	}
	locker, err := lock.NewPostgresSessionLocker(lock.WithLockTimeout(1, 300))
	if err != nil {
		return nil, fmt.Errorf("creating migration lock: %w", err)
	}
	return goose.NewProvider(goose.DialectPostgres, s.sqldb, os.DirFS(dir), goose.WithSessionLocker(locker))
}

func (s *SQLService) migrationsPath() (string, error) {
	if s.migrations == nil {
		return "", ErrNoMigrationsDir
	}
	info, err := s.migrations.Stat()
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalidMigrationsDir, err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("%w: %s is not a directory", ErrInvalidMigrationsDir, s.migrations.Name())
	}
	return s.migrations.Name(), nil
}

func (s *SQLService) out() io.Writer {
	if s.output != nil {
		return s.output
	}
	return os.Stdout
}
