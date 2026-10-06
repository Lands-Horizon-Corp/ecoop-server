package sql

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/logger"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/attribute"
)

type SQLService struct {
	dsn         string
	maxIdleConn int
	maxOpenConn int
	autoMigrate bool
	// db and sqldb are swapped by Run/Stop while requests may be reading them (a shutdown during
	// traffic), so they are atomic; a request sees either the open pool or nil, never a torn value.
	db         atomic.Pointer[bun.DB]
	sqldb      atomic.Pointer[sql.DB]
	migrations *os.File
	output     io.Writer
	models     []any

	log logger.LogContextService

	connMaxLifetime time.Duration
	connMaxIdleTime time.Duration
	dbSettings      map[string]string
}

func NewSQLService(
	dsn string,
	maxIdleConn int,
	maxOpenConn int,
	migrations *os.File,
	autoMigrate bool,
	output io.Writer,
	models []any,

	log logger.LogContextService,
	opts ...Option,
) SQLServices {
	s := &SQLService{
		dsn:         dsn,
		maxIdleConn: maxIdleConn,
		maxOpenConn: maxOpenConn,
		autoMigrate: autoMigrate,
		migrations:  migrations,
		output:      output,
		models:      models,

		log: log,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *SQLService) Client() *bun.DB {
	return s.db.Load()
}

func (s *SQLService) observe(name string, fn func() error, attrs ...attribute.KeyValue) error {
	if s.log == nil {
		return fn()
	}
	var orig error
	_ = s.log.Observe(name, func() error {
		orig = fn()
		return Redact(orig) // logged redacted, returned intact
	}, attrs...)
	return orig
}

func (s *SQLService) Ping(ctx context.Context) error {
	db := s.db.Load()
	if db == nil {
		return ErrNotInitialized
	}
	return db.PingContext(ctx)
}

func (s *SQLService) provider() (*goose.Provider, error) {
	sqldb := s.sqldb.Load()
	if sqldb == nil {
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
	return goose.NewProvider(goose.DialectPostgres, sqldb, os.DirFS(dir), goose.WithSessionLocker(locker))
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
