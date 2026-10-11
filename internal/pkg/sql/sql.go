package sql

import (
	"context"
	"database/sql"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/uptrace/bun"
)

type SQLService struct {
	dsn         string
	maxIdleConn int
	maxOpenConn int
	autoMigrate bool

	db         atomic.Pointer[bun.DB]
	sqldb      atomic.Pointer[sql.DB]
	migrations *os.File
	output     io.Writer
	models     []any

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

// Client implements [SQLServices].
func (s *SQLService) Client() *bun.DB {
	panic("unimplemented")
}

// Create implements [SQLServices].
func (s *SQLService) Create(ctx context.Context, name string) error {
	panic("unimplemented")
}

// Fresh implements [SQLServices].
func (s *SQLService) Fresh(ctx context.Context) error {
	panic("unimplemented")
}

// Migrate implements [SQLServices].
func (s *SQLService) Migrate(ctx context.Context) error {
	panic("unimplemented")
}

// Ping implements [SQLServices].
func (s *SQLService) Ping(ctx context.Context) error {
	panic("unimplemented")
}

// Redo implements [SQLServices].
func (s *SQLService) Redo(ctx context.Context) error {
	panic("unimplemented")
}

// Rollback implements [SQLServices].
func (s *SQLService) Rollback(ctx context.Context) error {
	panic("unimplemented")
}

// RollbackSteps implements [SQLServices].
func (s *SQLService) RollbackSteps(ctx context.Context, steps int) error {
	panic("unimplemented")
}

// RollbackTo implements [SQLServices].
func (s *SQLService) RollbackTo(ctx context.Context, version int64) error {
	panic("unimplemented")
}

// Run implements [SQLServices].
func (s *SQLService) Run(ctx context.Context) error {
	panic("unimplemented")
}

// Start implements [SQLServices].
func (s *SQLService) Start(ctx context.Context) error {
	panic("unimplemented")
}

// Status implements [SQLServices].
func (s *SQLService) Status(ctx context.Context) error {
	panic("unimplemented")
}

// Stop implements [SQLServices].
func (s *SQLService) Stop(ctx context.Context) error {
	panic("unimplemented")
}

// UpSteps implements [SQLServices].
func (s *SQLService) UpSteps(ctx context.Context, steps int) error {
	panic("unimplemented")
}

// Version implements [SQLServices].
func (s *SQLService) Version(ctx context.Context) error {
	panic("unimplemented")
}
