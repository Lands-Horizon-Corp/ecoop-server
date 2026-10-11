package sql

import (
	"context"
	"errors"
	"time"

	"github.com/uptrace/bun"
)

const (
	DefaultConnMaxLifetime = 30 * time.Minute // recycle connections before servers or proxies drop them
	DefaultConnMaxIdleTime = 5 * time.Minute  // close idle connections beyond MaxIdleConns sooner
	GooseTable             = "goose_db_version"
	// MigrationLockID keys the Postgres advisory lock that keeps two
	// instances from migrating at the same time.
	MigrationLockID int64 = 7_419_220_311
	// ReviewSuffix ends a generated migration that drops tables or columns.
	// goose ignores it; renaming the file to .sql after review applies it.
	ReviewSuffix = ".review"
)

var (
	ErrNotStarted      = errors.New("sql: service not started")
	ErrNoTracker       = errors.New("sql: service has no in-flight tracker; create it with NewSQLService")
	ErrNoDSN           = errors.New("sql: dsn is empty")
	ErrNoMigrationsDir = errors.New("sql: migrations directory is not set")
	ErrNotADirectory   = errors.New("sql: migrations path is not a directory")
	ErrEmptyName       = errors.New("sql: migration name is empty")
	ErrInvalidSteps    = errors.New("sql: steps must be greater than zero")
	ErrInvalidPool     = errors.New("sql: max open and max idle connections must be zero or more")
)

type (
	SQLServices interface {
		Start(ctx context.Context) error
		Run(ctx context.Context) error
		Stop(ctx context.Context) error
		Ping(ctx context.Context) error
		Client() *bun.DB

		// the migration works
		// 1. generate the migration files
		// 2. apply the migrations
		Migrate(ctx context.Context) error

		// the fresh command drops all tables and reapplies all migrations
		Fresh(ctx context.Context) error

		// the create command generates a new migration file with the given name
		Create(ctx context.Context, name string) error

		// the rollback command reverts the last applied migration
		Rollback(ctx context.Context) error
		// the rollback to command reverts migrations to the specified version
		RollbackTo(ctx context.Context, version int64) error
		// the redo command re-applies the last reverted migration
		Redo(ctx context.Context) error
		// the rollback steps command reverts the specified number of migrations
		RollbackSteps(ctx context.Context, steps int) error
		// the up steps command applies the specified number of migrations
		UpSteps(ctx context.Context, steps int) error

		// the down steps command reverts the specified number of migrations
		Status(ctx context.Context) error

		// the status command shows the current status of migrations
		Version(ctx context.Context) error
	}
)
