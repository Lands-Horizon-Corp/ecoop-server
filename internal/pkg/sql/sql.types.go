package sql

import (
	"context"
	"errors"
	"time"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
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
	// MigratePasses caps how many migrations one Migrate generates. A rename
	// can need a second pass for what it leaves behind (constraint names);
	// models that still differ after this many cannot be expressed.
	MigratePasses = 6
	// MigrationLockTimeout bounds how long a generated migration waits for a
	// lock before failing; the applier then retries it (MigrationLockRetries
	// times), so a migration never queues live traffic behind it for long.
	MigrationLockTimeout = 5 * time.Second
	MigrationLockRetries = 5

	migrationTemplate = `-- +goose Up
-- +goose StatementBegin
SELECT 'up SQL query';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 'down SQL query';
-- +goose StatementEnd
`
)

var (
	ErrNotStarted       = errors.New("sql: service not started")
	ErrNoTracker        = errors.New("sql: service has no in-flight tracker; create it with NewSQLService")
	ErrNoDSN            = errors.New("sql: dsn is empty")
	ErrInvalidDSN       = errors.New("sql: dsn is malformed")
	ErrNilContext       = errors.New("sql: nil context")
	ErrInvalidModel     = errors.New("sql: invalid model")
	ErrNoMigrationsDir  = errors.New("sql: migrations directory is not set")
	ErrNotADirectory    = errors.New("sql: migrations path is not a directory")
	ErrEmptyName        = errors.New("sql: migration name is empty")
	ErrInvalidSteps     = errors.New("sql: steps must be greater than zero")
	ErrTooManySteps     = errors.New("sql: more steps than migrations to run")
	ErrDrift            = errors.New("sql: the database differs from its migrations (changed by hand?)")
	ErrScratchDatabase  = errors.New("sql: cannot create a scratch database to plan with (Migrate needs CREATEDB on the server)")
	ErrInvalidMigration = errors.New("sql: the generated migration does not apply, so it was removed")
	ErrNotConverged     = errors.New("sql: the schema still differs from the models after migrating")
	ErrInvalidPool      = errors.New("sql: max open and max idle connections must be zero or more")

	ErrNilTx       = utils.ErrNilTx
	ErrUnsafeText  = utils.ErrUnsafeText
	ErrInvalidText = utils.ErrInvalidText
)

type (
	ISQLService interface {
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

		// the status command shows the current status of migrations
		Status(ctx context.Context) error

		// the version command shows the current migration version
		Version(ctx context.Context) error
	}
)
