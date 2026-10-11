package sql

import (
	"context"

	"github.com/uptrace/bun"
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
