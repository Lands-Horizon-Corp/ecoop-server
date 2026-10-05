package sql

import (
	"context"

	"github.com/uptrace/bun"
)

type SQLServices interface {
	Ping(ctx context.Context) error
	Client() *bun.DB
	Run(ctx context.Context) error
	Stop(ctx context.Context) error

	Migrate(ctx context.Context) error
	Rollback(ctx context.Context) error
	RollbackTo(ctx context.Context, version int64) error
	Redo(ctx context.Context) error
	Status(ctx context.Context) error
	Version(ctx context.Context) error
	Fresh(ctx context.Context) error
	Create(ctx context.Context, name string) error
	Diff(ctx context.Context, name string) (string, error)
	RollbackSteps(ctx context.Context, steps int) error
	UpSteps(ctx context.Context, steps int) error
}
