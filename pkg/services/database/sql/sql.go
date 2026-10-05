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
}
