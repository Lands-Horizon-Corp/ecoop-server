package cqrs

import (
	"context"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/uptrace/bun"
)

func (c *CQRSService[TData, TResponse, TRequest, TID]) Count(
	ctx context.Context, filter database.StructuredFilter,
) (int64, error) {
	return c.PaginationService.Count(ctx, filter)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) CountWithTx(
	ctx context.Context, tx *bun.Tx, filter database.StructuredFilter,
) (int64, error) {
	return c.PaginationService.CountWithTx(ctx, tx, filter)
}
