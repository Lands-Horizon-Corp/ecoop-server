package cqrs

import (
	"context"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/pagination"
	"github.com/uptrace/bun"
)

func (c *CQRSService[TData, TResponse, TRequest, TID]) Exists(
	ctx context.Context, filter pagination.StructuredFilter,
) (bool, error) {
	return c.PaginationService.Exists(ctx, filter)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) ExistsWithTx(
	ctx context.Context, tx *bun.Tx, filter pagination.StructuredFilter,
) (bool, error) {
	return c.PaginationService.ExistsWithTx(ctx, tx, filter)
}
