package pagination

import (
	"context"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/uptrace/bun"
)

func (c *PaginationService[TData, TID]) Paginate(
	ctx context.Context, pagination database.Pagination,
) (database.PaginationResult[TData], error) {
	result, err := c.Pagination(ctx, pagination)
	if err != nil {
		return database.PaginationResult[TData]{}, err
	}
	return *result, nil
}

func (c *PaginationService[TData, TID]) PaginateFilter(
	ctx context.Context, filter database.StructuredFilter, pagination database.Pagination,
) (database.PaginationResult[TData], error) {
	if err := c.checkReady(); err != nil {
		return database.PaginationResult[TData]{}, err
	}
	result, err := c.paginate(ctx, c.ReadSQLService.Client(), filter, pagination, false)
	if err != nil {
		return database.PaginationResult[TData]{}, err
	}
	return *result, nil
}

func (c *PaginationService[TData, TID]) Filter(
	ctx context.Context, filter database.StructuredFilter,
) ([]*TData, error) {
	result, err := c.PaginateFilter(ctx, filter, database.Pagination{})
	if err != nil {
		return nil, err
	}
	return result.Data, nil
}

func (c *PaginationService[TData, TID]) FilterWithTx(
	ctx context.Context, tx *bun.Tx, filter database.StructuredFilter,
) ([]*TData, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}
	result, err := c.paginate(ctx, tx, filter, database.Pagination{}, true)
	if err != nil {
		return nil, err
	}
	return result.Data, nil
}

func (c *PaginationService[TData, TID]) PaginateWithHertz(
	ctx context.Context, tx *bun.Tx, filter database.StructuredFilter, reqCtx *app.RequestContext,
) (database.PaginationResult[TData], error) {
	if err := c.checkReady(); err != nil {
		return database.PaginationResult[TData]{}, err
	}
	var pagination database.Pagination
	if err := pagination.Parse(reqCtx); err != nil {
		return database.PaginationResult[TData]{}, err
	}
	result, err := c.paginate(ctx, tx, filter, pagination, false)
	if err != nil {
		return database.PaginationResult[TData]{}, err
	}
	return *result, nil
}
