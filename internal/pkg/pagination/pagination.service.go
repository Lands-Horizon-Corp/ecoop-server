package pagination

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/uptrace/bun"
)

func (c *PaginationService[TData, TID]) Paginate(
	ctx context.Context, pagination Pagination,
) (PaginationResult[TData], error) {
	result, err := c.Pagination(ctx, pagination)
	if err != nil {
		return PaginationResult[TData]{}, err
	}
	return *result, nil
}

func (c *PaginationService[TData, TID]) PaginateFilter(
	ctx context.Context, filter StructuredFilter, pagination Pagination,
) (PaginationResult[TData], error) {
	if err := c.checkReady(); err != nil {
		return PaginationResult[TData]{}, err
	}
	result, err := c.paginate(ctx, c.ReadSQLService.Client(), filter, pagination, false)
	if err != nil {
		return PaginationResult[TData]{}, err
	}
	return *result, nil
}

func (c *PaginationService[TData, TID]) Filter(
	ctx context.Context, filter StructuredFilter,
) ([]*TData, error) {
	result, err := c.PaginateFilter(ctx, filter, Pagination{})
	if err != nil {
		return nil, err
	}
	return result.Data, nil
}

func (c *PaginationService[TData, TID]) FilterWithTx(
	ctx context.Context, tx *bun.Tx, filter StructuredFilter,
) ([]*TData, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}
	result, err := c.paginate(ctx, tx, filter, Pagination{}, true)
	if err != nil {
		return nil, err
	}
	return result.Data, nil
}

func (c *PaginationService[TData, TID]) PaginateWithHertz(
	ctx context.Context, tx *bun.Tx, filter StructuredFilter, reqCtx *app.RequestContext,
) (PaginationResult[TData], error) {
	if err := c.checkReady(); err != nil {
		return PaginationResult[TData]{}, err
	}
	var pagination Pagination
	if err := pagination.Parse(reqCtx); err != nil {
		return PaginationResult[TData]{}, err
	}
	// Without a transaction an HTTP listing reads the read model, like Paginate.
	var db bun.IDB = c.ReadSQLService.Client()
	if tx != nil {
		db = tx
	}
	result, err := c.paginate(ctx, db, filter, pagination, false)
	if err != nil {
		return PaginationResult[TData]{}, err
	}
	return *result, nil
}
