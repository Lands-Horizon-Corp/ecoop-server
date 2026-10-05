package pagination

import (
	"context"
	"database/sql"

	"github.com/uptrace/bun"
)

func (c *PaginationService[TData, TID]) GetMax(
	ctx context.Context, field string, filter StructuredFilter, preloads ...string,
) (*TData, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}
	return c.extreme(ctx, c.ReadSQLService.Client(), field, SortOrderDesc, filter, false, preloads...)
}

func (c *PaginationService[TData, TID]) GetMin(
	ctx context.Context, field string, filter StructuredFilter, preloads ...string,
) (*TData, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}
	return c.extreme(ctx, c.ReadSQLService.Client(), field, SortOrderAsc, filter, false, preloads...)
}

func (c *PaginationService[TData, TID]) GetMaxWithTx(
	ctx context.Context, tx *bun.Tx, field string, filter StructuredFilter, preloads ...string,
) (*TData, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}
	return c.extreme(ctx, tx, field, SortOrderDesc, filter, true, preloads...)
}

func (c *PaginationService[TData, TID]) GetMinWithTx(
	ctx context.Context, tx *bun.Tx, field string, filter StructuredFilter, preloads ...string,
) (*TData, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}
	return c.extreme(ctx, tx, field, SortOrderAsc, filter, true, preloads...)
}

func (c *PaginationService[TData, TID]) extreme(
	ctx context.Context, db bun.IDB, field string, order SortOrder,
	filter StructuredFilter, forUpdate bool, preloads ...string,
) (*TData, error) {
	result, err := c.paginate(ctx, db, filter, Pagination{
		PageSize: 1,
		Filter:   StructuredFilter{SortFields: []SortField{{Field: field, Order: order}}},
	}, forUpdate, preloads...)
	if err != nil {
		return nil, err
	}
	if len(result.Data) == 0 {
		return nil, sql.ErrNoRows
	}
	return result.Data[0], nil
}
