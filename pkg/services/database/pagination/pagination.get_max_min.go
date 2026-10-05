package pagination

import (
	"context"
	"database/sql"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/uptrace/bun"
)

func (c *PaginationService[TData, TID]) GetMax(
	ctx context.Context, field string, filter database.StructuredFilter, preloads ...string,
) (*TData, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}
	return c.extreme(ctx, c.ReadSQLService.Client(), field, database.SortOrderDesc, filter, false, preloads...)
}

func (c *PaginationService[TData, TID]) GetMin(
	ctx context.Context, field string, filter database.StructuredFilter, preloads ...string,
) (*TData, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}
	return c.extreme(ctx, c.ReadSQLService.Client(), field, database.SortOrderAsc, filter, false, preloads...)
}

func (c *PaginationService[TData, TID]) GetMaxWithTx(
	ctx context.Context, tx *bun.Tx, field string, filter database.StructuredFilter, preloads ...string,
) (*TData, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}
	return c.extreme(ctx, tx, field, database.SortOrderDesc, filter, true, preloads...)
}

func (c *PaginationService[TData, TID]) GetMinWithTx(
	ctx context.Context, tx *bun.Tx, field string, filter database.StructuredFilter, preloads ...string,
) (*TData, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}
	return c.extreme(ctx, tx, field, database.SortOrderAsc, filter, true, preloads...)
}

func (c *PaginationService[TData, TID]) extreme(
	ctx context.Context, db bun.IDB, field string, order database.SortOrder,
	filter database.StructuredFilter, forUpdate bool, preloads ...string,
) (*TData, error) {
	result, err := c.paginate(ctx, db, filter, database.Pagination{
		PageSize: 1,
		Filter:   database.StructuredFilter{SortFields: []database.SortField{{Field: field, Order: order}}},
	}, forUpdate, preloads...)
	if err != nil {
		return nil, err
	}
	if len(result.Data) == 0 {
		return nil, sql.ErrNoRows
	}
	return result.Data[0], nil
}
