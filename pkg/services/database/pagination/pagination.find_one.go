package pagination

import (
	"context"
	"database/sql"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/uptrace/bun"
)

func (c *PaginationService[TData, TID]) FindOne(
	ctx context.Context, filter database.StructuredFilter, preloads ...string,
) (*TData, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}
	return c.findOne(ctx, c.ReadSQLService.Client(), filter, false, preloads...)
}

func (c *PaginationService[TData, TID]) FindOneWithTx(
	ctx context.Context, tx *bun.Tx, filter database.StructuredFilter, preloads ...string,
) (*TData, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}
	return c.findOne(ctx, tx, filter, true, preloads...)
}

func (c *PaginationService[TData, TID]) findOne(
	ctx context.Context, db bun.IDB, filter database.StructuredFilter, forUpdate bool, preloads ...string,
) (*TData, error) {
	result, err := c.paginate(ctx, db, filter, database.Pagination{PageSize: 1}, forUpdate, preloads...)
	if err != nil {
		return nil, err
	}
	if len(result.Data) == 0 {
		return nil, sql.ErrNoRows
	}
	return result.Data[0], nil
}
