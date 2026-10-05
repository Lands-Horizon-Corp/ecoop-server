package pagination

import (
	"context"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/uptrace/bun"
)

func (c *PaginationService[TData, TID]) Find(
	ctx context.Context, filter database.StructuredFilter, preloads ...string,
) ([]*TData, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}
	result, err := c.paginate(ctx, c.ReadSQLService.Client(), filter, database.Pagination{}, false, preloads...)
	if err != nil {
		return nil, err
	}
	return result.Data, nil
}

func (c *PaginationService[TData, TID]) FindWithTx(
	ctx context.Context, tx *bun.Tx, filter database.StructuredFilter, preloads ...string,
) ([]*TData, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}
	result, err := c.paginate(ctx, tx, filter, database.Pagination{}, true, preloads...)
	if err != nil {
		return nil, err
	}
	return result.Data, nil
}
