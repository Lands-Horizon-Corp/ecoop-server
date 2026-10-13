package pagination

import (
	"context"

	"github.com/uptrace/bun"
)

func (c *PaginationService[TData, TID]) Find(
	ctx context.Context, filter StructuredFilter, preloads ...string,
) ([]*TData, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}
	result, err := c.paginate(ctx, c.ReadSQLService.Client(), filter, Pagination{}, false, preloads...)
	if err != nil {
		return nil, err
	}
	return result.Data, nil
}

func (c *PaginationService[TData, TID]) FindWithTx(
	ctx context.Context, tx *bun.Tx, filter StructuredFilter, preloads ...string,
) ([]*TData, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}
	result, err := c.paginate(ctx, tx, filter, Pagination{}, true, preloads...)
	if err != nil {
		return nil, err
	}
	return result.Data, nil
}
