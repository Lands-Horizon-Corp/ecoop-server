package pagination

import (
	"context"
	"fmt"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/uptrace/bun"
)

func (c *PaginationService[TData, TID]) Exists(
	ctx context.Context, filter database.StructuredFilter,
) (bool, error) {
	if err := c.checkReady(); err != nil {
		return false, err
	}
	return c.exists(ctx, c.ReadSQLService.Client(), filter)
}

func (c *PaginationService[TData, TID]) ExistsWithTx(
	ctx context.Context, tx *bun.Tx, filter database.StructuredFilter,
) (bool, error) {
	if err := c.checkReady(); err != nil {
		return false, err
	}
	return c.exists(ctx, tx, filter)
}

func (c *PaginationService[TData, TID]) exists(
	ctx context.Context, db bun.IDB, filter database.StructuredFilter,
) (bool, error) {
	var data []TData
	q := db.NewSelect().Model(&data)
	q, err := c.applyFilters(q, filter)
	if err != nil {
		return false, fmt.Errorf("applying filters: %w", err)
	}
	ok, err := q.Exists(ctx)
	if err != nil {
		return false, fmt.Errorf("checking existence: %w", err)
	}
	return ok, nil
}
