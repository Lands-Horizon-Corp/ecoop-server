package pagination

import (
	"context"
	"fmt"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"
	"time"

	"github.com/uptrace/bun"
)

func (c *PaginationService[TData, TID]) Count(
	ctx context.Context, filter StructuredFilter,
) (int64, error) {
	if err := c.checkReady(); err != nil {
		return 0, err
	}
	return c.count(ctx, c.ReadSQLService.Client(), filter)
}

func (c *PaginationService[TData, TID]) CountWithTx(
	ctx context.Context, tx *bun.Tx, filter StructuredFilter,
) (int64, error) {
	if err := c.checkReady(); err != nil {
		return 0, err
	}
	return c.count(ctx, tx, filter)
}

func (c *PaginationService[TData, TID]) count(
	ctx context.Context, db bun.IDB, filter StructuredFilter,
) (int64, error) {
	started := time.Now()
	total, err := sql.Scoped(ctx, db, func(q bun.IDB) (int64, error) { return c.countQuery(ctx, q, filter) })
	if err != nil {
		return 0, c.report(ctx, "count", started, err, nil, filter)
	}
	return total, nil
}

func (c *PaginationService[TData, TID]) countQuery(
	ctx context.Context, db bun.IDB, filter StructuredFilter,
) (int64, error) {
	if err := checkDB(db); err != nil {
		return 0, err
	}
	var data []TData
	q := db.NewSelect().Model(&data)
	q, err := c.applyFilters(q, filter)
	if err != nil {
		return 0, fmt.Errorf("applying filters: %w", err)
	}
	total, err := q.Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("counting: %w", err)
	}
	return int64(total), nil
}
