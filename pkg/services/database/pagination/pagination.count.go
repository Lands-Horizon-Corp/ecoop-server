package pagination

import (
	"context"
	"fmt"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
)

// Count returns the number of rows in TData's table matching filter — "how
// many rows would Filter(ctx, filter) have returned", without scanning any
// of them back. A zero-value domains.StructuredFilter{} counts every row.
// filter is treated the same trusted/hardcoded way Filter/FilterWithTx
// treat theirs (see paginate's doc comment): an unknown field in it is a
// real error, not something normalizeFilters would silently drop — there's
// no separate frontend-supplied filter parameter here for that leniency to
// apply to.
func (c *PaginationService[TData, TID]) Count(
	ctx context.Context, filter database.StructuredFilter,
) (int64, error) {
	if err := c.checkReady(); err != nil {
		return 0, err
	}
	return c.count(ctx, c.ReadSQLService.Client(), filter)
}

// CountWithTx is Count run against a caller-supplied *bun.Tx instead of a
// plain client. That tx is expected to come from WriteSQLService (the
// writer), not ReadSQLService — see FilterWithTx's doc comment in
// pagination.service.go for why — e.g. counting rows written earlier in the
// same transaction, before it commits and becomes visible through a
// separate connection.
func (c *PaginationService[TData, TID]) CountWithTx(
	ctx context.Context, tx *bun.Tx, filter database.StructuredFilter,
) (int64, error) {
	if err := c.checkReady(); err != nil {
		return 0, err
	}
	return c.count(ctx, tx, filter)
}

// count applies filter to a COUNT(*) query against db — the counting
// equivalent of paginate, minus everything cursor/sort/preload-related that
// only matters once actual rows are being scanned back.
func (c *PaginationService[TData, TID]) count(
	ctx context.Context, db bun.IDB, filter database.StructuredFilter,
) (int64, error) {
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
