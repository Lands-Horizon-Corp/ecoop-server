package pagination

import (
	"context"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/uptrace/bun"
)

// Find returns every row in TData's table matching filter — the multi-row
// counterpart to FindOne, which returns only the first match. It's built
// directly on paginate (a zero-value database.Pagination: default page
// size, no cursor) the same way Filter is, but — unlike Filter — forwards
// an optional preloads override, the same way FindOne/Count/Exists do.
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

// FindWithTx is Find run against a caller-supplied *bun.Tx instead of a
// plain client. That tx is expected to come from WriteSQLService (the
// writer), not ReadSQLService — see FilterWithTx's doc comment in
// pagination.service.go for why — e.g. finding rows written earlier in the
// same transaction, before it commits and becomes visible through a
// separate connection. Every matched row is locked ("SELECT ... FOR
// UPDATE" — see paginate's own doc comment for why every *WithTx fetch
// does this).
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
