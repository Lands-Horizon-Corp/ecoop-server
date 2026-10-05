package cqrs

import (
	"context"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/uptrace/bun"
)

// Find returns every row in TData's table matching filter — the multi-row
// counterpart to FindOne, which returns only the first match.
func (c *CQRSService[TData, TResponse, TRequest, TID]) Find(
	ctx context.Context, filter database.StructuredFilter, preloads ...string,
) ([]*TData, error) {
	return c.PaginationService.Find(ctx, filter, preloads...)
}

// FindFormat is Find with each matched row converted through ToResource,
// for callers that want the TResponse-shaped view instead of TData itself.
func (c *CQRSService[TData, TResponse, TRequest, TID]) FindFormat(
	ctx context.Context, filter database.StructuredFilter, preloads ...string,
) ([]*TResponse, error) {
	data, err := c.Find(ctx, filter, preloads...)
	if err != nil {
		return nil, err
	}
	return c.ToModels(data), nil
}

// FindWithTx is Find run against a caller-supplied *bun.Tx instead of a
// plain client. That tx is expected to come from WriteSQLService (the
// writer), not ReadSQLService — a transaction only shows its own
// uncommitted work to callers sharing that same connection, and
// ReadSQLService may point at a replica that doesn't even share it — e.g.
// finding rows written earlier in the same transaction, before it commits
// and becomes visible through a separate connection.
func (c *CQRSService[TData, TResponse, TRequest, TID]) FindWithTx(
	ctx context.Context, tx *bun.Tx, filter database.StructuredFilter, preloads ...string,
) ([]*TData, error) {
	return c.PaginationService.FindWithTx(ctx, tx, filter, preloads...)
}

// FindWithTxFormat is FindWithTx with each matched row converted through
// ToResource, for callers that want the TResponse-shaped view instead of
// TData itself.
func (c *CQRSService[TData, TResponse, TRequest, TID]) FindWithTxFormat(
	ctx context.Context, tx *bun.Tx, filter database.StructuredFilter, preloads ...string,
) ([]*TResponse, error) {
	data, err := c.FindWithTx(ctx, tx, filter, preloads...)
	if err != nil {
		return nil, err
	}
	return c.ToModels(data), nil
}
