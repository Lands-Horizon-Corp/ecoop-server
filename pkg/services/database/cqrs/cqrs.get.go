package cqrs

import (
	"context"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/uptrace/bun"
)

func (c *CQRSService[TData, TResponse, TRequest, TID]) GetByID(
	ctx context.Context, id TID, preloads ...string,
) (*TData, error) {
	return c.FindOne(ctx, c.idFilter(id), preloads...)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) GetByIDFormat(
	ctx context.Context, id TID, preloads ...string,
) (*TResponse, error) {
	return c.FindOneFormat(ctx, c.idFilter(id), preloads...)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) GetByIDWithTx(
	ctx context.Context, tx *bun.Tx, id TID, preloads ...string,
) (*TData, error) {
	return c.FindOneWithTx(ctx, tx, c.idFilter(id), preloads...)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) GetByIDWithTxFormat(
	ctx context.Context, tx *bun.Tx, id TID, preloads ...string,
) (*TResponse, error) {
	return c.FindOneWithTxFormat(ctx, tx, c.idFilter(id), preloads...)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) idFilter(id TID) database.StructuredFilter {
	return database.StructuredFilter{
		Filters: []database.Filter{{Field: c.ColumnDefaultID, Mode: database.ModeEqual, Value: id}},
	}
}
