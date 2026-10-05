package cqrs

import (
	"context"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/pagination"
	"github.com/uptrace/bun"
)

func (c *CQRSService[TData, TResponse, TRequest, TID]) Find(
	ctx context.Context, filter pagination.StructuredFilter, preloads ...string,
) ([]*TData, error) {
	return c.PaginationService.Find(ctx, filter, preloads...)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) FindFormat(
	ctx context.Context, filter pagination.StructuredFilter, preloads ...string,
) ([]*TResponse, error) {
	data, err := c.Find(ctx, filter, preloads...)
	if err != nil {
		return nil, err
	}
	return c.ToModels(data), nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) FindWithTx(
	ctx context.Context, tx *bun.Tx, filter pagination.StructuredFilter, preloads ...string,
) ([]*TData, error) {
	return c.PaginationService.FindWithTx(ctx, tx, filter, preloads...)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) FindWithTxFormat(
	ctx context.Context, tx *bun.Tx, filter pagination.StructuredFilter, preloads ...string,
) ([]*TResponse, error) {
	data, err := c.FindWithTx(ctx, tx, filter, preloads...)
	if err != nil {
		return nil, err
	}
	return c.ToModels(data), nil
}
