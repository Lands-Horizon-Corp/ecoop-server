package cqrs

import (
	"context"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/uptrace/bun"
)

func (c *CQRSService[TData, TResponse, TRequest, TID]) FindOne(
	ctx context.Context, filter database.StructuredFilter, preloads ...string,
) (*TData, error) {
	return c.PaginationService.FindOne(ctx, filter, preloads...)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) FindOneFormat(
	ctx context.Context, filter database.StructuredFilter, preloads ...string,
) (*TResponse, error) {
	result, err := c.FindOne(ctx, filter, preloads...)
	if err != nil {
		return nil, err
	}
	if c.ToResource == nil {
		return nil, nil
	}
	return c.ToResource(result), nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) FindOneWithTx(
	ctx context.Context, tx *bun.Tx, filter database.StructuredFilter, preloads ...string,
) (*TData, error) {
	return c.PaginationService.FindOneWithTx(ctx, tx, filter, preloads...)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) FindOneWithTxFormat(
	ctx context.Context, tx *bun.Tx, filter database.StructuredFilter, preloads ...string,
) (*TResponse, error) {
	result, err := c.FindOneWithTx(ctx, tx, filter, preloads...)
	if err != nil {
		return nil, err
	}
	if c.ToResource == nil {
		return nil, nil
	}
	return c.ToResource(result), nil
}
