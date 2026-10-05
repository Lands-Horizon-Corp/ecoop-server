package cqrs

import (
	"context"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/pagination"
	"github.com/uptrace/bun"
)

func (c *CQRSService[TData, TResponse, TRequest, TID]) Max(
	ctx context.Context, field string, filter pagination.StructuredFilter, preloads ...string,
) (*TData, error) {
	return c.PaginationService.GetMax(ctx, field, filter, preloads...)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) MaxFormat(
	ctx context.Context, field string, filter pagination.StructuredFilter, preloads ...string,
) (*TResponse, error) {
	result, err := c.Max(ctx, field, filter, preloads...)
	if err != nil {
		return nil, err
	}
	if c.ToResource == nil {
		return nil, nil
	}
	return c.ToResource(result), nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) Min(
	ctx context.Context, field string, filter pagination.StructuredFilter, preloads ...string,
) (*TData, error) {
	return c.PaginationService.GetMin(ctx, field, filter, preloads...)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) MinFormat(
	ctx context.Context, field string, filter pagination.StructuredFilter, preloads ...string,
) (*TResponse, error) {
	result, err := c.Min(ctx, field, filter, preloads...)
	if err != nil {
		return nil, err
	}
	if c.ToResource == nil {
		return nil, nil
	}
	return c.ToResource(result), nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) MaxWithTx(
	ctx context.Context, tx *bun.Tx, field string, filter pagination.StructuredFilter, preloads ...string,
) (*TData, error) {
	return c.PaginationService.GetMaxWithTx(ctx, tx, field, filter, preloads...)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) MaxWithTxFormat(
	ctx context.Context, tx *bun.Tx, field string, filter pagination.StructuredFilter, preloads ...string,
) (*TResponse, error) {
	result, err := c.MaxWithTx(ctx, tx, field, filter, preloads...)
	if err != nil {
		return nil, err
	}
	if c.ToResource == nil {
		return nil, nil
	}
	return c.ToResource(result), nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) MinWithTx(
	ctx context.Context, tx *bun.Tx, field string, filter pagination.StructuredFilter, preloads ...string,
) (*TData, error) {
	return c.PaginationService.GetMinWithTx(ctx, tx, field, filter, preloads...)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) MinWithTxFormat(
	ctx context.Context, tx *bun.Tx, field string, filter pagination.StructuredFilter, preloads ...string,
) (*TResponse, error) {
	result, err := c.MinWithTx(ctx, tx, field, filter, preloads...)
	if err != nil {
		return nil, err
	}
	if c.ToResource == nil {
		return nil, nil
	}
	return c.ToResource(result), nil
}
