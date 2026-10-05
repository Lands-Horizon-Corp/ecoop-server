package cqrs

import (
	"context"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/uptrace/bun"
)

func (c *CQRSService[TData, TResponse, TRequest, TID]) Paginate(
	ctx context.Context, pagination database.Pagination) (database.PaginationResult[TData], error) {
	return c.PaginationService.Paginate(ctx, pagination)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) PaginateFormat(
	ctx context.Context, pagination database.Pagination) (database.PaginationResult[TResponse], error) {
	paginationResult, err := c.PaginationService.Paginate(ctx, pagination)
	if err != nil {
		return database.PaginationResult[TResponse]{}, err
	}
	return database.PaginationResult[TResponse]{
		Data:           c.ToModels(paginationResult.Data),
		CurrentCursor:  paginationResult.CurrentCursor,
		NextCursor:     paginationResult.NextCursor,
		PreviousCursor: paginationResult.PreviousCursor,
		PageSize:       paginationResult.PageSize,
	}, nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) PaginateFilter(
	ctx context.Context, filter database.StructuredFilter, pagination database.Pagination) (database.PaginationResult[TData], error) {
	return c.PaginationService.PaginateFilter(ctx, filter, pagination)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) PaginateFilterFormat(
	ctx context.Context, filter database.StructuredFilter, pagination database.Pagination) (database.PaginationResult[TResponse], error) {
	paginationResult, err := c.PaginationService.PaginateFilter(ctx, filter, pagination)
	if err != nil {
		return database.PaginationResult[TResponse]{}, err
	}
	return database.PaginationResult[TResponse]{
		Data:           c.ToModels(paginationResult.Data),
		CurrentCursor:  paginationResult.CurrentCursor,
		NextCursor:     paginationResult.NextCursor,
		PreviousCursor: paginationResult.PreviousCursor,
		PageSize:       paginationResult.PageSize,
	}, nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) Filter(
	ctx context.Context, filter database.StructuredFilter) ([]*TData, error) {
	return c.PaginationService.Filter(ctx, filter)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) FilterFormat(
	ctx context.Context, filter database.StructuredFilter) ([]*TResponse, error) {
	data, err := c.PaginationService.Filter(ctx, filter)
	if err != nil {
		return nil, err
	}
	return c.ToModels(data), nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) FilterWithTx(
	ctx context.Context, tx *bun.Tx, filter database.StructuredFilter) ([]*TData, error) {
	return c.PaginationService.FilterWithTx(ctx, tx, filter)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) FilterWithTxFormat(
	ctx context.Context, tx *bun.Tx, filter database.StructuredFilter) ([]*TResponse, error) {
	data, err := c.PaginationService.FilterWithTx(ctx, tx, filter)
	if err != nil {
		return nil, err
	}
	return c.ToModels(data), nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) PaginateWithHertz(
	ctx context.Context, tx *bun.Tx, filter database.StructuredFilter, reqCtx *app.RequestContext) (database.PaginationResult[TData], error) {
	return c.PaginationService.PaginateWithHertz(ctx, tx, filter, reqCtx)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) PaginateWithHertzFormat(
	ctx context.Context, tx *bun.Tx, filter database.StructuredFilter, reqCtx *app.RequestContext) (database.PaginationResult[TResponse], error) {
	paginationResult, err := c.PaginationService.PaginateWithHertz(ctx, tx, filter, reqCtx)
	if err != nil {
		return database.PaginationResult[TResponse]{}, err
	}
	return database.PaginationResult[TResponse]{
		Data:           c.ToModels(paginationResult.Data),
		CurrentCursor:  paginationResult.CurrentCursor,
		NextCursor:     paginationResult.NextCursor,
		PreviousCursor: paginationResult.PreviousCursor,
		PageSize:       paginationResult.PageSize,
	}, nil
}
