package cqrs

import (
	"context"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/pagination"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/uptrace/bun"
)

func (c *CQRSService[TData, TResponse, TRequest, TID]) Paginate(
	ctx context.Context, page pagination.Pagination) (pagination.PaginationResult[TData], error) {
	return c.PaginationService.Paginate(ctx, page)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) PaginateFormat(
	ctx context.Context, page pagination.Pagination) (pagination.PaginationResult[TResponse], error) {
	paginationResult, err := c.PaginationService.Paginate(ctx, page)
	if err != nil {
		return pagination.PaginationResult[TResponse]{}, err
	}
	return pagination.PaginationResult[TResponse]{
		Data:           c.ToModels(paginationResult.Data),
		CurrentCursor:  paginationResult.CurrentCursor,
		NextCursor:     paginationResult.NextCursor,
		PreviousCursor: paginationResult.PreviousCursor,
		PageSize:       paginationResult.PageSize,
	}, nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) PaginateFilter(
	ctx context.Context, filter pagination.StructuredFilter, page pagination.Pagination) (pagination.PaginationResult[TData], error) {
	return c.PaginationService.PaginateFilter(ctx, filter, page)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) PaginateFilterFormat(
	ctx context.Context, filter pagination.StructuredFilter, page pagination.Pagination) (pagination.PaginationResult[TResponse], error) {
	paginationResult, err := c.PaginationService.PaginateFilter(ctx, filter, page)
	if err != nil {
		return pagination.PaginationResult[TResponse]{}, err
	}
	return pagination.PaginationResult[TResponse]{
		Data:           c.ToModels(paginationResult.Data),
		CurrentCursor:  paginationResult.CurrentCursor,
		NextCursor:     paginationResult.NextCursor,
		PreviousCursor: paginationResult.PreviousCursor,
		PageSize:       paginationResult.PageSize,
	}, nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) Filter(
	ctx context.Context, filter pagination.StructuredFilter) ([]*TData, error) {
	return c.PaginationService.Filter(ctx, filter)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) FilterFormat(
	ctx context.Context, filter pagination.StructuredFilter) ([]*TResponse, error) {
	data, err := c.PaginationService.Filter(ctx, filter)
	if err != nil {
		return nil, err
	}
	return c.ToModels(data), nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) FilterWithTx(
	ctx context.Context, tx *bun.Tx, filter pagination.StructuredFilter) ([]*TData, error) {
	return c.PaginationService.FilterWithTx(ctx, tx, filter)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) FilterWithTxFormat(
	ctx context.Context, tx *bun.Tx, filter pagination.StructuredFilter) ([]*TResponse, error) {
	data, err := c.PaginationService.FilterWithTx(ctx, tx, filter)
	if err != nil {
		return nil, err
	}
	return c.ToModels(data), nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) PaginateWithHertz(
	ctx context.Context, tx *bun.Tx, filter pagination.StructuredFilter, reqCtx *app.RequestContext) (pagination.PaginationResult[TData], error) {
	return c.PaginationService.PaginateWithHertz(ctx, tx, filter, reqCtx)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) PaginateWithHertzFormat(
	ctx context.Context, tx *bun.Tx, filter pagination.StructuredFilter, reqCtx *app.RequestContext) (pagination.PaginationResult[TResponse], error) {
	paginationResult, err := c.PaginationService.PaginateWithHertz(ctx, tx, filter, reqCtx)
	if err != nil {
		return pagination.PaginationResult[TResponse]{}, err
	}
	return pagination.PaginationResult[TResponse]{
		Data:           c.ToModels(paginationResult.Data),
		CurrentCursor:  paginationResult.CurrentCursor,
		NextCursor:     paginationResult.NextCursor,
		PreviousCursor: paginationResult.PreviousCursor,
		PageSize:       paginationResult.PageSize,
	}, nil
}
