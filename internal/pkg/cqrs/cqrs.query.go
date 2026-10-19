package cqrs

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pagination"
)

func (s *CQRSService[TData, TResponse, TRequest, TID]) Paginate(ctx context.Context, page pagination.Pagination) (pagination.PaginationResult[TData], error) {
	return s.PaginationService.Paginate(ctx, page)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) PaginateFormat(ctx context.Context, page pagination.Pagination) (pagination.PaginationResult[TResponse], error) {
	paginationResult, err := s.PaginationService.Paginate(ctx, page)
	if err != nil {
		return pagination.PaginationResult[TResponse]{}, err
	}
	return pagination.PaginationResult[TResponse]{
		Data:           s.ToModels(paginationResult.Data),
		CurrentCursor:  paginationResult.CurrentCursor,
		NextCursor:     paginationResult.NextCursor,
		PreviousCursor: paginationResult.PreviousCursor,
		PageSize:       paginationResult.PageSize,
	}, nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) PaginateFilter(ctx context.Context, filter pagination.StructuredFilter, page pagination.Pagination) (pagination.PaginationResult[TData], error) {
	return s.PaginationService.PaginateFilter(ctx, filter, page)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) PaginateFilterFormat(ctx context.Context, filter pagination.StructuredFilter, page pagination.Pagination) (pagination.PaginationResult[TResponse], error) {
	paginationResult, err := s.PaginationService.PaginateFilter(ctx, filter, page)
	if err != nil {
		return pagination.PaginationResult[TResponse]{}, err
	}
	return pagination.PaginationResult[TResponse]{
		Data:           s.ToModels(paginationResult.Data),
		CurrentCursor:  paginationResult.CurrentCursor,
		NextCursor:     paginationResult.NextCursor,
		PreviousCursor: paginationResult.PreviousCursor,
		PageSize:       paginationResult.PageSize,
	}, nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) PaginateWithHertz(ctx context.Context, tx *bun.Tx, filter pagination.StructuredFilter, reqCtx *app.RequestContext) (pagination.PaginationResult[TData], error) {
	return s.PaginationService.PaginateWithHertz(ctx, tx, filter, reqCtx)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) PaginateWithHertzFormat(ctx context.Context, tx *bun.Tx, filter pagination.StructuredFilter, reqCtx *app.RequestContext) (pagination.PaginationResult[TResponse], error) {
	paginationResult, err := s.PaginationService.PaginateWithHertz(ctx, tx, filter, reqCtx)
	if err != nil {
		return pagination.PaginationResult[TResponse]{}, err
	}
	return pagination.PaginationResult[TResponse]{
		Data:           s.ToModels(paginationResult.Data),
		CurrentCursor:  paginationResult.CurrentCursor,
		NextCursor:     paginationResult.NextCursor,
		PreviousCursor: paginationResult.PreviousCursor,
		PageSize:       paginationResult.PageSize,
	}, nil
}
