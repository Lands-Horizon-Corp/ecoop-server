package cqrs

import (
	"context"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pagination"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/uptrace/bun"
)

func (s *CQRSService[TData, TResponse, TRequest, TID]) Paginate(ctx context.Context, page pagination.Pagination) (pagination.PaginationResult[TData], error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) PaginateFormat(ctx context.Context, page pagination.Pagination) (pagination.PaginationResult[TResponse], error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) PaginateFilter(ctx context.Context, filter pagination.StructuredFilter, page pagination.Pagination) (pagination.PaginationResult[TData], error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) PaginateFilterFormat(ctx context.Context, filter pagination.StructuredFilter, page pagination.Pagination) (pagination.PaginationResult[TResponse], error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) PaginateWithHertz(ctx context.Context, tx *bun.Tx, filter pagination.StructuredFilter, reqCtx *app.RequestContext) (pagination.PaginationResult[TData], error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) PaginateWithHertzFormat(ctx context.Context, tx *bun.Tx, filter pagination.StructuredFilter, reqCtx *app.RequestContext) (pagination.PaginationResult[TResponse], error) {
	panic("not implemented")
}
