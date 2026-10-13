package cqrs

import (
	"context"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pagination"
	"github.com/uptrace/bun"
)

func (s *CQRSService[TData, TResponse, TRequest, TID]) Max(ctx context.Context, field string, filter pagination.StructuredFilter, preloads ...string) (*TData, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) MaxFormat(ctx context.Context, field string, filter pagination.StructuredFilter, preloads ...string) (*TResponse, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) MaxWithTx(ctx context.Context, tx *bun.Tx, field string, filter pagination.StructuredFilter, preloads ...string) (*TData, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) MaxWithTxFormat(ctx context.Context, tx *bun.Tx, field string, filter pagination.StructuredFilter, preloads ...string) (*TResponse, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) Min(ctx context.Context, field string, filter pagination.StructuredFilter, preloads ...string) (*TData, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) MinFormat(ctx context.Context, field string, filter pagination.StructuredFilter, preloads ...string) (*TResponse, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) MinWithTx(ctx context.Context, tx *bun.Tx, field string, filter pagination.StructuredFilter, preloads ...string) (*TData, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) MinWithTxFormat(ctx context.Context, tx *bun.Tx, field string, filter pagination.StructuredFilter, preloads ...string) (*TResponse, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) Filter(ctx context.Context, filter pagination.StructuredFilter) ([]*TData, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) FilterFormat(ctx context.Context, filter pagination.StructuredFilter) ([]*TResponse, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) FilterWithTx(ctx context.Context, tx *bun.Tx, filter pagination.StructuredFilter) ([]*TData, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) FilterWithTxFormat(ctx context.Context, tx *bun.Tx, filter pagination.StructuredFilter) ([]*TResponse, error) {
	panic("not implemented")
}
