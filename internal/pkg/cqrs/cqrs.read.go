package cqrs

import (
	"context"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pagination"
	"github.com/uptrace/bun"
)

func (s *CQRSService[TData, TResponse, TRequest, TID]) GetByID(ctx context.Context, id TID, preloads ...string) (*TData, error) {
	return s.FindOne(ctx, s.idFilter(id), preloads...)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) GetByIDFormat(ctx context.Context, id TID, preloads ...string) (*TResponse, error) {
	return s.FindOneFormat(ctx, s.idFilter(id), preloads...)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) GetByIDWithTx(ctx context.Context, tx *bun.Tx, id TID, preloads ...string) (*TData, error) {
	return s.FindOneWithTx(ctx, tx, s.idFilter(id), preloads...)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) GetByIDWithTxFormat(ctx context.Context, tx *bun.Tx, id TID, preloads ...string) (*TResponse, error) {
	return s.FindOneWithTxFormat(ctx, tx, s.idFilter(id), preloads...)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) Find(ctx context.Context, filter pagination.StructuredFilter, preloads ...string) ([]*TData, error) {
	return s.PaginationService.Find(ctx, filter, preloads...)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) FindFormat(ctx context.Context, filter pagination.StructuredFilter, preloads ...string) ([]*TResponse, error) {
	data, err := s.Find(ctx, filter, preloads...)
	if err != nil {
		return nil, err
	}
	return s.ToModels(data), nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) FindWithTx(ctx context.Context, tx *bun.Tx, filter pagination.StructuredFilter, preloads ...string) ([]*TData, error) {
	return s.PaginationService.FindWithTx(ctx, tx, filter, preloads...)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) FindWithTxFormat(ctx context.Context, tx *bun.Tx, filter pagination.StructuredFilter, preloads ...string) ([]*TResponse, error) {
	data, err := s.FindWithTx(ctx, tx, filter, preloads...)
	if err != nil {
		return nil, err
	}
	return s.ToModels(data), nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) FindOne(ctx context.Context, filter pagination.StructuredFilter, preloads ...string) (*TData, error) {
	return s.PaginationService.FindOne(ctx, filter, preloads...)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) FindOneFormat(ctx context.Context, filter pagination.StructuredFilter, preloads ...string) (*TResponse, error) {
	data, err := s.FindOne(ctx, filter, preloads...)
	if err != nil {
		return nil, err
	}
	return s.ToModel(data), nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) FindOneWithTx(ctx context.Context, tx *bun.Tx, filter pagination.StructuredFilter, preloads ...string) (*TData, error) {
	return s.PaginationService.FindOneWithTx(ctx, tx, filter, preloads...)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) FindOneWithTxFormat(ctx context.Context, tx *bun.Tx, filter pagination.StructuredFilter, preloads ...string) (*TResponse, error) {
	data, err := s.FindOneWithTx(ctx, tx, filter, preloads...)
	if err != nil {
		return nil, err
	}
	return s.ToModel(data), nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) Count(ctx context.Context, filter pagination.StructuredFilter) (int64, error) {
	return s.PaginationService.Count(ctx, filter)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) CountWithTx(ctx context.Context, tx *bun.Tx, filter pagination.StructuredFilter) (int64, error) {
	return s.PaginationService.CountWithTx(ctx, tx, filter)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) Exists(ctx context.Context, filter pagination.StructuredFilter) (bool, error) {
	return s.PaginationService.Exists(ctx, filter)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) ExistsWithTx(ctx context.Context, tx *bun.Tx, filter pagination.StructuredFilter) (bool, error) {
	return s.PaginationService.ExistsWithTx(ctx, tx, filter)
}
