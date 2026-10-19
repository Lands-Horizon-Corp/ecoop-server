package cqrs

import (
	"context"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pagination"
)

func (s *CQRSService[TData, TResponse, TRequest, TID]) Max(ctx context.Context, field string, filter pagination.StructuredFilter, preloads ...string) (*TData, error) {
	return s.PaginationService.GetMax(ctx, field, filter, preloads...)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) MaxFormat(ctx context.Context, field string, filter pagination.StructuredFilter, preloads ...string) (*TResponse, error) {
	result, err := s.Max(ctx, field, filter, preloads...)
	if err != nil {
		return nil, err
	}

	return s.ToModel(result), nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) MaxWithTx(ctx context.Context, tx *bun.Tx, field string, filter pagination.StructuredFilter, preloads ...string) (*TData, error) {
	return s.PaginationService.GetMaxWithTx(ctx, tx, field, filter, preloads...)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) MaxWithTxFormat(ctx context.Context, tx *bun.Tx, field string, filter pagination.StructuredFilter, preloads ...string) (*TResponse, error) {
	result, err := s.MaxWithTx(ctx, tx, field, filter, preloads...)
	if err != nil {
		return nil, err
	}
	return s.ToModel(result), nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) Min(ctx context.Context, field string, filter pagination.StructuredFilter, preloads ...string) (*TData, error) {
	return s.PaginationService.GetMin(ctx, field, filter, preloads...)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) MinFormat(ctx context.Context, field string, filter pagination.StructuredFilter, preloads ...string) (*TResponse, error) {
	result, err := s.Min(ctx, field, filter, preloads...)
	if err != nil {
		return nil, err
	}
	return s.ToModel(result), nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) MinWithTx(ctx context.Context, tx *bun.Tx, field string, filter pagination.StructuredFilter, preloads ...string) (*TData, error) {
	return s.PaginationService.GetMinWithTx(ctx, tx, field, filter, preloads...)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) MinWithTxFormat(ctx context.Context, tx *bun.Tx, field string, filter pagination.StructuredFilter, preloads ...string) (*TResponse, error) {
	result, err := s.MinWithTx(ctx, tx, field, filter, preloads...)
	if err != nil {
		return nil, err
	}
	return s.ToModel(result), nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) Filter(ctx context.Context, filter pagination.StructuredFilter) ([]*TData, error) {
	return s.PaginationService.Filter(ctx, filter)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) FilterFormat(ctx context.Context, filter pagination.StructuredFilter) ([]*TResponse, error) {
	data, err := s.PaginationService.Filter(ctx, filter)
	if err != nil {
		return nil, err
	}
	return s.ToModels(data), nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) FilterWithTx(ctx context.Context, tx *bun.Tx, filter pagination.StructuredFilter) ([]*TData, error) {
	return s.PaginationService.FilterWithTx(ctx, tx, filter)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) FilterWithTxFormat(ctx context.Context, tx *bun.Tx, filter pagination.StructuredFilter) ([]*TResponse, error) {
	data, err := s.PaginationService.FilterWithTx(ctx, tx, filter)
	if err != nil {
		return nil, err
	}
	return s.ToModels(data), nil
}
