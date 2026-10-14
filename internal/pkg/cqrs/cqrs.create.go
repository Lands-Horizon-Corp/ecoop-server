package cqrs

import (
	"context"
	"fmt"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
	"github.com/uptrace/bun"
)

func (s *CQRSService[TData, TResponse, TRequest, TID]) Create(ctx context.Context, data TData, preload ...string) (*TData, error) {
	if s.Validator != nil {
		if err := s.Validator.StructCtx(ctx, &data); err != nil {
			return nil, fmt.Errorf("validating request payload: %w", err)
		}
	}
	db, err := s.writeDB()
	if err != nil {
		return nil, err
	}
	return utils.Scoped(ctx, db, func(q bun.IDB) (*TData, error) {
		return s.insertOne(ctx, q, data, preload)
	})
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) CreateFormat(ctx context.Context, data TData, preload ...string) (*TResponse, error) {
	result, err := s.Create(ctx, data, preload...)
	if err != nil {
		return nil, err
	}
	return s.ToModel(result), nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) CreateMany(ctx context.Context, data []TData, preload ...string) ([]*TData, error) {
	if len(data) == 0 {
		return []*TData{}, nil
	}

	if s.Validator != nil {
		for i := range data {
			if err := s.Validator.StructCtx(ctx, &data[i]); err != nil {
				return nil, fmt.Errorf("validating request payload at index %d: %w", i, err)
			}
		}
	}
	db, err := s.writeDB()
	if err != nil {
		return nil, err
	}
	return utils.Scoped(ctx, db, func(q bun.IDB) ([]*TData, error) {
		return s.insertMany(ctx, q, data, preload)
	})
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) CreateManyFormat(ctx context.Context, data []TData, preload ...string) ([]*TResponse, error) {
	result, err := s.CreateMany(ctx, data, preload...)
	if err != nil {
		return nil, err
	}
	return s.ToModels(result), nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) CreateWithTx(ctx context.Context, tx bun.Tx, data TData, preload ...string) (*TData, error) {
	if s.Validator != nil {
		if err := s.Validator.StructCtx(ctx, &data); err != nil {
			return nil, fmt.Errorf("validating request payload: %w", err)
		}
	}
	return s.insertOne(ctx, tx, data, preload)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) CreateWithTxFormat(ctx context.Context, tx bun.Tx, data TData, preload ...string) (*TResponse, error) {
	result, err := s.CreateWithTx(ctx, tx, data, preload...)
	if err != nil {
		return nil, err
	}
	return s.ToModel(result), nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) CreateManyWithTx(ctx context.Context, tx bun.Tx, data []TData, preload ...string) ([]*TData, error) {
	if len(data) == 0 {
		return []*TData{}, nil
	}
	if s.Validator != nil {
		for i := range data {
			if err := s.Validator.StructCtx(ctx, &data[i]); err != nil {
				return nil, fmt.Errorf("validating request payload at index %d: %w", i, err)
			}
		}
	}
	return s.insertMany(ctx, tx, data, preload)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) CreateManyWithTxFormat(ctx context.Context, tx bun.Tx, data []TData, preload ...string) ([]*TResponse, error) {
	result, err := s.CreateManyWithTx(ctx, tx, data, preload...)
	if err != nil {
		return nil, err
	}
	if s.ToResource == nil {
		return nil, nil
	}
	return s.ToModels(result), nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) CreateWithValidation(ctx context.Context, request TRequest, preload ...string) (*TData, error) {
	data, err := s.dataFromRequest(ctx, request, 0)
	if err != nil {
		return nil, err
	}
	return s.insertOne(ctx, s.WriteSQLService.Client(), data, preload)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) CreateWithValidationFormat(ctx context.Context, request TRequest, preload ...string) (*TResponse, error) {
	result, err := s.CreateWithValidation(ctx, request, preload...)
	if err != nil {
		return nil, err
	}
	return s.ToModel(result), nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) CreateManyWithValidation(ctx context.Context, requests []TRequest, preload ...string) ([]*TData, error) {
	if len(requests) == 0 {
		return []*TData{}, nil
	}
	data, err := s.dataFromRequests(ctx, requests)
	if err != nil {
		return nil, err
	}
	return s.insertMany(ctx, s.WriteSQLService.Client(), data, preload)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) CreateManyWithValidationFormat(ctx context.Context, requests []TRequest, preload ...string) ([]*TResponse, error) {
	result, err := s.CreateManyWithValidation(ctx, requests, preload...)
	if err != nil {
		return nil, err
	}
	return s.ToModels(result), nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) CreateWithValidationTx(ctx context.Context, tx bun.Tx, request TRequest, preload ...string) (*TData, error) {
	data, err := s.dataFromRequest(ctx, request, 0)
	if err != nil {
		return nil, err
	}
	return s.insertOne(ctx, tx, data, preload)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) CreateWithValidationTxFormat(ctx context.Context, tx bun.Tx, request TRequest, preload ...string) (*TResponse, error) {
	result, err := s.CreateWithValidationTx(ctx, tx, request, preload...)
	if err != nil {
		return nil, err
	}
	return s.ToModel(result), nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) CreateManyWithValidationTx(ctx context.Context, tx bun.Tx, requests []TRequest, preload ...string) ([]*TData, error) {
	if len(requests) == 0 {
		return []*TData{}, nil
	}
	data, err := s.dataFromRequests(ctx, requests)
	if err != nil {
		return nil, err
	}
	return s.insertMany(ctx, tx, data, preload)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) CreateManyWithValidationTxFormat(ctx context.Context, tx bun.Tx, requests []TRequest, preload ...string) ([]*TResponse, error) {
	result, err := s.CreateManyWithValidationTx(ctx, tx, requests, preload...)
	if err != nil {
		return nil, err
	}
	return s.ToModels(result), nil
}
