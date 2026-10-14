package cqrs

import (
	"context"
	"fmt"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
	"github.com/uptrace/bun"
)

func (s *CQRSService[TData, TResponse, TRequest, TID]) UpdateByID(ctx context.Context, id TID, data TData, preload ...string) (*TData, error) {
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
		return s.updateOne(ctx, q, id, data, preload)
	})
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) UpdateByIDFormat(ctx context.Context, id TID, data TData, preload ...string) (*TResponse, error) {
	result, err := s.UpdateByID(ctx, id, data, preload...)
	if err != nil {
		return nil, err
	}
	if s.ToResource == nil {
		return nil, nil
	}
	return s.ToModel(result), nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) UpdateByIDWithTx(ctx context.Context, tx bun.Tx, id TID, data TData, preload ...string) (*TData, error) {
	if s.Validator != nil {
		if err := s.Validator.StructCtx(ctx, &data); err != nil {
			return nil, fmt.Errorf("validating request payload: %w", err)
		}
	}
	return s.updateOne(ctx, tx, id, data, preload)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) UpdateByIDWithTxFormat(ctx context.Context, tx bun.Tx, id TID, data TData, preload ...string) (*TResponse, error) {
	result, err := s.UpdateByIDWithTx(ctx, tx, id, data, preload...)
	if err != nil {
		return nil, err
	}
	if s.ToResource == nil {
		return nil, nil
	}
	return s.ToModel(result), nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) UpdateMany(ctx context.Context, data []TData, preload ...string) ([]*TData, error) {
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
		return s.updateMany(ctx, q, data, preload)
	})
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) UpdateManyFormat(ctx context.Context, data []TData, preload ...string) ([]*TResponse, error) {
	result, err := s.UpdateMany(ctx, data, preload...)
	if err != nil {
		return nil, err
	}
	if s.ToResource == nil {
		return nil, nil
	}
	responses := make([]*TResponse, 0, len(result))
	for _, d := range result {
		if res := s.ToModel(d); res != nil {
			responses = append(responses, res)
		}
	}
	return responses, nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) UpdateManyWithTx(ctx context.Context, tx bun.Tx, data []TData, preload ...string) ([]*TData, error) {
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

	return s.updateMany(ctx, tx, data, preload)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) UpdateManyWithTxFormat(ctx context.Context, tx bun.Tx, data []TData, preload ...string) ([]*TResponse, error) {
	result, err := s.UpdateManyWithTx(ctx, tx, data, preload...)
	if err != nil {
		return nil, err
	}
	if s.ToResource == nil {
		return nil, nil
	}
	responses := make([]*TResponse, 0, len(result))
	for _, d := range result {
		if res := s.ToModel(d); res != nil {
			responses = append(responses, res)
		}
	}
	return responses, nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) UpdateByIDWithValidation(ctx context.Context, id TID, request TRequest, preload ...string) (*TData, error) {
	data, err := s.dataFromRequest(ctx, request, 0)
	if err != nil {
		return nil, err
	}
	return s.updateOne(ctx, s.WriteSQLService.Client(), id, data, preload)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) UpdateByIDWithValidationFormat(ctx context.Context, id TID, request TRequest, preload ...string) (*TResponse, error) {
	result, err := s.UpdateByIDWithValidation(ctx, id, request, preload...)
	if err != nil {
		return nil, err
	}
	return s.ToModel(result), nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) UpdateByIDWithValidationTx(ctx context.Context, tx bun.Tx, id TID, request TRequest, preload ...string) (*TData, error) {
	data, err := s.dataFromRequest(ctx, request, 0)
	if err != nil {
		return nil, err
	}
	return s.updateOne(ctx, tx, id, data, preload)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) UpdateByIDWithValidationTxFormat(ctx context.Context, tx bun.Tx, id TID, request TRequest, preload ...string) (*TResponse, error) {
	result, err := s.UpdateByIDWithValidationTx(ctx, tx, id, request, preload...)
	if err != nil {
		return nil, err
	}
	return s.ToModel(result), nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) UpdateManyWithValidation(ctx context.Context, requests []TRequest, preload ...string) ([]*TData, error) {
	if len(requests) == 0 {
		return []*TData{}, nil
	}
	data, err := s.dataFromRequests(ctx, requests)
	if err != nil {
		return nil, err
	}
	return s.updateMany(ctx, s.WriteSQLService.Client(), data, preload)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) UpdateManyWithValidationFormat(ctx context.Context, requests []TRequest, preload ...string) ([]*TResponse, error) {
	result, err := s.UpdateManyWithValidation(ctx, requests, preload...)
	if err != nil {
		return nil, err
	}
	return s.ToModels(result), nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) UpdateManyWithValidationTx(ctx context.Context, tx bun.Tx, requests []TRequest, preload ...string) ([]*TData, error) {
	if len(requests) == 0 {
		return []*TData{}, nil
	}
	data, err := s.dataFromRequests(ctx, requests)
	if err != nil {
		return nil, err
	}
	return s.updateMany(ctx, tx, data, preload)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) UpdateManyWithValidationTxFormat(ctx context.Context, tx bun.Tx, requests []TRequest, preload ...string) ([]*TResponse, error) {
	result, err := s.UpdateManyWithValidationTx(ctx, tx, requests, preload...)
	if err != nil {
		return nil, err
	}
	return s.ToModels(result), nil
}
