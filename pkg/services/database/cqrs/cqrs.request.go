package cqrs

import (
	"context"
	"fmt"

	"github.com/uptrace/bun"
)

func (c *CQRSService[TData, TResponse, TRequest, TID]) dataFromRequest(
	ctx context.Context,
	request TRequest,
	index int,
) (TData, error) {
	var zero TData
	if c.FromRequest == nil {
		return zero, ErrFromRequestNotSet
	}
	if c.Validator != nil {
		if err := c.Validator.StructCtx(ctx, &request); err != nil {
			return zero, fmt.Errorf("validating request payload at index %d: %w", index, err)
		}
	}
	data := c.FromRequest(&request)
	if data == nil {
		return zero, fmt.Errorf("converting request payload at index %d: FromRequest returned nil", index)
	}
	return *data, nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) dataFromRequests(
	ctx context.Context,
	requests []TRequest,
) ([]TData, error) {
	out := make([]TData, len(requests))
	for i := range requests {
		d, err := c.dataFromRequest(ctx, requests[i], i)
		if err != nil {
			return nil, err
		}
		out[i] = d
	}
	return out, nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) CreateWithValidation(
	ctx context.Context,
	request TRequest,
	preload ...string,
) (*TData, error) {
	data, err := c.dataFromRequest(ctx, request, 0)
	if err != nil {
		return nil, err
	}
	return c.insertOne(ctx, c.WriteSQLService.Client(), data, preload)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) CreateWithValidationFormat(
	ctx context.Context,
	request TRequest,
	preload ...string,
) (*TResponse, error) {
	result, err := c.CreateWithValidation(ctx, request, preload...)
	if err != nil {
		return nil, err
	}
	return c.ToModel(result), nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) CreateWithValidationTx(
	ctx context.Context,
	tx bun.Tx,
	request TRequest,
	preload ...string,
) (*TData, error) {
	data, err := c.dataFromRequest(ctx, request, 0)
	if err != nil {
		return nil, err
	}
	return c.insertOne(ctx, tx, data, preload)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) CreateWithValidationTxFormat(
	ctx context.Context,
	tx bun.Tx,
	request TRequest,
	preload ...string,
) (*TResponse, error) {
	result, err := c.CreateWithValidationTx(ctx, tx, request, preload...)
	if err != nil {
		return nil, err
	}
	return c.ToModel(result), nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) CreateManyWithValidation(
	ctx context.Context,
	requests []TRequest,
	preload ...string,
) ([]*TData, error) {
	if len(requests) == 0 {
		return []*TData{}, nil
	}
	data, err := c.dataFromRequests(ctx, requests)
	if err != nil {
		return nil, err
	}
	return c.insertMany(ctx, c.WriteSQLService.Client(), data, preload)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) CreateManyWithValidationFormat(
	ctx context.Context,
	requests []TRequest,
	preload ...string,
) ([]*TResponse, error) {
	result, err := c.CreateManyWithValidation(ctx, requests, preload...)
	if err != nil {
		return nil, err
	}
	return c.ToModels(result), nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) CreateManyWithValidationTx(
	ctx context.Context,
	tx bun.Tx,
	requests []TRequest,
	preload ...string,
) ([]*TData, error) {
	if len(requests) == 0 {
		return []*TData{}, nil
	}
	data, err := c.dataFromRequests(ctx, requests)
	if err != nil {
		return nil, err
	}
	return c.insertMany(ctx, tx, data, preload)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) CreateManyWithValidationTxFormat(
	ctx context.Context,
	tx bun.Tx,
	requests []TRequest,
	preload ...string,
) ([]*TResponse, error) {
	result, err := c.CreateManyWithValidationTx(ctx, tx, requests, preload...)
	if err != nil {
		return nil, err
	}
	return c.ToModels(result), nil
}

// UpdateByIDWithValidation validates request, converts it via FromRequest,
// and updates the row matching id. The TData-based UpdateByID is unchanged.
func (c *CQRSService[TData, TResponse, TRequest, TID]) UpdateByIDWithValidation(
	ctx context.Context,
	id TID,
	request TRequest,
	preload ...string,
) (*TData, error) {
	data, err := c.dataFromRequest(ctx, request, 0)
	if err != nil {
		return nil, err
	}
	return c.updateOne(ctx, c.WriteSQLService.Client(), id, data, preload)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) UpdateByIDWithValidationFormat(
	ctx context.Context,
	id TID,
	request TRequest,
	preload ...string,
) (*TResponse, error) {
	result, err := c.UpdateByIDWithValidation(ctx, id, request, preload...)
	if err != nil {
		return nil, err
	}
	return c.ToModel(result), nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) UpdateByIDWithValidationTx(
	ctx context.Context,
	tx bun.Tx,
	id TID,
	request TRequest,
	preload ...string,
) (*TData, error) {
	data, err := c.dataFromRequest(ctx, request, 0)
	if err != nil {
		return nil, err
	}
	return c.updateOne(ctx, tx, id, data, preload)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) UpdateByIDWithValidationTxFormat(
	ctx context.Context,
	tx bun.Tx,
	id TID,
	request TRequest,
	preload ...string,
) (*TResponse, error) {
	result, err := c.UpdateByIDWithValidationTx(ctx, tx, id, request, preload...)
	if err != nil {
		return nil, err
	}
	return c.ToModel(result), nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) UpdateManyWithValidation(
	ctx context.Context,
	requests []TRequest,
	preload ...string,
) ([]*TData, error) {
	if len(requests) == 0 {
		return []*TData{}, nil
	}
	data, err := c.dataFromRequests(ctx, requests)
	if err != nil {
		return nil, err
	}
	return c.updateMany(ctx, c.WriteSQLService.Client(), data, preload)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) UpdateManyWithValidationFormat(
	ctx context.Context,
	requests []TRequest,
	preload ...string,
) ([]*TResponse, error) {
	result, err := c.UpdateManyWithValidation(ctx, requests, preload...)
	if err != nil {
		return nil, err
	}
	return c.ToModels(result), nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) UpdateManyWithValidationTx(
	ctx context.Context,
	tx bun.Tx,
	requests []TRequest,
	preload ...string,
) ([]*TData, error) {
	if len(requests) == 0 {
		return []*TData{}, nil
	}
	data, err := c.dataFromRequests(ctx, requests)
	if err != nil {
		return nil, err
	}
	return c.updateMany(ctx, tx, data, preload)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) UpdateManyWithValidationTxFormat(
	ctx context.Context,
	tx bun.Tx,
	requests []TRequest,
	preload ...string,
) ([]*TResponse, error) {
	result, err := c.UpdateManyWithValidationTx(ctx, tx, requests, preload...)
	if err != nil {
		return nil, err
	}
	return c.ToModels(result), nil
}
