package cqrs

import (
	"context"
	"fmt"

	"github.com/uptrace/bun"
)

func (c *CQRSService[TData, TResponse, TRequest, TID]) Create(
	ctx context.Context,
	data TData,
	preload ...string,
) (*TData, error) {
	if c.Validator != nil {
		if err := c.Validator.StructCtx(ctx, &data); err != nil {
			return nil, fmt.Errorf("validating request payload: %w", err)
		}
	}
	return c.insertOne(ctx, c.WriteSQLService.Client(), data, preload)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) CreateFormat(
	ctx context.Context,
	data TData,
	preload ...string,
) (*TResponse, error) {
	result, err := c.Create(ctx, data, preload...)
	if err != nil {
		return nil, err
	}
	return c.ToModel(result), nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) CreateMany(
	ctx context.Context,
	data []TData,
	preload ...string,
) ([]*TData, error) {
	if len(data) == 0 {
		return []*TData{}, nil
	}

	if c.Validator != nil {
		for i := range data {
			if err := c.Validator.StructCtx(ctx, &data[i]); err != nil {
				return nil, fmt.Errorf("validating request payload at index %d: %w", i, err)
			}
		}
	}
	return c.insertMany(ctx, c.WriteSQLService.Client(), data, preload)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) CreateManyFormat(
	ctx context.Context,
	data []TData,
	preload ...string,
) ([]*TResponse, error) {
	result, err := c.CreateMany(ctx, data, preload...)
	if err != nil {
		return nil, err
	}
	return c.ToModels(result), nil
}

// CreateWithTx is Create run against a caller-supplied transaction,
// returning the persisted row itself (TData) rather than running it through
// ToResource. Use CreateWithTxFormat instead when the caller wants the
// TResponse-shaped view.
func (c *CQRSService[TData, TResponse, TRequest, TID]) CreateWithTx(
	ctx context.Context,
	tx bun.Tx,
	data TData,
	preload ...string,
) (*TData, error) {
	if c.Validator != nil {
		if err := c.Validator.StructCtx(ctx, &data); err != nil {
			return nil, fmt.Errorf("validating request payload: %w", err)
		}
	}
	return c.insertOne(ctx, tx, data, preload)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) CreateWithTxFormat(
	ctx context.Context,
	tx bun.Tx,
	data TData,
	preload ...string,
) (*TResponse, error) {
	result, err := c.CreateWithTx(ctx, tx, data, preload...)
	if err != nil {
		return nil, err
	}
	return c.ToModel(result), nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) CreateManyWithTx(
	ctx context.Context,
	tx bun.Tx,
	data []TData,
	preload ...string,
) ([]*TData, error) {
	if len(data) == 0 {
		return []*TData{}, nil
	}
	if c.Validator != nil {
		for i := range data {
			if err := c.Validator.StructCtx(ctx, &data[i]); err != nil {
				return nil, fmt.Errorf("validating request payload at index %d: %w", i, err)
			}
		}
	}
	return c.insertMany(ctx, tx, data, preload)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) insertOne(
	ctx context.Context,
	db bun.IDB,
	data TData,
	preload []string,
) (*TData, error) {
	_, err := db.NewInsert().
		Model(&data).
		Returning("*").
		Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("inserting record: %w", err)
	}
	if err := c.applyPreloads(ctx, db, &data, preload...); err != nil {
		return nil, err
	}
	return &data, nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) insertMany(
	ctx context.Context,
	db bun.IDB,
	data []TData,
	preload []string,
) ([]*TData, error) {
	_, err := db.NewInsert().
		Model(&data).
		Returning("*").
		Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("bulk inserting records: %w", err)
	}
	if err := c.applyPreloadsMany(ctx, db, &data, preload...); err != nil {
		return nil, fmt.Errorf("loading preloads: %w", err)
	}

	out := make([]*TData, len(data))
	for i := range data {
		out[i] = &data[i]
	}
	return out, nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) CreateManyWithTxFormat(
	ctx context.Context,
	tx bun.Tx,
	data []TData,
	preload ...string,
) ([]*TResponse, error) {
	result, err := c.CreateManyWithTx(ctx, tx, data, preload...)
	if err != nil {
		return nil, err
	}
	return c.ToModels(result), nil
}
