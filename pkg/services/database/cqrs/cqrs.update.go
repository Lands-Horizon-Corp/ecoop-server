package cqrs

import (
	"context"
	"database/sql"
	"fmt"
	sqlsvc "github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"

	"github.com/uptrace/bun"
)

func (c *CQRSService[TData, TResponse, TRequest, TID]) UpdateByID(
	ctx context.Context,
	id TID,
	data TData,
	preload ...string,
) (*TData, error) {
	if c.Validator != nil {
		if err := c.Validator.StructCtx(ctx, &data); err != nil {
			return nil, fmt.Errorf("validating request payload: %w", err)
		}
	}
	db, err := c.writeDB()
	if err != nil {
		return nil, err
	}
	return sqlsvc.Scoped(ctx, db, func(q bun.IDB) (*TData, error) { return c.updateOne(ctx, q, id, data, preload) })
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) updateOne(
	ctx context.Context,
	db bun.IDB,
	id TID,
	data TData,
	preload []string,
) (*TData, error) {
	if err := checkDB(db); err != nil {
		return nil, err
	}
	if err := c.prepareText(&data); err != nil {
		return nil, err
	}
	res, err := db.NewUpdate().
		Model(&data).
		Where("? = ?", bun.Ident(c.ColumnDefaultID), id).
		Returning("*").
		Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("updating record: %w", err)
	}
	rows, err := res.RowsAffected()
	if err == nil && rows == 0 {
		return nil, sql.ErrNoRows
	}
	if err := c.applyPreloads(ctx, db, &data, preload...); err != nil {
		return nil, err
	}
	return &data, nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) updateMany(
	ctx context.Context,
	db bun.IDB,
	data []TData,
	preload []string,
) ([]*TData, error) {
	if err := checkDB(db); err != nil {
		return nil, err
	}
	data, err := c.prepareTexts(data)
	if err != nil {
		return nil, err
	}
	_, err = db.NewUpdate().
		Model(&data).
		Bulk().
		Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("bulk updating records: %w", err)
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

func (c *CQRSService[TData, TResponse, TRequest, TID]) UpdateByIDFormat(
	ctx context.Context,
	id TID,
	data TData,
	preload ...string,
) (*TResponse, error) {
	result, err := c.UpdateByID(ctx, id, data, preload...)
	if err != nil {
		return nil, err
	}
	if c.ToResource == nil {
		return nil, nil
	}
	return c.ToResource(result), nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) UpdateMany(
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

	db, err := c.writeDB()
	if err != nil {
		return nil, err
	}
	return sqlsvc.Scoped(ctx, db, func(q bun.IDB) ([]*TData, error) { return c.updateMany(ctx, q, data, preload) })
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) UpdateManyFormat(
	ctx context.Context,
	data []TData,
	preload ...string,
) ([]*TResponse, error) {
	result, err := c.UpdateMany(ctx, data, preload...)
	if err != nil {
		return nil, err
	}
	if c.ToResource == nil {
		return nil, nil
	}
	responses := make([]*TResponse, 0, len(result))
	for _, d := range result {
		if res := c.ToResource(d); res != nil {
			responses = append(responses, res)
		}
	}
	return responses, nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) UpdateManyWithTx(
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

	return c.updateMany(ctx, tx, data, preload)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) UpdateManyWithTxFormat(
	ctx context.Context,
	tx bun.Tx,
	data []TData,
	preload ...string,
) ([]*TResponse, error) {
	result, err := c.UpdateManyWithTx(ctx, tx, data, preload...)
	if err != nil {
		return nil, err
	}
	if c.ToResource == nil {
		return nil, nil
	}
	responses := make([]*TResponse, 0, len(result))
	for _, d := range result {
		if res := c.ToResource(d); res != nil {
			responses = append(responses, res)
		}
	}
	return responses, nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) UpdateByIDWithTx(
	ctx context.Context,
	tx bun.Tx,
	id TID,
	data TData,
	preload ...string,
) (*TData, error) {
	if c.Validator != nil {
		if err := c.Validator.StructCtx(ctx, &data); err != nil {
			return nil, fmt.Errorf("validating request payload: %w", err)
		}
	}
	return c.updateOne(ctx, tx, id, data, preload)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) UpdateByIDWithTxFormat(
	ctx context.Context,
	tx bun.Tx,
	id TID,
	data TData,
	preload ...string,
) (*TResponse, error) {
	result, err := c.UpdateByIDWithTx(ctx, tx, id, data, preload...)
	if err != nil {
		return nil, err
	}
	if c.ToResource == nil {
		return nil, nil
	}
	return c.ToResource(result), nil
}
