package cqrs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"reflect"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pagination"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
	"github.com/uptrace/bun"
)

func (c *CQRSService[TData, TResponse, TRequest, TID]) writeDB() (*bun.DB, error) {
	if c.WriteSQLService == nil {
		return nil, ErrWriteDBNotInitialized
	}
	db := c.WriteSQLService.Client()
	if db == nil {
		return nil, ErrWriteDBNotInitialized
	}
	return db, nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) insertOne(
	ctx context.Context,
	db bun.IDB,
	data TData,
	preload []string,
) (*TData, error) {
	if err := utils.CheckDB(db, ErrWriteDBNotInitialized); err != nil {
		return nil, err
	}
	if err := c.prepareText(&data); err != nil {
		return nil, err
	}
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
	if err := utils.CheckDB(db, ErrWriteDBNotInitialized); err != nil {
		return nil, err
	}
	data, err := c.prepareTexts(data)
	if err != nil {
		return nil, err
	}
	_, err = db.NewInsert().
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

func incrementByID[TData any](
	ctx context.Context, db bun.IDB, columnDefaultID string, id any, field string, delta float64,
) (*TData, error) {
	if err := utils.CheckDB(db, ErrWriteDBNotInitialized); err != nil {
		return nil, err
	}
	if utils.StructTagFieldIndex[TData]("bun", field) == -1 {
		return nil, fmt.Errorf("%w: increment %q", ErrUnknownField, field)
	}
	var amount any = delta
	if delta == math.Trunc(delta) && math.Abs(delta) <= 1<<53 {
		amount = int64(delta)
	}
	var data TData
	_, err := db.NewUpdate().
		Model((*TData)(nil)).
		Set("? = ? + ?", bun.Ident(field), bun.Ident(field), amount).
		Where("? = ?", bun.Ident(columnDefaultID), id).
		Returning("*").
		Exec(ctx, &data)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, sql.ErrNoRows
		}
		return nil, fmt.Errorf("incrementing %s: %w", field, err)
	}
	return &data, nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) updateOne(
	ctx context.Context,
	db bun.IDB,
	id TID,
	data TData,
	preload []string,
) (*TData, error) {
	if err := utils.CheckDB(db, ErrWriteDBNotInitialized); err != nil {
		return nil, err
	}
	if err := s.prepareText(&data); err != nil {
		return nil, err
	}
	res, err := db.NewUpdate().
		Model(&data).
		Where("? = ?", bun.Ident(s.ColumnDefaultID), id).
		Returning("*").
		Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("updating record: %w", err)
	}
	rows, err := res.RowsAffected()
	if err == nil && rows == 0 {
		return nil, sql.ErrNoRows
	}
	if err := s.applyPreloads(ctx, db, &data, preload...); err != nil {
		return nil, err
	}
	return &data, nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) updateMany(
	ctx context.Context,
	db bun.IDB,
	data []TData,
	preload []string,
) ([]*TData, error) {
	if err := utils.CheckDB(db, ErrWriteDBNotInitialized); err != nil {
		return nil, err
	}
	data, err := s.prepareTexts(data)
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
	if err := s.applyPreloadsMany(ctx, db, &data, preload...); err != nil {
		return nil, fmt.Errorf("loading preloads: %w", err)
	}

	out := make([]*TData, len(data))
	for i := range data {
		out[i] = &data[i]
	}
	return out, nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) prepareText(data *TData) error {
	if len(s.textFields) == 0 || data == nil {
		return nil
	}
	v := reflect.ValueOf(data).Elem()
	for _, i := range s.textFields {
		clean, err := utils.CleanText(v.Field(i), utils.TextPolicy{Normalize: !s.RawText, RejectUnsafe: !s.RawText})
		if err != nil {
			return fmt.Errorf("field %s: %w", v.Type().Field(i).Name, err)
		}
		v.Field(i).Set(clean)
	}
	return nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) prepareTexts(data []TData) ([]TData, error) {
	if len(s.textFields) == 0 {
		return data, nil
	}
	out := make([]TData, len(data))
	copy(out, data)
	for i := range out {
		if err := s.prepareText(&out[i]); err != nil {
			return nil, fmt.Errorf("record %d: %w", i, err)
		}
	}
	return out, nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) dataFromRequest(
	ctx context.Context,
	request TRequest,
	index int,
) (TData, error) {
	var zero TData
	if s.FromRequest == nil {
		return zero, ErrFromRequestNotSet
	}
	if s.Validator != nil {
		if err := s.Validator.StructCtx(ctx, &request); err != nil {
			return zero, fmt.Errorf("validating request payload at index %d: %w", index, err)
		}
	}
	data := s.FromRequest(&request)
	if data == nil {
		return zero, fmt.Errorf("converting request payload at index %d: FromRequest returned nil", index)
	}
	return *data, nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) dataFromRequests(
	ctx context.Context,
	requests []TRequest,
) ([]TData, error) {
	out := make([]TData, len(requests))
	for i := range requests {
		d, err := s.dataFromRequest(ctx, requests[i], i)
		if err != nil {
			return nil, err
		}
		out[i] = d
	}
	return out, nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) idFilter(id TID) pagination.StructuredFilter {
	return pagination.StructuredFilter{
		Filters: []pagination.Filter{{Field: s.ColumnDefaultID, Mode: pagination.ModeEqual, Value: id}},
	}
}
