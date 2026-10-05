package cqrs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/Lands-Horizon-Corp/ecoop-server/utils"
	"github.com/uptrace/bun"
)

func (c *CQRSService[TData, TResponse, TRequest, TID]) IncrementByID(
	ctx context.Context, id TID, field string, delta float64,
) (*TData, error) {
	return incrementByID[TData](ctx, c.WriteSQLService.Client(), c.ColumnDefaultID, id, field, delta)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) IncrementByIDWithTx(
	ctx context.Context, tx bun.Tx, id TID, field string, delta float64,
) (*TData, error) {
	return incrementByID[TData](ctx, tx, c.ColumnDefaultID, id, field, delta)
}

func incrementByID[TData any](
	ctx context.Context, db bun.IDB, columnDefaultID string, id any, field string, delta float64,
) (*TData, error) {
	if utils.BunColumnFieldIndex[TData](field) == -1 {
		return nil, fmt.Errorf("%w: increment %q", ErrUnknownField, field)
	}
	var data TData
	_, err := db.NewUpdate().
		Model((*TData)(nil)).
		Set("? = ? + ?", bun.Ident(field), bun.Ident(field), delta).
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
