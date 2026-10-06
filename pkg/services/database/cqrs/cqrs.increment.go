package cqrs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	sqlsvc "github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"
	"math"

	"github.com/Lands-Horizon-Corp/ecoop-server/utils"
	"github.com/uptrace/bun"
)

func (c *CQRSService[TData, TResponse, TRequest, TID]) IncrementByID(
	ctx context.Context, id TID, field string, delta float64,
) (*TData, error) {
	db, err := c.writeDB()
	if err != nil {
		return nil, err
	}
	return sqlsvc.Scoped(ctx, db, func(q bun.IDB) (*TData, error) {
		return incrementByID[TData](ctx, q, c.ColumnDefaultID, id, field, delta)
	})
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) IncrementByIDWithTx(
	ctx context.Context, tx bun.Tx, id TID, field string, delta float64,
) (*TData, error) {
	return incrementByID[TData](ctx, tx, c.ColumnDefaultID, id, field, delta)
}

func incrementByID[TData any](
	ctx context.Context, db bun.IDB, columnDefaultID string, id any, field string, delta float64,
) (*TData, error) {
	if err := checkDB(db); err != nil {
		return nil, err
	}
	if utils.BunColumnFieldIndex[TData](field) == -1 {
		return nil, fmt.Errorf("%w: increment %q", ErrUnknownField, field)
	}
	// A whole delta is sent as an integer so integer columns (money in minor units) stay exact:
	// bigint + double precision would round balances above 2^53.
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
