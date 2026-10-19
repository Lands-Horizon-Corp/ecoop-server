package cqrs

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

func (s *CQRSService[TData, TResponse, TRequest, TID]) IncrementByID(ctx context.Context, id TID, field string, delta float64) (*TData, error) {
	db, err := s.writeDB()
	if err != nil {
		return nil, err
	}
	return utils.Scoped(ctx, db, func(q bun.IDB) (*TData, error) {
		return incrementByID[TData](ctx, q, s.ColumnDefaultID, id, field, delta)
	})
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) IncrementByIDWithTx(ctx context.Context, tx bun.Tx, id TID, field string, delta float64) (*TData, error) {
	return incrementByID[TData](ctx, tx, s.ColumnDefaultID, id, field, delta)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) DeleteByID(ctx context.Context, id TID) error {
	db, err := s.writeDB()
	if err != nil {
		return err
	}
	res, err := utils.Scoped(ctx, db, func(q bun.IDB) (sql.Result, error) {
		return q.NewDelete().
			Model((*TData)(nil)).
			Where("? = ?", bun.Ident(s.ColumnDefaultID), id).
			Exec(ctx)
	})
	if err != nil {
		return fmt.Errorf("deleting record: %w", err)
	}
	rows, err := res.RowsAffected()
	if err == nil && rows == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) DeleteByIDWithTx(ctx context.Context, tx bun.Tx, id TID) error {
	if err := utils.CheckDB(tx, ErrWriteDBNotInitialized); err != nil {
		return err
	}
	res, err := tx.NewDelete().
		Model((*TData)(nil)).
		Where("? = ?", bun.Ident(s.ColumnDefaultID), id).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("deleting record in tx: %w", err)
	}
	rows, err := res.RowsAffected()
	if err == nil && rows == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) DeleteMany(ctx context.Context, ids []TID) error {
	if len(ids) == 0 {
		return nil
	}
	db, err := s.writeDB()
	if err != nil {
		return err
	}
	_, err = utils.Scoped(ctx, db, func(q bun.IDB) (sql.Result, error) {
		return q.NewDelete().
			Model((*TData)(nil)).
			Where("? IN (?)", bun.Ident(s.ColumnDefaultID), bun.List(ids)).
			Exec(ctx)
	})
	if err != nil {
		return fmt.Errorf("bulk deleting records: %w", err)
	}
	return nil
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) DeleteManyWithTx(ctx context.Context, tx bun.Tx, ids []TID) error {
	if err := utils.CheckDB(tx, ErrWriteDBNotInitialized); err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	_, err := tx.NewDelete().
		Model((*TData)(nil)).
		Where("? IN (?)", bun.Ident(s.ColumnDefaultID), bun.List(ids)).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("bulk deleting records in tx: %w", err)
	}
	return nil
}
