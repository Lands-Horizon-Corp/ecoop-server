package cqrs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// StartTx begins a transaction on the write database, stamped with the tenant of
// ctx (if any) so row-level security applies to everything run inside it. End it
// with EndTx; use only the ...WithTx methods with the returned transaction.
func (s *CQRSService[TData, TResponse, TRequest, TID]) StartTx(ctx context.Context) (bun.Tx, error) {
	if ctx == nil {
		return bun.Tx{}, ErrNilContext
	}
	db, err := s.writeDB()
	if err != nil {
		return bun.Tx{}, err
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return bun.Tx{}, fmt.Errorf("beginning transaction: %w", err)
	}
	if err := utils.SetTenant(ctx, tx); err != nil {
		return bun.Tx{}, errors.Join(fmt.Errorf("setting the tenant: %w", err), tx.Rollback())
	}
	return tx, nil
}

// EndTx finishes a transaction from StartTx: it rolls back and returns err when
// err is not nil, otherwise it commits. It never panics on a transaction that
// was not started and always leaves the transaction closed.
func (s *CQRSService[TData, TResponse, TRequest, TID]) EndTx(ctx context.Context, tx bun.Tx, err error) error {
	if tx.Tx == nil {
		return errors.Join(ErrNilTx, err)
	}
	if err != nil {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			return errors.Join(err, fmt.Errorf("rolling back: %w", rbErr))
		}
		return err
	}
	if cErr := tx.Commit(); cErr != nil {
		return fmt.Errorf("committing: %w", cErr)
	}
	return nil
}
