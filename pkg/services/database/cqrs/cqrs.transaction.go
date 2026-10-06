package cqrs

import (
	"context"
	"fmt"

	"github.com/uptrace/bun"
)

// writeDB returns the write connection, or ErrWriteDBNotInitialized when the service was never
// started or has been stopped (its client is nil then).
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

// Transactions
func (c *CQRSService[TData, TResponse, TRequest, TID]) StartTx(ctx context.Context) (bun.Tx, error) {
	db, err := c.writeDB()
	if err != nil {
		return bun.Tx{}, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return bun.Tx{}, fmt.Errorf("starting transaction: %w", err)
	}
	return tx, nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) EndTx(ctx context.Context, tx bun.Tx, err error) error {
	if err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("rolling back transaction after error (%w): %w", err, rbErr)
		}
		return err
	}
	if commitErr := tx.Commit(); commitErr != nil {
		return fmt.Errorf("committing transaction: %w", commitErr)
	}
	return nil
}
