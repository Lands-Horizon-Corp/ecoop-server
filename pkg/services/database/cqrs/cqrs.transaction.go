package cqrs

import (
	"context"
	"errors"
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

// checkDB rejects a nil or zero-value connection or transaction before it is dereferenced.
func checkDB(db bun.IDB) error {
	switch v := db.(type) {
	case nil:
		return ErrWriteDBNotInitialized
	case *bun.DB:
		if v == nil {
			return ErrWriteDBNotInitialized
		}
	case bun.Tx:
		if v.Tx == nil {
			return ErrNilTx
		}
	case *bun.Tx:
		if v == nil || v.Tx == nil {
			return ErrNilTx
		}
	}
	return nil
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
	if txErr := checkDB(tx); txErr != nil {
		return errors.Join(err, txErr)
	}
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
