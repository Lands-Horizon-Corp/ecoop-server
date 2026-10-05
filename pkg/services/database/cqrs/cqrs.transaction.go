package cqrs

import (
	"context"
	"fmt"

	"github.com/uptrace/bun"
)

// Transactions
func (c *CQRSService[TData, TResponse, TRequest, TID]) StartTx(ctx context.Context) (bun.Tx, error) {
	tx, err := c.WriteSQLService.Client().BeginTx(ctx, nil)
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
