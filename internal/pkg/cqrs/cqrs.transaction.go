package cqrs

import (
	"context"

	"github.com/uptrace/bun"
)

func (s *CQRSService[TData, TResponse, TRequest, TID]) StartTx(ctx context.Context) (bun.Tx, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) EndTx(ctx context.Context, tx bun.Tx, err error) error {
	panic("not implemented")
}
