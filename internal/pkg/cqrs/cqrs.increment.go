package cqrs

import (
	"context"

	"github.com/uptrace/bun"
)

func (s *CQRSService[TData, TResponse, TRequest, TID]) IncrementByID(ctx context.Context, id TID, field string, delta float64) (*TData, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) IncrementByIDWithTx(ctx context.Context, tx bun.Tx, id TID, field string, delta float64) (*TData, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) DeleteByID(ctx context.Context, id TID) error {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) DeleteByIDWithTx(ctx context.Context, tx bun.Tx, id TID) error {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) DeleteMany(ctx context.Context, ids []TID) error {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) DeleteManyWithTx(ctx context.Context, tx bun.Tx, ids []TID) error {
	panic("not implemented")
}
