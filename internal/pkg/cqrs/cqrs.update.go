package cqrs

import (
	"context"

	"github.com/uptrace/bun"
)

func (s *CQRSService[TData, TResponse, TRequest, TID]) UpdateByID(ctx context.Context, id TID, data TData, preload ...string) (*TData, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) UpdateByIDFormat(ctx context.Context, id TID, data TData, preload ...string) (*TResponse, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) UpdateByIDWithTx(ctx context.Context, tx bun.Tx, id TID, data TData, preload ...string) (*TData, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) UpdateByIDWithTxFormat(ctx context.Context, tx bun.Tx, id TID, data TData, preload ...string) (*TResponse, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) UpdateMany(ctx context.Context, data []TData, preload ...string) ([]*TData, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) UpdateManyFormat(ctx context.Context, data []TData, preload ...string) ([]*TResponse, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) UpdateManyWithTx(ctx context.Context, tx bun.Tx, data []TData, preload ...string) ([]*TData, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) UpdateManyWithTxFormat(ctx context.Context, tx bun.Tx, data []TData, preload ...string) ([]*TResponse, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) UpdateByIDWithValidation(ctx context.Context, id TID, request TRequest, preload ...string) (*TData, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) UpdateByIDWithValidationFormat(ctx context.Context, id TID, request TRequest, preload ...string) (*TResponse, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) UpdateByIDWithValidationTx(ctx context.Context, tx bun.Tx, id TID, request TRequest, preload ...string) (*TData, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) UpdateByIDWithValidationTxFormat(ctx context.Context, tx bun.Tx, id TID, request TRequest, preload ...string) (*TResponse, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) UpdateManyWithValidation(ctx context.Context, requests []TRequest, preload ...string) ([]*TData, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) UpdateManyWithValidationFormat(ctx context.Context, requests []TRequest, preload ...string) ([]*TResponse, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) UpdateManyWithValidationTx(ctx context.Context, tx bun.Tx, requests []TRequest, preload ...string) ([]*TData, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) UpdateManyWithValidationTxFormat(ctx context.Context, tx bun.Tx, requests []TRequest, preload ...string) ([]*TResponse, error) {
	panic("not implemented")
}
