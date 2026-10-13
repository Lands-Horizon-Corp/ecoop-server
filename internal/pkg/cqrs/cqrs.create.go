package cqrs

import (
	"context"

	"github.com/uptrace/bun"
)

func (s *CQRSService[TData, TResponse, TRequest, TID]) Create(ctx context.Context, data TData, preload ...string) (*TData, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) CreateFormat(ctx context.Context, data TData, preload ...string) (*TResponse, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) CreateMany(ctx context.Context, data []TData, preload ...string) ([]*TData, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) CreateManyFormat(ctx context.Context, data []TData, preload ...string) ([]*TResponse, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) CreateWithTx(ctx context.Context, tx bun.Tx, data TData, preload ...string) (*TData, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) CreateWithTxFormat(ctx context.Context, tx bun.Tx, data TData, preload ...string) (*TResponse, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) CreateManyWithTx(ctx context.Context, tx bun.Tx, data []TData, preload ...string) ([]*TData, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) CreateManyWithTxFormat(ctx context.Context, tx bun.Tx, data []TData, preload ...string) ([]*TResponse, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) CreateWithValidation(ctx context.Context, request TRequest, preload ...string) (*TData, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) CreateWithValidationFormat(ctx context.Context, request TRequest, preload ...string) (*TResponse, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) CreateManyWithValidation(ctx context.Context, requests []TRequest, preload ...string) ([]*TData, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) CreateManyWithValidationFormat(ctx context.Context, requests []TRequest, preload ...string) ([]*TResponse, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) CreateWithValidationTx(ctx context.Context, tx bun.Tx, request TRequest, preload ...string) (*TData, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) CreateWithValidationTxFormat(ctx context.Context, tx bun.Tx, request TRequest, preload ...string) (*TResponse, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) CreateManyWithValidationTx(ctx context.Context, tx bun.Tx, requests []TRequest, preload ...string) ([]*TData, error) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) CreateManyWithValidationTxFormat(ctx context.Context, tx bun.Tx, requests []TRequest, preload ...string) ([]*TResponse, error) {
	panic("not implemented")
}
