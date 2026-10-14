package cqrs

import "context"

func (s *CQRSService[TData, TResponse, TRequest, TID]) Run(ctx context.Context) error {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) Stop(ctx context.Context) error {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) OnCreated(ctx context.Context, data *TData) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) OnUpdated(ctx context.Context, data *TData) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) OnDeleted(ctx context.Context, data *TData) {
	panic("not implemented")
}
