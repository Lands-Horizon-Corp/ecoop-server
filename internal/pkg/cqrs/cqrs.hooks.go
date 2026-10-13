package cqrs

import "context"

func (s *CQRSService[TData, TResponse, TRequest, TID]) OnCreated(ctx context.Context, data *TData) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) OnUpdated(ctx context.Context, data *TData) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) OnDeleted(ctx context.Context, data *TData) {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) ToModel(data *TData) *TResponse {
	panic("not implemented")
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) ToModels(data []*TData) []*TResponse {
	panic("not implemented")
}
