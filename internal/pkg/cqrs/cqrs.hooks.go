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
	if data == nil {
		return nil
	}
	return s.ToResource(data)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) ToModels(data []*TData) []*TResponse {
	if data == nil {
		return []*TResponse{}
	}
	out := make([]*TResponse, 0, len(data))
	for _, item := range data {
		if m := s.ToModel(item); m != nil {
			out = append(out, m)
		}
	}
	return out
}
