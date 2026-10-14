package cqrs

func (s *CQRSService[TData, TResponse, TRequest, TID]) ToModel(data *TData) *TResponse {
	if data == nil || s.ToResource == nil {
		return nil
	}
	return s.ToResource(data)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) ToModels(data []*TData) []*TResponse {
	if s.ToResource == nil || len(data) == 0 || data == nil {
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
