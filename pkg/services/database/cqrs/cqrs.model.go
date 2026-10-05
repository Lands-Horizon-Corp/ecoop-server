package cqrs

func (c *CQRSService[TData, TResponse, TRequest, TID]) ToModel(data *TData) *TResponse {
	if data == nil || c.ToResource == nil {
		return nil
	}
	return c.ToResource(data)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) ToModels(data []*TData) []*TResponse {
	if data == nil || c.ToResource == nil {
		return nil
	}
	out := make([]*TResponse, 0, len(data))
	for _, item := range data {
		if item == nil {
			continue
		}
		if m := c.ToModel(item); m != nil {
			out = append(out, m)
		}
	}
	if out == nil {
		return []*TResponse{}
	}
	return out
}
