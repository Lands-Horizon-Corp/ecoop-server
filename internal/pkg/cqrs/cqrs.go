package cqrs

type CQRSService[TData any, TResponse any, TRequest any, TID comparable] struct{}

func NewCQRS[TData any, TResponse any, TRequest any, TID comparable](
	c CQRSService[TData, TResponse, TRequest, TID],
) ICQRSService[TData, TResponse, TRequest, TID] {

	return &CQRSService[TData, TResponse, TRequest, TID]{}
}
