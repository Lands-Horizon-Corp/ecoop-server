package cqrs

import "context"

func (c *CQRSService[TData, TResponse, TRequest, TID]) info(ctx context.Context, msg string) {
	if c.LogService != nil {
		go func(ctx context.Context, msg string) {
			c.LogService.Log(ctx, msg)
		}(ctx, msg)
	}
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) error(ctx context.Context, msg string) {
	if c.LogService != nil {
		go func(ctx context.Context, msg string) {
			c.LogService.Error(ctx, msg)
		}(ctx, msg)
	}
}
func (c *CQRSService[TData, TResponse, TRequest, TID]) warn(ctx context.Context, msg string) {
	if c.LogService != nil {
		go func(ctx context.Context, msg string) {
			c.LogService.Warn(ctx, msg)
		}(ctx, msg)
	}
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) success(ctx context.Context, msg string) {
	if c.LogService != nil {
		go func(ctx context.Context, msg string) {
			c.LogService.Success(ctx, msg)
		}(ctx, msg)
	}
}
