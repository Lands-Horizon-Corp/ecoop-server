package pagination

import "context"

func (c *PaginationService[TData, TID]) warn(ctx context.Context, msg string) {
	if c.LogService != nil {
		go func(ctx context.Context, msg string) {
			c.LogService.Warn(ctx, msg)
		}(ctx, msg)
	}
}
