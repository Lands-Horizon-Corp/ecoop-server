package pagination

import "context"

// warn mirrors cqrs.CQRSImpl's own logger helpers: a no-op when LogService
// isn't set, fire-and-forget otherwise so a slow/unavailable log sink never
// adds latency to a page request.
func (c *PaginationService[TData, TID]) warn(ctx context.Context, msg string) {
	if c.LogService != nil {
		go func(ctx context.Context, msg string) {
			c.LogService.Warn(ctx, msg)
		}(ctx, msg)
	}
}
