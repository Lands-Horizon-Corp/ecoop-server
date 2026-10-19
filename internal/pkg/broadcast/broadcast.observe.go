package broadcast

import (
	"context"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
)

// Option configures a BroadcasterService: WithObserver.
type Option func(*BroadcasterService)

// WithObserver gives the service an observer for its metrics, traces and logs.
// Without one it observes nothing. Only the operation name and its outcome are
// ever recorded: never a channel, an event name or a payload.
func WithObserver(o observability.IObserver) Option {
	return func(b *BroadcasterService) { b.obs = observability.Pick(o) }
}

// begin starts the Operation of one call. A service built as a bare struct
// literal has no observer, so it falls back to Nop without writing.
func (b *BroadcasterService) begin(ctx context.Context, name string) (context.Context, observability.Operation) {
	if b.obs == nil {
		return observability.Nop().Begin(ctx, observability.ComponentBroadcast, name)
	}
	return b.obs.Begin(ctx, observability.ComponentBroadcast, name)
}
