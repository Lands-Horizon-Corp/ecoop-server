package config

import (
	"context"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
)

type (
	// Option configures a ConfigService: WithObserver.
	Option func(observed)

	// observed is what an Option may set: the observer. The service is generic,
	// so an Option works through this interface instead of the service type.
	observed interface{ setObserver(observability.IObserver) }
)

func (c *ConfigService[T]) setObserver(o observability.IObserver) { c.obs = observability.Pick(o) }

// WithObserver gives the service an observer for its metrics, traces and logs.
// Without one it observes nothing. Only the operation name and its outcome are
// ever recorded: never a variable name, a value, a file path or the config.
func WithObserver(o observability.IObserver) Option {
	return func(s observed) { s.setObserver(o) }
}

// begin starts the Operation of one call. A service built as a bare struct
// literal has no observer, so it falls back to Nop without writing.
func (c *ConfigService[T]) begin(ctx context.Context, name string) (context.Context, observability.Operation) {
	if c.obs == nil {
		return observability.Nop().Begin(ctx, observability.ComponentConfig, name)
	}
	return c.obs.Begin(ctx, observability.ComponentConfig, name)
}

// fail records one failed call that was not worth a span of its own.
func (c *ConfigService[T]) fail(ctx context.Context, name string, err error) {
	_, op := c.begin(ctx, name)
	op.End(err)
}
