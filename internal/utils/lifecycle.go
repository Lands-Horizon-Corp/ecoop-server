package utils

import (
	"context"

	"go.uber.org/fx"
)

// OnLifecycle ties a service to the application's life: start runs when the
// application starts and stop when it shuts down. Either may be nil.
//
// Call it from the service's constructor, not from a separate Invoke. fx builds
// a service's dependencies first, so their hooks are registered first: they
// start before it does and stop after it, which is the order a service needs
// (the queue is up before the mailer that sends through it).
func OnLifecycle(lc fx.Lifecycle, start, stop func(context.Context) error) {
	lc.Append(fx.Hook{OnStart: start, OnStop: stop})
}

// Sequence runs each step in turn and stops at the first error. A service whose
// Start and Run are both needed (open, then migrate) hands them to OnLifecycle
// as one start function.
func Sequence(steps ...func(context.Context) error) func(context.Context) error {
	return func(ctx context.Context) error {
		for _, step := range steps {
			if step == nil {
				continue
			}
			if err := step(ctx); err != nil {
				return err
			}
		}
		return nil
	}
}
