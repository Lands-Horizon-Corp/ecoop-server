package cron

import (
	"context"

	"go.uber.org/fx"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/queue"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Module provides ICronServices and starts it with the application, after the
// queue its jobs run on. It has no settings of its own: jobs are registered in
// code, with a schedule, on the service it provides.
var Module = fx.Module("cron", fx.Provide(New))

// Params is what New needs from the application.
type Params struct {
	fx.In

	Lifecycle fx.Lifecycle
	Queue     queue.IQueueService
	Observer  observability.IObserver `optional:"true"`
	Inflight  *utils.Inflight         `optional:"true"`
}

// New builds the service and ties it to the application: Start when it starts,
// Stop when it shuts down.
func New(p Params) ICronServices {
	var opts []Option
	if p.Observer != nil {
		opts = append(opts, WithObserver(p.Observer))
	}
	svc := NewCronService(p.Queue, p.Inflight, opts...)
	utils.OnLifecycle(p.Lifecycle,
		func(ctx context.Context) error { return svc.Start(ctx) },
		func(ctx context.Context) error { return svc.Stop(ctx) },
	)
	return svc
}
