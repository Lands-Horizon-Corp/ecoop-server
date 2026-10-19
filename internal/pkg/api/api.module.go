package api

import (
	"context"
	"log/slog"
	"os"

	"go.uber.org/fx"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/security"
)

// Module provides the API service to an fx application and ties it to the
// application's life. The configuration comes from the environment (see FromEnv),
// and an unsafe value stops the process at startup instead of weakening a control.
//
// Routes are registered by other modules' fx.Invoke functions, which all run
// before any OnStart hook, so the service is built, filled with routes, and only
// then starts listening. The observer, the logger, the cache and the security
// service are optional: without them the API observes nothing, discards its
// security log, shares no rate-limit state and checks no tokens.
//
// Stop is bounded by the configured shutdown timeout, which should stay inside
// fx's own stop timeout.
var Module = fx.Module("api",
	fx.Provide(
		func() (Config, error) { return FromEnv(os.LookupEnv) },
		newService,
	),
	fx.Invoke(func(lc fx.Lifecycle, svc IAPIService) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				if err := svc.Start(ctx); err != nil {
					return err
				}
				return svc.Run(ctx)
			},
			OnStop: svc.Stop,
		})
	}),
)

type serviceParams struct {
	fx.In

	Config   Config
	Observer observability.IObserver   `optional:"true"`
	Logger   *slog.Logger              `optional:"true"`
	Cache    cache.ICacheService       `optional:"true"`
	Security security.ISecurityService `optional:"true"`
}

func newService(p serviceParams) IAPIService {
	opts := []Option{WithConfig(p.Config)}
	if p.Observer != nil {
		opts = append(opts, WithObserver(p.Observer))
	}
	if p.Logger != nil {
		opts = append(opts, WithLogger(p.Logger))
	}
	return NewAPIService(0, p.Cache, p.Security, opts...)
}
