package cache

import (
	"context"
	"net"
	"os"

	"go.uber.org/fx"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Environment, read by FromEnv:
//
//	CACHE_HOST      default 127.0.0.1
//	CACHE_PORT      default 6379
//	CACHE_PASSWORD  the server password
//
// Config.Addresses holds one host:port, or several for a cluster.

// FromEnv builds the Config from the environment. lookup is os.LookupEnv in
// production. Every mistake is reported at once.
func FromEnv(lookup func(string) (string, bool)) (Config, error) {
	e := utils.NewEnv(lookup)
	host := e.String("CACHE_HOST", "127.0.0.1")
	port := e.String("CACHE_PORT", "6379")
	cfg := Config{
		Addresses: []string{net.JoinHostPort(host, port)},
		Password:  e.String("CACHE_PASSWORD", ""),
	}
	return cfg, e.Err()
}

// Module provides ICacheService and runs it with the application. It needs a
// Config from FromEnv (provided here from the process environment). An observer
// is used when the application provides one.
//
// To use another configuration, supply it and leave this Module's own provider
// out by building the service with New instead.
var Module = fx.Module("cache",
	fx.Provide(
		func() (Config, error) { return FromEnv(os.LookupEnv) },
		New,
	),
)

// Params is what New needs from the application.
type Params struct {
	fx.In

	Lifecycle fx.Lifecycle
	Config    Config
	Observer  observability.IObserver `optional:"true"`
	Inflight  *utils.Inflight         `optional:"true"`
}

// New builds the service from its Config and ties it to the application: Run
// when the application starts, Stop when it shuts down.
func New(p Params) ICacheService {
	opts := []Option{}
	if p.Observer != nil {
		opts = append(opts, WithObserver(p.Observer))
	}
	svc := NewCacheService(p.Config.Addresses, p.Config.Password, p.Inflight, opts...)
	utils.OnLifecycle(p.Lifecycle,
		func(ctx context.Context) error { return svc.Run(ctx) },
		func(ctx context.Context) error { return svc.Stop(ctx) },
	)
	return svc
}
