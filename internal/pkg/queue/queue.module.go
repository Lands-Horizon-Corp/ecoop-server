package queue

import (
	"context"
	"os"

	"go.uber.org/fx"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Config is how a QueueService is set up. The queue lives in the cache, so its
// connection comes from the cache service.
type Config struct {
	// Namespace isolates this app's jobs from every other app on the same
	// Valkey. Replicas of one app share a namespace. Empty means none.
	Namespace string
}

// FromEnv builds the Config from the environment:
//
//	QUEUE_NAMESPACE   default empty
func FromEnv(lookup func(string) (string, bool)) (Config, error) {
	e := utils.NewEnv(lookup)
	cfg := Config{Namespace: e.String("QUEUE_NAMESPACE", "")}
	return cfg, e.Err()
}

// Module provides IQueueService and starts it with the application, after the
// cache it is built on. Provide a cache first (cache.Module).
var Module = fx.Module("queue",
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
	Cache     cache.ICacheService
	Observer  observability.IObserver `optional:"true"`
}

// New builds the service and ties it to the application: Start when it starts,
// Stop when it shuts down.
func New(p Params) IQueueService {
	opts := []ServiceOption{WithNamespace(p.Config.Namespace)}
	if p.Observer != nil {
		opts = append(opts, WithObserver(p.Observer))
	}
	svc := NewQueueService(p.Cache, opts...)
	utils.OnLifecycle(p.Lifecycle,
		func(ctx context.Context) error { return svc.Start(ctx) },
		func(ctx context.Context) error { return svc.Stop(ctx) },
	)
	return svc
}
