package broker

import (
	"context"
	"net"
	"os"

	"go.uber.org/fx"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Config is how a BrokerService reaches Kafka.
type Config struct {
	// Brokers are the seed brokers, host:port.
	Brokers []string
	// GroupID is the consumer group: replicas of one app share it so each
	// message is handled once.
	GroupID string
}

// FromEnv builds the Config from the environment:
//
//	BROKER_HOST      default 127.0.0.1
//	BROKER_PORT      default 9094
//	BROKER_GROUP_ID  required
func FromEnv(lookup func(string) (string, bool)) (Config, error) {
	e := utils.NewEnv(lookup)
	cfg := Config{
		Brokers: []string{net.JoinHostPort(e.String("BROKER_HOST", "127.0.0.1"), e.String("BROKER_PORT", "9094"))},
		GroupID: e.Required("BROKER_GROUP_ID"),
	}
	return cfg, e.Err()
}

// Module provides IBrokerService and runs it with the application.
var Module = fx.Module("broker",
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
// when it starts, Stop when it shuts down.
func New(p Params) IBrokerService {
	var opts []Option
	if p.Observer != nil {
		opts = append(opts, WithObserver(p.Observer))
	}
	svc := NewBrokerService(p.Config.Brokers, p.Config.GroupID, p.Inflight, opts...)
	utils.OnLifecycle(p.Lifecycle,
		func(ctx context.Context) error { return svc.Run(ctx) },
		func(ctx context.Context) error { return svc.Stop(ctx) },
	)
	return svc
}
