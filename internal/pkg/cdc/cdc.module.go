package cdc

import (
	"context"
	"net"
	"os"

	"go.uber.org/fx"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Config is how a CDCService reads Debezium's change topics.
type Config struct {
	// Brokers are Kafka's seed brokers, host:port (the same Kafka Debezium
	// writes to).
	Brokers []string
	// GroupID is the consumer group; replicas of one app share it.
	GroupID string
	// TopicPrefix is Debezium's topic.prefix: topics are <prefix>.<schema>.<table>.
	TopicPrefix string
}

// FromEnv builds the Config from the environment:
//
//	BROKER_HOST       default 127.0.0.1
//	BROKER_PORT       default 9094
//	CDC_GROUP_ID      default BROKER_GROUP_ID, required if both are unset
//	CDC_TOPIC_PREFIX  default ecoop
func FromEnv(lookup func(string) (string, bool)) (Config, error) {
	e := utils.NewEnv(lookup)
	group := e.String("CDC_GROUP_ID", "")
	if group == "" {
		group = e.Required("BROKER_GROUP_ID")
	}
	cfg := Config{
		Brokers:     []string{net.JoinHostPort(e.String("BROKER_HOST", "127.0.0.1"), e.String("BROKER_PORT", "9094"))},
		GroupID:     group,
		TopicPrefix: e.String("CDC_TOPIC_PREFIX", defaultTopicPrefix),
	}
	return cfg, e.Err()
}

// Module provides ICDCService and runs it with the application.
var Module = fx.Module("cdc",
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
}

// New builds the service from its Config and ties it to the application: Run
// when it starts, Stop when it shuts down.
func New(p Params) ICDCService {
	var opts []Option
	if p.Observer != nil {
		opts = append(opts, WithObserver(p.Observer))
	}
	svc := NewCDCService(p.Config.Brokers, p.Config.GroupID, p.Config.TopicPrefix, opts...)
	utils.OnLifecycle(p.Lifecycle,
		func(ctx context.Context) error { return svc.Run(ctx) },
		func(ctx context.Context) error { return svc.Stop(ctx) },
	)
	return svc
}
