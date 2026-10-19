package broadcast

import (
	"context"
	"os"

	"go.uber.org/fx"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Config is how a BroadcasterService reaches the websocket server.
type Config struct {
	Host   string
	Port   string
	AppID  string
	Key    string
	Secret string
	// Secured makes the connection https.
	Secured bool
	// MaxPayloadKB is the largest message the server accepts.
	MaxPayloadKB int
}

// FromEnv builds the Config from the environment:
//
//	BROADCAST_HOST            default 127.0.0.1
//	BROADCAST_PORT            default 6001
//	BROADCAST_APP_ID          required
//	BROADCAST_APP_KEY         required
//	BROADCAST_APP_SECRET      required
//	BROADCAST_SECURED         default false
//	BROADCAST_MAX_PAYLOAD_KB  default 10
func FromEnv(lookup func(string) (string, bool)) (Config, error) {
	e := utils.NewEnv(lookup)
	cfg := Config{
		Host:         e.String("BROADCAST_HOST", "127.0.0.1"),
		Port:         e.String("BROADCAST_PORT", "6001"),
		AppID:        e.Required("BROADCAST_APP_ID"),
		Key:          e.Required("BROADCAST_APP_KEY"),
		Secret:       e.Required("BROADCAST_APP_SECRET"),
		Secured:      e.Bool("BROADCAST_SECURED", false),
		MaxPayloadKB: e.Int("BROADCAST_MAX_PAYLOAD_KB", DefaultMaxPayloadKB),
	}
	if cfg.MaxPayloadKB < 1 {
		e.Invalid("BROADCAST_MAX_PAYLOAD_KB", "at least 1")
	}
	return cfg, e.Err()
}

// Module provides IBroadcastService and runs it with the application.
var Module = fx.Module("broadcast",
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
// when it starts (connect, announce on the health channel), Stop when it shuts
// down.
func New(p Params) IBroadcastService {
	c := p.Config
	var opts []Option
	if p.Observer != nil {
		opts = append(opts, WithObserver(p.Observer))
	}
	svc := NewBroadcastService(c.AppID, c.Key, c.Secret, c.Host, c.Port, c.Secured, c.MaxPayloadKB, p.Inflight, opts...)
	utils.OnLifecycle(p.Lifecycle,
		func(ctx context.Context) error { return svc.Run(ctx) },
		func(ctx context.Context) error { return svc.Stop(ctx) },
	)
	return svc
}
