package smtp

import (
	"context"
	"os"

	"go.uber.org/fx"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/queue"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Config is how an SMTPService connects and who it sends as.
type Config struct {
	Host     string
	Port     int
	Username string
	Password string
	// From is the sender address; FromName the display name.
	From     string
	FromName string
	// Secured sends through the real server, over a queue with retries. Off, the
	// service validates and renders but does not deliver (development).
	Secured bool
}

// FromEnv builds the Config from the environment:
//
//	SMTP_HOST       default 127.0.0.1
//	SMTP_PORT       default 1025
//	SMTP_USERNAME   default empty
//	SMTP_PASSWORD   default empty
//	SMTP_FROM       required
//	SMTP_FROM_NAME  default empty
//	SMTP_SECURED    default false
func FromEnv(lookup func(string) (string, bool)) (Config, error) {
	e := utils.NewEnv(lookup)
	cfg := Config{
		Host:     e.String("SMTP_HOST", "127.0.0.1"),
		Port:     e.Int("SMTP_PORT", 1025),
		Username: e.String("SMTP_USERNAME", ""),
		Password: e.String("SMTP_PASSWORD", ""),
		From:     e.Required("SMTP_FROM"),
		FromName: e.String("SMTP_FROM_NAME", ""),
		Secured:  e.Bool("SMTP_SECURED", false),
	}
	if cfg.Port < 1 || cfg.Port > 65535 {
		e.Invalid("SMTP_PORT", "between 1 and 65535")
	}
	return cfg, e.Err()
}

// Module provides ISMTPService and starts it with the application, after the
// queue it sends through. Provide a queue first (queue.Module).
var Module = fx.Module("smtp",
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
	Queue     queue.IQueueService
	Observer  observability.IObserver `optional:"true"`
	Inflight  *utils.Inflight         `optional:"true"`
}

// New builds the service and ties it to the application: Start when it starts,
// Stop when it shuts down.
func New(p Params) ISMTPService {
	c := p.Config
	var opts []Option
	if p.Observer != nil {
		opts = append(opts, WithObserver(p.Observer))
	}
	svc := NewSMTPService(c.Host, c.Port, c.Username, c.Password, c.From, c.FromName, c.Secured, p.Queue, p.Inflight, opts...)
	utils.OnLifecycle(p.Lifecycle,
		func(ctx context.Context) error { return svc.Start(ctx) },
		func(ctx context.Context) error { return svc.Stop(ctx) },
	)
	return svc
}
