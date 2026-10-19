package sms

import (
	"context"
	"os"

	"go.uber.org/fx"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/queue"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// DefaultMaxCharacters is the length of one message, the limit of a single
// standard SMS segment.
const DefaultMaxCharacters = 160

// Config is how an SMSService reaches Twilio.
type Config struct {
	AccountSID string
	AuthToken  string
	// Sender is the number messages come from, in E.164 form (+15005550006).
	Sender string
	// MaxCharacters is the longest message accepted.
	MaxCharacters int32
	// Secured sends through Twilio, over a queue with retries. Off, the service
	// validates and formats but does not deliver (development).
	Secured bool
}

// FromEnv builds the Config from the environment:
//
//	SMS_ACCOUNT_SID     required when SMS_SECURED is true
//	SMS_AUTH_TOKEN      required when SMS_SECURED is true
//	SMS_SENDER          required, +15005550006 form
//	SMS_MAX_CHARACTERS  default 160
//	SMS_SECURED         default false
func FromEnv(lookup func(string) (string, bool)) (Config, error) {
	e := utils.NewEnv(lookup)
	cfg := Config{
		Sender:        e.Required("SMS_SENDER"),
		MaxCharacters: int32(e.Int("SMS_MAX_CHARACTERS", DefaultMaxCharacters)),
		Secured:       e.Bool("SMS_SECURED", false),
	}
	if cfg.Secured {
		cfg.AccountSID = e.Required("SMS_ACCOUNT_SID")
		cfg.AuthToken = e.Required("SMS_AUTH_TOKEN")
	} else {
		cfg.AccountSID = e.String("SMS_ACCOUNT_SID", "")
		cfg.AuthToken = e.String("SMS_AUTH_TOKEN", "")
	}
	if cfg.MaxCharacters < 1 {
		e.Invalid("SMS_MAX_CHARACTERS", "at least 1")
	}
	return cfg, e.Err()
}

// Module provides ISMSServices and starts it with the application, after the
// queue it sends through. Provide a queue first (queue.Module).
var Module = fx.Module("sms",
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
func New(p Params) ISMSServices {
	c := p.Config
	var opts []Option
	if p.Observer != nil {
		opts = append(opts, WithObserver(p.Observer))
	}
	svc := NewSMSService(c.AccountSID, c.AuthToken, c.Sender, c.MaxCharacters, c.Secured, p.Queue, p.Inflight, opts...)
	utils.OnLifecycle(p.Lifecycle,
		func(ctx context.Context) error { return svc.Start(ctx) },
		func(ctx context.Context) error { return svc.Stop(ctx) },
	)
	return svc
}
