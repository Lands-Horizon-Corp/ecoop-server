package qr

import (
	"context"
	"os"

	"go.uber.org/fx"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Config is how a QRService seals the codes it makes.
type Config struct {
	// Secret keys the encryption of every code's payload.
	Secret string
}

// FromEnv builds the Config from the environment:
//
//	APP_SECRET   required
func FromEnv(lookup func(string) (string, bool)) (Config, error) {
	e := utils.NewEnv(lookup)
	cfg := Config{Secret: e.Required("APP_SECRET")}
	return cfg, e.Err()
}

// Module provides IQRServices and starts it with the application.
var Module = fx.Module("qr",
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

// New builds the service and ties it to the application: Start when it starts,
// Stop when it shuts down.
func New(p Params) IQRServices {
	var opts []Option
	if p.Observer != nil {
		opts = append(opts, WithObserver(p.Observer))
	}
	svc := NewQRServices(p.Config.Secret, p.Inflight, opts...)
	utils.OnLifecycle(p.Lifecycle,
		func(ctx context.Context) error { return svc.Start(ctx) },
		func(ctx context.Context) error { return svc.Stop(ctx) },
	)
	return svc
}
