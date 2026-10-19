package otp

import (
	"context"
	"os"

	"go.uber.org/fx"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/security"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sms"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/smtp"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Config is how an OTPService protects its codes.
type Config struct {
	// Secret is mixed into every stored code, so a leaked cache cannot be
	// brute-forced offline (a six-digit code has only a million values).
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

// Module provides IOTPService and starts it with the application, after the
// cache, security, mail and SMS services it uses.
var Module = fx.Module("otp",
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
	Security  security.ISecurityService
	Cache     cache.ICacheService
	SMTP      smtp.ISMTPService
	SMS       sms.ISMSServices
	Observer  observability.IObserver `optional:"true"`
	Inflight  *utils.Inflight         `optional:"true"`
}

// New builds the service and ties it to the application: Start when it starts,
// Stop when it shuts down.
func New(p Params) IOTPService {
	var opts []Option
	if p.Observer != nil {
		opts = append(opts, WithObserver(p.Observer))
	}
	svc := NewOTPService(p.Config.Secret, p.Security, p.Cache, p.SMTP, p.SMS, p.Inflight, opts...)
	utils.OnLifecycle(p.Lifecycle,
		func(ctx context.Context) error { return svc.Start(ctx) },
		func(ctx context.Context) error { return svc.Stop(ctx) },
	)
	return svc
}
