package pdf

import (
	"context"
	"os"

	"go.uber.org/fx"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Config is how a PDFEngineService reaches the rendering engine.
type Config struct {
	// URL is the engine's base address, e.g. http://127.0.0.1:6767.
	URL string
	// Token is the engine's bearer token.
	Token string
}

// FromEnv builds the Config from the environment:
//
//	PDF_ENGINE_API_URL           the full address, when set
//	PDF_ENGINE_API_HOST / _PORT  used when the URL is not set (127.0.0.1, 6767)
//	PDF_ENGINE_API_BEARER_TOKEN  required
func FromEnv(lookup func(string) (string, bool)) (Config, error) {
	e := utils.NewEnv(lookup)
	url := e.String("PDF_ENGINE_API_URL", "")
	if url == "" {
		url = "http://" + e.String("PDF_ENGINE_API_HOST", "127.0.0.1") + ":" + e.String("PDF_ENGINE_API_PORT", "6767")
	}
	cfg := Config{URL: url, Token: e.Required("PDF_ENGINE_API_BEARER_TOKEN")}
	return cfg, e.Err()
}

// Module provides IPDFServices and starts it with the application.
var Module = fx.Module("pdf",
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

// New builds the service and ties it to the application: Start when it starts
// (checks the engine is up), Stop when it shuts down.
func New(p Params) IPDFServices {
	var opts []Option
	if p.Observer != nil {
		opts = append(opts, WithObserver(p.Observer))
	}
	svc := NewPDFService(p.Config.URL, p.Config.Token, p.Inflight, opts...)
	utils.OnLifecycle(p.Lifecycle,
		func(ctx context.Context) error { return svc.Start(ctx) },
		func(ctx context.Context) error { return svc.Stop(ctx) },
	)
	return svc
}
