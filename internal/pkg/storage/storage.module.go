package storage

import (
	"context"
	"net"
	"os"
	"time"

	"go.uber.org/fx"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Config is how a StorageService reaches its bucket.
type Config struct {
	// Driver is the kind of server: DriverS3 for AWS S3, anything else for an
	// S3-compatible server addressed by path (RustFS, MinIO).
	Driver string
	// Endpoint is the server's host:port; PublicEndpoint, when set, is the
	// address browsers use to download.
	Endpoint       string
	PublicEndpoint string
	AccessKey      string
	SecretKey      string
	Bucket         string
	Region         string
	// SSL makes the connection https.
	SSL bool
	// MaxFileSize caps every upload; URLTimeout bounds a download from a URL.
	MaxFileSize int64
	URLTimeout  time.Duration
}

// FromEnv builds the Config from the environment:
//
//	STORAGE_DRIVER           default rustfs
//	STORAGE_HOST             default 127.0.0.1
//	STORAGE_PORT             default 9000
//	STORAGE_PUBLIC_ENDPOINT  default empty
//	STORAGE_ACCESS_KEY       required
//	STORAGE_SECRET_KEY       required
//	STORAGE_BUCKET           required
//	STORAGE_REGION           default us-east-1
//	STORAGE_SECURED          default false
//	STORAGE_MAX_FILE_SIZE    default 104857600 (100 MiB)
//	STORAGE_URL_TIMEOUT      default 30s
func FromEnv(lookup func(string) (string, bool)) (Config, error) {
	e := utils.NewEnv(lookup)
	cfg := Config{
		Driver:         e.String("STORAGE_DRIVER", "rustfs"),
		Endpoint:       net.JoinHostPort(e.String("STORAGE_HOST", "127.0.0.1"), e.String("STORAGE_PORT", "9000")),
		PublicEndpoint: e.String("STORAGE_PUBLIC_ENDPOINT", ""),
		AccessKey:      e.Required("STORAGE_ACCESS_KEY"),
		SecretKey:      e.Required("STORAGE_SECRET_KEY"),
		Bucket:         e.Required("STORAGE_BUCKET"),
		Region:         e.String("STORAGE_REGION", DefaultRegion),
		SSL:            e.Bool("STORAGE_SECURED", false),
		MaxFileSize:    e.Int64("STORAGE_MAX_FILE_SIZE", DefaultMaxFileSize),
		URLTimeout:     e.Duration("STORAGE_URL_TIMEOUT", DefaultURLTimeout),
	}
	if cfg.MaxFileSize < 1 {
		e.Invalid("STORAGE_MAX_FILE_SIZE", "at least 1")
	}
	return cfg, e.Err()
}

// Module provides IStorageService and runs it with the application.
var Module = fx.Module("storage",
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
// when it starts (connect, create the bucket), Stop when it shuts down.
func New(p Params) IStorageService {
	c := p.Config
	opts := []Option{WithMaxFileSize(c.MaxFileSize), WithURLTimeout(c.URLTimeout)}
	if p.Observer != nil {
		opts = append(opts, WithObserver(p.Observer))
	}
	svc := NewStorageService(c.Driver, c.Endpoint, c.PublicEndpoint, c.AccessKey, c.SecretKey, c.Bucket, c.Region, c.SSL, p.Inflight, opts...)
	utils.OnLifecycle(p.Lifecycle,
		func(ctx context.Context) error { return svc.Run(ctx) },
		func(ctx context.Context) error { return svc.Stop(ctx) },
	)
	return svc
}
