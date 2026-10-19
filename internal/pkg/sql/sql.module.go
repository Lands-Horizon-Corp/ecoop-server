package sql

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"

	"go.uber.org/fx"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Config is how an SQLService reaches one Postgres database.
type Config struct {
	// DSN is the connection string, postgres://user:password@host:port/db.
	DSN string
	// MaxOpenConns and MaxIdleConns size the pool.
	MaxOpenConns int
	MaxIdleConns int
	// MigrationsDir is the directory of goose .sql files; empty means the
	// service cannot run migration commands.
	MigrationsDir string
	// AutoMigrate applies pending migrations when the service runs.
	AutoMigrate bool
}

// FromEnv builds the Config of the main (writer) database from the environment:
//
//	POSTGRES_HOST             default 127.0.0.1
//	POSTGRES_PORT             default 5432
//	POSTGRES_USER             required
//	POSTGRES_PASSWORD         required
//	POSTGRES_DB               required
//	POSTGRES_SSLMODE          default disable
//	POSTGRES_MAX_CONNECTIONS  default 20
//	POSTGRES_MIGRATIONS_DIR   default empty
//	POSTGRES_AUTO_MIGRATE     default false
func FromEnv(lookup func(string) (string, bool)) (Config, error) {
	return fromEnvWithPrefix(lookup, "POSTGRES")
}

// ReadFromEnv builds the Config of the read-model database. It uses the same
// settings under POSTGRES_READ_ (POSTGRES_READ_HOST, _PORT, _USER, _PASSWORD,
// _DB, _SSLMODE, _MAX_CONNECTIONS); the read database is never migrated by the
// service, so it has no migrations directory or auto-migrate setting.
func ReadFromEnv(lookup func(string) (string, bool)) (Config, error) {
	cfg, err := fromEnvWithPrefix(lookup, "POSTGRES_READ")
	cfg.MigrationsDir, cfg.AutoMigrate = "", false
	return cfg, err
}

func fromEnvWithPrefix(lookup func(string) (string, bool), prefix string) (Config, error) {
	e := utils.NewEnv(lookup)
	port := "5432"
	if prefix == "POSTGRES_READ" {
		port = "5433"
	}
	u := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(e.Required(prefix+"_USER"), e.Required(prefix+"_PASSWORD")),
		Host:   net.JoinHostPort(e.String(prefix+"_HOST", "127.0.0.1"), e.String(prefix+"_PORT", port)),
		Path:   "/" + e.Required(prefix+"_DB"),
	}
	q := url.Values{}
	q.Set("sslmode", e.String(prefix+"_SSLMODE", "disable"))
	u.RawQuery = q.Encode()

	open := e.Int(prefix+"_MAX_CONNECTIONS", 20)
	if open < 1 {
		e.Invalid(prefix+"_MAX_CONNECTIONS", "at least 1")
	}
	cfg := Config{
		DSN:           u.String(),
		MaxOpenConns:  open,
		MaxIdleConns:  max(1, open/4),
		MigrationsDir: e.String(prefix+"_MIGRATIONS_DIR", ""),
		AutoMigrate:   e.Bool(prefix+"_AUTO_MIGRATE", false),
	}
	return cfg, e.Err()
}

// Module provides the main database as ISQLService. The models its migrations
// diff against come from the application: supply them with fx.Supply or a
// provider of Models. The read-model database for CQRS is built the same way
// with NewFromConfig and ReadFromEnv; see the package documentation.
var Module = fx.Module("sql",
	fx.Provide(
		func() (Config, error) { return FromEnv(os.LookupEnv) },
		New,
	),
)

// Models are the Bun models the service diffs its migrations against. Provide
// them once: fx.Supply(sql.Models{&User{}, &Member{}}).
type Models []any

// Params is what New needs from the application.
type Params struct {
	fx.In

	Lifecycle fx.Lifecycle
	Config    Config
	Models    Models                  `optional:"true"`
	Output    io.Writer               `optional:"true"`
	Observer  observability.IObserver `optional:"true"`
	Inflight  *utils.Inflight         `optional:"true"`
}

// New builds the main database service from its Config and ties it to the
// application: Start and then Run (which migrates, when AutoMigrate is on) when
// it starts, Stop when it shuts down.
func New(p Params) (ISQLService, error) {
	return NewFromConfig(p.Lifecycle, p.Config, p.Models, p.Output, p.Observer, p.Inflight)
}

// NewFromConfig builds a database service from a Config and ties it to the
// lifecycle. It is how a second database (the CQRS read model, from
// ReadFromEnv) is built next to the main one. An empty MigrationsDir gives a
// service without a migrations directory. output, observer and inflight may be
// nil.
func NewFromConfig(lc fx.Lifecycle, c Config, models Models, output io.Writer, o observability.IObserver, inflight *utils.Inflight) (ISQLService, error) {
	var dir *os.File
	if c.MigrationsDir != "" {
		f, err := os.Open(c.MigrationsDir)
		if err != nil {
			return nil, fmt.Errorf("sql: opening the migrations directory: %w", err)
		}
		dir = f
	}
	var opts []Option
	if o != nil {
		opts = append(opts, WithObserver(o))
	}
	svc := NewSQLService(c.DSN, c.MaxIdleConns, c.MaxOpenConns, dir, c.AutoMigrate, output, models, inflight, opts...)
	utils.OnLifecycle(lc,
		utils.Sequence(
			func(ctx context.Context) error { return svc.Start(ctx) },
			func(ctx context.Context) error { return svc.Run(ctx) },
		),
		func(ctx context.Context) error {
			err := svc.Stop(ctx)
			if dir != nil {
				_ = dir.Close()
			}
			return err
		},
	)
	return svc, nil
}
