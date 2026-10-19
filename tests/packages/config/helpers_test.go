package config_test

import (
	"context"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/config"
)

type redisCfg struct {
	Host string `env:"HOST" envDefault:"localhost"`
	Port int    `env:"PORT" envDefault:"6379"`
}

type appCfg struct {
	Name     string        `env:"APP_NAME" envDefault:"ecoop"`
	Port     int           `env:"PORT" envDefault:"8080"`
	Debug    bool          `env:"DEBUG"`
	Timeout  time.Duration `env:"HTTP_TIMEOUT" envDefault:"30s"`
	Ratio    float64       `env:"RATIO"`
	Workers  uint8         `env:"WORKERS"`
	Brokers  []string      `env:"KAFKA_BROKERS"`
	Ports    []int         `env:"EXTRA_PORTS"`
	DSN      string        `env:"DATABASE_URL,required"`
	Secret   string        `env:"JWT_SECRET,required"`
	MaxConns *int          `env:"MAX_CONNS"`
	Bind     netip.Addr    `env:"BIND_ADDR" envDefault:"0.0.0.0"`
	Started  time.Time     `env:"STARTED_AT"`
	Redis    redisCfg      `envPrefix:"REDIS_"`
	Cache    *redisCfg     `envPrefix:"CACHE_"`
	//lint:ignore U1000 unexported on purpose: the loader must skip it
	ignored string
}

var keys = []string{"APP_NAME", "PORT", "DEBUG", "HTTP_TIMEOUT", "RATIO", "WORKERS", "KAFKA_BROKERS",
	"EXTRA_PORTS", "DATABASE_URL", "JWT_SECRET", "MAX_CONNS", "BIND_ADDR", "STARTED_AT",
	"REDIS_HOST", "REDIS_PORT", "CACHE_HOST", "CACHE_PORT"}

var required = map[string]string{"DATABASE_URL": "postgres://x", "JWT_SECRET": "s"}

// setup moves the test into a temp dir holding files and sets env over a
// cleared environment (an empty variable counts as unset).
func setup(t *testing.T, files, env map[string]string) {
	t.Helper()
	dir := t.TempDir()
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Chdir(dir)
	for _, k := range keys {
		t.Setenv(k, "")
	}
	for k, v := range env {
		t.Setenv(k, v)
	}
}

func load(t *testing.T) (appCfg, error) {
	t.Helper()
	svc, err := config.NewConfigService[appCfg]()
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(context.Background()); err != nil {
		return appCfg{}, err
	}
	return svc.Get(context.Background())
}

func mustLoad(t *testing.T) appCfg {
	t.Helper()
	cfg, err := load(t)
	if err != nil {
		t.Fatalf("Start/Get: %v", err)
	}
	return cfg
}

// --- Run: reload when the files change -------------------------------------

func started(t *testing.T, files map[string]string) (*config.ConfigService[appCfg], chan error) {
	t.Helper()
	setup(t, files, nil)
	svc, _ := config.NewConfigService[appCfg]()
	svc.ReloadInterval = 10 * time.Millisecond
	reloads := make(chan error, 64)
	svc.OnReload = func(err error) { reloads <- err }
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := svc.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Stop(context.Background()) })
	return svc, reloads
}

func write(t *testing.T, name, body string) {
	t.Helper()
	if err := os.WriteFile(name, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func nextReload(t *testing.T, reloads chan error) error {
	t.Helper()
	select {
	case err := <-reloads:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("no reload within 5s")
		return nil
	}
}

const baseEnv = "DATABASE_URL=d\nJWT_SECRET=s\n"
