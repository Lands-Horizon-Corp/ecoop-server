package config_test

import (
	"context"
	"net/netip"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/config"
)

// Happy path: defaults, every field type, source precedence, YAML shapes and
// reloads that succeed.

func TestDefaultsAndEnvironment(t *testing.T) {
	setup(t, nil, required)
	cfg := mustLoad(t)
	if cfg.Name != "ecoop" || cfg.Port != 8080 || cfg.Timeout != 30*time.Second ||
		cfg.Redis.Host != "localhost" || cfg.Redis.Port != 6379 || cfg.Bind.String() != "0.0.0.0" {
		t.Fatalf("defaults wrong: %+v", cfg)
	}
	if cfg.Cache == nil || cfg.Cache.Host != "localhost" {
		t.Fatalf("a pointer struct whose fields have defaults is populated with them, got %+v", cfg.Cache)
	}
	if cfg.DSN != "postgres://x" || cfg.Secret != "s" {
		t.Fatalf("required values wrong: %+v", cfg)
	}
}

func TestEveryFieldType(t *testing.T) {
	setup(t, nil, map[string]string{
		"DATABASE_URL": "d", "JWT_SECRET": "s",
		"DEBUG": "true", "HTTP_TIMEOUT": "2m30s", "RATIO": "0.25", "WORKERS": "200",
		"KAFKA_BROKERS": "a:1, b:2,,", "EXTRA_PORTS": "81, 82", "MAX_CONNS": "42",
		"BIND_ADDR": "10.0.0.7", "STARTED_AT": "2026-01-02T03:04:05Z",
		"REDIS_HOST": "r", "REDIS_PORT": "7000", "CACHE_HOST": "c",
	})
	cfg := mustLoad(t)
	checks := map[string]bool{
		"bool":            cfg.Debug,
		"duration":        cfg.Timeout == 150*time.Second,
		"float":           cfg.Ratio == 0.25,
		"uint8":           cfg.Workers == 200,
		"string slice":    strings.Join(cfg.Brokers, "|") == "a:1|b:2",
		"int slice":       len(cfg.Ports) == 2 && cfg.Ports[0] == 81 && cfg.Ports[1] == 82,
		"pointer":         cfg.MaxConns != nil && *cfg.MaxConns == 42,
		"TextUnmarshaler": cfg.Bind == netip.MustParseAddr("10.0.0.7"),
		"time.Time":       cfg.Started.Equal(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)),
		"nested prefix":   cfg.Redis.Host == "r" && cfg.Redis.Port == 7000,
		"pointer struct":  cfg.Cache != nil && cfg.Cache.Host == "c" && cfg.Cache.Port == 6379,
	}
	for what, ok := range checks {
		if !ok {
			t.Errorf("%s not loaded: %+v", what, cfg)
		}
	}
}

func TestSourcesOverrideInOrder(t *testing.T) {
	files := map[string]string{
		config.EnvFile:     "PORT=1001\nAPP_NAME=from-dotenv\nDATABASE_URL=d\nJWT_SECRET=s\n",
		config.HelmFile:    "env:\n  - name: PORT\n    value: \"1002\"\n  - name: APP_NAME\n    value: from-helm\n",
		config.SecretsFile: "PORT: 1003\n",
	}
	t.Run("secrets beat helm beat .env", func(t *testing.T) {
		setup(t, files, nil)
		cfg := mustLoad(t)
		if cfg.Port != 1003 || cfg.Name != "from-helm" {
			t.Fatalf("port=%d name=%q", cfg.Port, cfg.Name)
		}
	})
	t.Run("the environment beats every file", func(t *testing.T) {
		setup(t, files, map[string]string{"PORT": "1004"})
		if cfg := mustLoad(t); cfg.Port != 1004 {
			t.Fatalf("port=%d", cfg.Port)
		}
	})
	t.Run("an empty variable falls through to files", func(t *testing.T) {
		setup(t, files, map[string]string{"APP_NAME": ""})
		if cfg := mustLoad(t); cfg.Name != "from-helm" {
			t.Fatalf("name=%q", cfg.Name)
		}
	})
	t.Run("a file value beats the default", func(t *testing.T) {
		setup(t, map[string]string{config.EnvFile: "DATABASE_URL=d\nJWT_SECRET=s\nHTTP_TIMEOUT=5s\n"}, nil)
		if cfg := mustLoad(t); cfg.Timeout != 5*time.Second {
			t.Fatalf("timeout=%v", cfg.Timeout)
		}
	})
	t.Run("missing files are fine", func(t *testing.T) {
		setup(t, nil, required)
		mustLoad(t)
	})
}

func TestYAMLShapes(t *testing.T) {
	setup(t, map[string]string{
		config.HelmFile: `
database: {url: postgres://nested}
redis: {host: redis.internal, port: 6380}
kafkaBrokers: [k1:9092, k2:9092]
HTTPTimeout: 2m
env: {DEBUG: "true", BIND_ADDR: 10.0.0.7, CACHE_HOST: cache}
`,
		config.SecretsFile: `
kind: Secret
stringData: {JWT_SECRET: plain-secret}
data: {MAX_CONNS: NDI=}   # base64 of 42
---
EXTRA_PORTS: "81, 82"
`,
	}, nil)
	cfg := mustLoad(t)
	checks := map[string]bool{
		"nested map":         cfg.DSN == "postgres://nested" && cfg.Redis.Host == "redis.internal" && cfg.Redis.Port == 6380,
		"camelCase list":     strings.Join(cfg.Brokers, "|") == "k1:9092|k2:9092",
		"acronym key":        cfg.Timeout == 2*time.Minute,
		"env map":            cfg.Debug && cfg.Bind.String() == "10.0.0.7",
		"Secret stringData":  cfg.Secret == "plain-secret",
		"Secret base64 data": cfg.MaxConns != nil && *cfg.MaxConns == 42,
		"second YAML doc":    len(cfg.Ports) == 2 && cfg.Ports[1] == 82,
		"pointer nested":     cfg.Cache != nil && cfg.Cache.Host == "cache",
	}
	for what, ok := range checks {
		if !ok {
			t.Errorf("%s not loaded: %+v", what, cfg)
		}
	}
}

func TestPointerStructStaysNilWithoutValues(t *testing.T) {
	type opt struct {
		Host string `env:"HOST"`
	}
	type cfg struct {
		Opt *opt `envPrefix:"OPT_"`
	}
	setup(t, nil, nil)
	svc, _ := config.NewConfigService[cfg]()
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, _ := svc.Get(context.Background()); got.Opt != nil {
		t.Fatalf("Opt = %+v, want nil", got.Opt)
	}
	t.Setenv("OPT_HOST", "h")
	_ = svc.Start(context.Background())
	if got, _ := svc.Get(context.Background()); got.Opt == nil || got.Opt.Host != "h" {
		t.Fatalf("Opt = %+v", got.Opt)
	}
}

func TestRun_ReloadsWhenAFileChanges(t *testing.T) {
	svc, reloads := started(t, map[string]string{config.EnvFile: baseEnv + "APP_NAME=one\n"})
	write(t, config.EnvFile, baseEnv+"APP_NAME=two\n")
	if err := nextReload(t, reloads); err != nil {
		t.Fatal(err)
	}
	if cfg, _ := svc.Get(context.Background()); cfg.Name != "two" {
		t.Fatalf("name = %q, want two", cfg.Name)
	}
}

func TestRun_PicksUpAFileThatAppearsAndOneThatIsRemoved(t *testing.T) {
	svc, reloads := started(t, map[string]string{config.EnvFile: baseEnv})
	write(t, config.SecretsFile, "PORT: 9000\n")
	if err := nextReload(t, reloads); err != nil {
		t.Fatal(err)
	}
	if cfg, _ := svc.Get(context.Background()); cfg.Port != 9000 {
		t.Fatalf("port = %d", cfg.Port)
	}
	if err := os.Remove(config.SecretsFile); err != nil {
		t.Fatal(err)
	}
	if err := nextReload(t, reloads); err != nil {
		t.Fatal(err)
	}
	if cfg, _ := svc.Get(context.Background()); cfg.Port != 8080 {
		t.Fatalf("port after removal = %d, want the default", cfg.Port)
	}
}

func TestRun_DoesNotReloadWithoutAChange(t *testing.T) {
	_, reloads := started(t, map[string]string{config.EnvFile: baseEnv})
	select {
	case err := <-reloads:
		t.Fatalf("reloaded with no change: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
}
