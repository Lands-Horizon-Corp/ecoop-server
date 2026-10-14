package config_test

import (
	"context"
	"errors"
	"maps"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
	ignored  string
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

func TestStartErrors(t *testing.T) {
	t.Run("every missing required key is listed", func(t *testing.T) {
		setup(t, nil, nil)
		_, err := load(t)
		if !errors.Is(err, config.ErrMissing) || !strings.Contains(err.Error(), "DATABASE_URL") || !strings.Contains(err.Error(), "JWT_SECRET") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("an unparsable value names the key but not the value", func(t *testing.T) {
		env := map[string]string{"PORT": "s3cr3t-port"}
		maps.Copy(env, required)
		setup(t, nil, env)
		_, err := load(t)
		if !errors.Is(err, config.ErrInvalidValue) || !strings.Contains(err.Error(), "PORT") || strings.Contains(err.Error(), "s3cr3t") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("bad values per type", func(t *testing.T) {
		for k, v := range map[string]string{
			"DEBUG": "maybe", "HTTP_TIMEOUT": "soon", "RATIO": "x", "WORKERS": "300",
			"EXTRA_PORTS": "1,x", "BIND_ADDR": "not-an-ip", "STARTED_AT": "yesterday", "MAX_CONNS": "many",
		} {
			env := map[string]string{k: v}
			maps.Copy(env, required)
			setup(t, nil, env)
			if _, err := load(t); !errors.Is(err, config.ErrInvalidValue) || !strings.Contains(err.Error(), k) {
				t.Errorf("%s=%q: err = %v", k, v, err)
			}
		}
	})
	t.Run("a malformed .env or yaml file is an error", func(t *testing.T) {
		setup(t, map[string]string{config.HelmFile: "a: [unclosed\n"}, required)
		if _, err := load(t); err == nil || !strings.Contains(err.Error(), config.HelmFile) {
			t.Fatalf("err = %v", err)
		}
		setup(t, map[string]string{config.SecretsFile: "kind: Secret\ndata: {X: '!!notbase64'}\n"}, required)
		if _, err := load(t); err == nil || !strings.Contains(err.Error(), config.SecretsFile) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("T must be a struct", func(t *testing.T) {
		setup(t, nil, nil)
		svc, _ := config.NewConfigService[map[string]string]()
		if err := svc.Start(context.Background()); !errors.Is(err, config.ErrNotStruct) {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestGet(t *testing.T) {
	t.Run("before Start is an error", func(t *testing.T) {
		svc, _ := config.NewConfigService[appCfg]()
		if _, err := svc.Get(context.Background()); err == nil {
			t.Fatal("Get before Start succeeded")
		}
	})
	t.Run("after a failed Start is still an error", func(t *testing.T) {
		setup(t, nil, nil)
		svc, _ := config.NewConfigService[appCfg]()
		if err := svc.Start(context.Background()); err == nil {
			t.Fatal("Start should fail")
		}
		if _, err := svc.Get(context.Background()); err == nil {
			t.Fatal("Get after a failed Start succeeded")
		}
	})
	t.Run("returns independent copies", func(t *testing.T) {
		setup(t, nil, map[string]string{"DATABASE_URL": "d", "JWT_SECRET": "jwt", "KAFKA_BROKERS": "a", "MAX_CONNS": "1"})
		svc, _ := config.NewConfigService[appCfg]()
		if err := svc.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		first, _ := svc.Get(context.Background())
		first.Secret = "changed"
		first.Brokers[0] = "changed"
		*first.MaxConns = 99
		second, _ := svc.Get(context.Background())
		if second.Secret != "jwt" || second.Brokers[0] != "a" || *second.MaxConns != 1 {
			t.Fatalf("caller's change leaked: %+v", second)
		}
	})
	t.Run("keeps the Start snapshot after the environment changes", func(t *testing.T) {
		setup(t, nil, required)
		svc, _ := config.NewConfigService[appCfg]()
		if err := svc.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		t.Setenv("JWT_SECRET", "rotated")
		if cfg, _ := svc.Get(context.Background()); cfg.Secret != "s" {
			t.Fatalf("secret = %q", cfg.Secret)
		}
	})
	t.Run("a second Start reloads", func(t *testing.T) {
		setup(t, nil, required)
		svc, _ := config.NewConfigService[appCfg]()
		_ = svc.Start(context.Background())
		t.Setenv("JWT_SECRET", "rotated")
		if err := svc.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		if cfg, _ := svc.Get(context.Background()); cfg.Secret != "rotated" {
			t.Fatalf("secret = %q", cfg.Secret)
		}
	})
	t.Run("services do not share keys", func(t *testing.T) {
		setup(t, nil, required)
		a, _ := config.NewConfigService[appCfg]()
		b, _ := config.NewConfigService[appCfg]()
		if err := errors.Join(a.Start(context.Background()), b.Start(context.Background())); err != nil {
			t.Fatal(err)
		}
		for _, s := range []*config.ConfigService[appCfg]{a, b} {
			if cfg, err := s.Get(context.Background()); err != nil || cfg.DSN != "postgres://x" {
				t.Fatalf("Get = %+v, %v", cfg, err)
			}
		}
	})
}

func TestConcurrentGetAndStart(t *testing.T) {
	setup(t, nil, required)
	svc, _ := config.NewConfigService[appCfg]()
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			for range 50 {
				if cfg, err := svc.Get(context.Background()); err != nil || cfg.DSN != "postgres://x" {
					t.Errorf("Get = %+v, %v", cfg, err)
					return
				}
			}
		})
	}
	wg.Go(func() {
		for range 5 {
			if err := svc.Start(context.Background()); err != nil {
				t.Errorf("Start: %v", err)
			}
		}
	})
	wg.Wait()
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

func TestRun_BadReloadKeepsThePreviousConfigThenRecovers(t *testing.T) {
	svc, reloads := started(t, map[string]string{config.EnvFile: baseEnv + "APP_NAME=good\n"})

	write(t, config.EnvFile, baseEnv+"PORT=not-a-number\nAPP_NAME=bad\n")
	if err := nextReload(t, reloads); !errors.Is(err, config.ErrInvalidValue) {
		t.Fatalf("reload error = %v, want ErrInvalidValue", err)
	}
	if cfg, err := svc.Get(context.Background()); err != nil || cfg.Name != "good" {
		t.Fatalf("after a bad reload: %+v, %v; the old config must stay", cfg, err)
	}

	write(t, config.EnvFile, "APP_NAME=missing-required\n")
	if err := nextReload(t, reloads); !errors.Is(err, config.ErrMissing) {
		t.Fatalf("reload error = %v, want ErrMissing", err)
	}

	write(t, config.EnvFile, baseEnv+"APP_NAME=fixed\n")
	if err := nextReload(t, reloads); err != nil {
		t.Fatal(err)
	}
	if cfg, _ := svc.Get(context.Background()); cfg.Name != "fixed" {
		t.Fatalf("name = %q, want fixed", cfg.Name)
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

func TestRun_StopsOnStopAndOnContextCancel(t *testing.T) {
	t.Run("Stop", func(t *testing.T) {
		svc, reloads := started(t, map[string]string{config.EnvFile: baseEnv})
		if err := svc.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
		write(t, config.EnvFile, baseEnv+"APP_NAME=late\n")
		select {
		case <-reloads:
			t.Fatal("reloaded after Stop")
		case <-time.After(200 * time.Millisecond):
		}
		if err := svc.Stop(context.Background()); err != nil {
			t.Fatalf("second Stop = %v", err)
		}
	})
	t.Run("context cancel", func(t *testing.T) {
		setup(t, map[string]string{config.EnvFile: baseEnv}, nil)
		svc, _ := config.NewConfigService[appCfg]()
		svc.ReloadInterval = 10 * time.Millisecond
		reloads := make(chan error, 8)
		svc.OnReload = func(err error) { reloads <- err }
		_ = svc.Start(context.Background())
		ctx, cancel := context.WithCancel(context.Background())
		if err := svc.Run(ctx); err != nil {
			t.Fatal(err)
		}
		cancel()
		time.Sleep(50 * time.Millisecond)
		write(t, config.EnvFile, baseEnv+"APP_NAME=late\n")
		select {
		case <-reloads:
			t.Fatal("reloaded after the context was cancelled")
		case <-time.After(200 * time.Millisecond):
		}
	})
	t.Run("Stop without Run", func(t *testing.T) {
		svc, _ := config.NewConfigService[appCfg]()
		if err := svc.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestRun_RequiresStartAndIsIdempotent(t *testing.T) {
	setup(t, nil, required)
	svc, _ := config.NewConfigService[appCfg]()
	if err := svc.Run(context.Background()); !errors.Is(err, config.ErrNotLoaded) {
		t.Fatalf("Run before Start = %v, want ErrNotLoaded", err)
	}
	_ = svc.Start(context.Background())
	for range 2 {
		if err := svc.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if err := svc.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRun_GetIsSafeDuringReloads(t *testing.T) {
	svc, reloads := started(t, map[string]string{config.EnvFile: baseEnv})
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 8 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				if cfg, err := svc.Get(context.Background()); err != nil || cfg.DSN != "d" {
					t.Errorf("Get = %+v, %v", cfg, err)
					return
				}
			}
		})
	}
	for i := range 5 {
		write(t, config.EnvFile, baseEnv+"PORT="+strings.Repeat("1", i+1)+"\n")
		nextReload(t, reloads)
	}
	close(stop)
	wg.Wait()
}
