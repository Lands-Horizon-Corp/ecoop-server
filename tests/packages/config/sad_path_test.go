package config_test

import (
	"context"
	"errors"
	"maps"
	"strings"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/config"
)

// Sad path: invalid or missing configuration, Get before Start, and reloads
// that fail while the previous config keeps serving.

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
