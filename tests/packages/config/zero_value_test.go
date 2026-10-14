package config_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/config"
)

// Zero value & absence: unstarted, stopped and nil services, restarting.

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

// The zero ConfigService (not from the constructor) and a nil one fail with
// errors, not panics.
func TestZeroValue_ServiceWithoutConstructor(t *testing.T) {
	setup(t, nil, required)
	ctx := context.Background()
	var zero config.ConfigService[appCfg]
	if _, err := zero.Get(ctx); !errors.Is(err, config.ErrNotLoaded) {
		t.Errorf("zero Get = %v, want ErrNotLoaded", err)
	}
	if err := zero.Run(ctx); !errors.Is(err, config.ErrNotLoaded) {
		t.Errorf("zero Run = %v, want ErrNotLoaded", err)
	}
	if err := zero.Start(ctx); err == nil {
		t.Error("zero Start succeeded without an encryption key")
	}
	if err := zero.Stop(ctx); err != nil {
		t.Errorf("zero Stop = %v", err)
	}
}

func TestZeroValue_NilReceiver(t *testing.T) {
	setup(t, nil, required)
	ctx := context.Background()
	var nilSvc *config.ConfigService[appCfg]
	if _, err := nilSvc.Get(ctx); !errors.Is(err, config.ErrNotLoaded) {
		t.Errorf("nil Get = %v", err)
	}
	if err := nilSvc.Start(ctx); !errors.Is(err, config.ErrNotLoaded) {
		t.Errorf("nil Start = %v", err)
	}
	if err := nilSvc.Run(ctx); !errors.Is(err, config.ErrNotLoaded) {
		t.Errorf("nil Run = %v", err)
	}
	if err := nilSvc.Stop(ctx); err != nil {
		t.Errorf("nil Stop = %v", err)
	}
}

func TestZeroValue_GetBeforeStartWrapsErrNotLoaded(t *testing.T) {
	svc, _ := config.NewConfigService[appCfg]()
	if _, err := svc.Get(context.Background()); !errors.Is(err, config.ErrNotLoaded) {
		t.Fatalf("Get = %v, want ErrNotLoaded", err)
	}
}

// Absent and zero values survive the encrypted round trip.
func TestZeroValue_AbsentAndZeroValuesRoundTrip(t *testing.T) {
	setup(t, nil, map[string]string{"DATABASE_URL": "d", "JWT_SECRET": "s", "MAX_CONNS": "0", "DEBUG": "false", "PORT": "0"})
	cfg := mustLoad(t)
	if cfg.MaxConns == nil || *cfg.MaxConns != 0 {
		t.Errorf("MAX_CONNS=0 became %v; a set pointer to zero must stay set", cfg.MaxConns)
	}
	if cfg.Port != 0 || cfg.Debug {
		t.Errorf("explicit zero values lost: %+v", cfg)
	}
	if cfg.Brokers != nil || cfg.Ports != nil {
		t.Errorf("absent slices should be nil: %v %v", cfg.Brokers, cfg.Ports)
	}
	if !cfg.Started.IsZero() {
		t.Errorf("absent time should be zero, got %v", cfg.Started)
	}
}

func TestZeroValue_EmptyStructTarget(t *testing.T) {
	setup(t, nil, nil)
	type empty struct{}
	svc, _ := config.NewConfigService[empty]()
	if err := svc.Start(context.Background()); err == nil {
		if _, err := svc.Get(context.Background()); err != nil {
			t.Fatalf("started but Get failed: %v", err)
		}
	}
}

// Stop then Run again starts a fresh watcher.
func TestZeroValue_RestartAfterStop(t *testing.T) {
	svc, reloads := started(t, map[string]string{config.EnvFile: baseEnv + "APP_NAME=a\n"})
	if err := svc.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := svc.Run(context.Background()); err != nil {
		t.Fatalf("Run after Stop: %v", err)
	}
	write(t, config.EnvFile, baseEnv+"APP_NAME=b\n")
	if err := nextReload(t, reloads); err != nil {
		t.Fatal(err)
	}
	if cfg, _ := svc.Get(context.Background()); cfg.Name != "b" {
		t.Fatalf("name = %q", cfg.Name)
	}
}

// Stop honours its own context when a reload callback is slow.
func TestZeroValue_StopRespectsItsContext(t *testing.T) {
	setup(t, map[string]string{config.EnvFile: baseEnv}, nil)
	svc, _ := config.NewConfigService[appCfg]()
	svc.ReloadInterval = 5 * time.Millisecond
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	svc.OnReload = func(error) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
	}
	_ = svc.Start(context.Background())
	_ = svc.Run(context.Background())
	write(t, config.EnvFile, baseEnv+"APP_NAME=x\n")
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := svc.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop = %v, want DeadlineExceeded", err)
	}
	close(release)
}
