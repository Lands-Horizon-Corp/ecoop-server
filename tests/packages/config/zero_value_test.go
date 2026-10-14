package config_test

import (
	"context"
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
