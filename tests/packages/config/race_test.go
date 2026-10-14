package config_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/config"
)

// Sync & race: run with -race.

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
