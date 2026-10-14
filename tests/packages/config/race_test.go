package config_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

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

// Run, Stop, Start and Get hammered together from many goroutines.
func TestRace_LifecycleFromManyGoroutines(t *testing.T) {
	setup(t, map[string]string{config.EnvFile: baseEnv}, nil)
	svc, _ := config.NewConfigService[appCfg]()
	svc.ReloadInterval = time.Millisecond
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 6 {
		wg.Go(func() {
			for range 40 {
				_ = svc.Run(context.Background())
				_ = svc.Stop(context.Background())
			}
		})
		wg.Go(func() {
			for range 100 {
				if cfg, err := svc.Get(context.Background()); err != nil || cfg.DSN != "d" {
					t.Errorf("Get = %+v, %v", cfg, err)
					return
				}
			}
		})
	}
	wg.Go(func() {
		for i := range 30 {
			write(t, config.EnvFile, baseEnv+"PORT="+strings.Repeat("2", i%4+1)+"\n")
			time.Sleep(time.Millisecond)
		}
	})
	wg.Wait()
	_ = svc.Stop(context.Background())
}

// OnReload runs on the watcher goroutine; Get inside it must not deadlock.
func TestRace_GetFromInsideOnReload(t *testing.T) {
	setup(t, map[string]string{config.EnvFile: baseEnv}, nil)
	svc, _ := config.NewConfigService[appCfg]()
	svc.ReloadInterval = 5 * time.Millisecond
	got := make(chan string, 1)
	svc.OnReload = func(error) {
		cfg, _ := svc.Get(context.Background())
		select {
		case got <- cfg.Name:
		default:
		}
	}
	_ = svc.Start(context.Background())
	_ = svc.Run(context.Background())
	t.Cleanup(func() { _ = svc.Stop(context.Background()) })
	write(t, config.EnvFile, baseEnv+"APP_NAME=inside\n")
	select {
	case name := <-got:
		if name != "inside" {
			t.Fatalf("Get inside OnReload saw %q: the new config must be stored before the callback", name)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnReload never ran")
	}
}
