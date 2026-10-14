package config_test

import (
	"context"
	"testing"
	"time"
)

// Smoke: the whole lifecycle once, fast.
func TestSmoke(t *testing.T) {
	svc, reloads := started(t, map[string]string{".env": baseEnv + "APP_NAME=one\n"})
	ctx := context.Background()
	if cfg, err := svc.Get(ctx); err != nil || cfg.Name != "one" || cfg.DSN != "d" {
		t.Fatalf("Get = %+v, %v", cfg, err)
	}
	write(t, ".env", baseEnv+"APP_NAME=two\n")
	if err := nextReload(t, reloads); err != nil {
		t.Fatal(err)
	}
	if cfg, _ := svc.Get(ctx); cfg.Name != "two" {
		t.Fatalf("after reload name = %q", cfg.Name)
	}
	stop, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := svc.Stop(stop); err != nil {
		t.Fatal(err)
	}
}
