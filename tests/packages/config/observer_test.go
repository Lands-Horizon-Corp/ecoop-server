package config_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/config"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/tests/internal/obstest"
)

func newObserver(t testing.TB) *observability.Service {
	t.Helper()
	return obstest.New(t)
}

func configCount(o *observability.Service, op, outcome string) float64 {
	return obstest.Count(o, "config", op, outcome)
}

// A good load is a success; a missing required variable is an error.
func TestObserver_LoadIsAnOperation(t *testing.T) {
	setup(t, nil, required)
	o := newObserver(t)
	svc, err := config.NewConfigService[appCfg](config.WithObserver(o))
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := configCount(o, "load", "success"); got != 1 {
		t.Fatalf("load success = %v, want 1", got)
	}

	setup(t, nil, nil) // DATABASE_URL and JWT_SECRET missing
	bad, _ := config.NewConfigService[appCfg](config.WithObserver(o))
	if err := bad.Start(context.Background()); err == nil {
		t.Fatal("a config with missing required variables loaded")
	}
	if got := configCount(o, "load", "error"); got != 1 {
		t.Fatalf("load error = %v, want 1", got)
	}
}

// A reload after a file change is counted as its own operation.
func TestObserver_ReloadIsAnOperation(t *testing.T) {
	setup(t, map[string]string{".env": "APP_NAME=one\nDATABASE_URL=postgres://x\nJWT_SECRET=s\n"}, nil)
	o := newObserver(t)
	svc, _ := config.NewConfigService[appCfg](config.WithObserver(o))
	svc.ReloadInterval = 10 * time.Millisecond
	reloads := make(chan error, 8)
	svc.OnReload = func(err error) { reloads <- err }
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := svc.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Stop(context.Background()) })
	write(t, ".env", "APP_NAME=two\nDATABASE_URL=postgres://x\nJWT_SECRET=s\n")
	if err := nextReload(t, reloads); err != nil {
		t.Fatal(err)
	}
	if got := configCount(o, "reload", "success"); got != 1 {
		t.Fatalf("reload success = %v, want 1", got)
	}
}

// Reading the config on a hot path writes nothing.
func TestObserver_GetWritesNothing(t *testing.T) {
	setup(t, nil, required)
	o := newObserver(t)
	svc, _ := config.NewConfigService[appCfg](config.WithObserver(o))
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	before := len(o.Snapshot())
	for range 100 {
		if _, err := svc.Get(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if after := len(o.Snapshot()); after != before {
		t.Fatalf("Get wrote to the registry: %d series before, %d after", before, after)
	}
}

// Neither a variable name, a value nor a path reaches the registry.
func TestObserver_NoNameOrValueIsRecorded(t *testing.T) {
	setup(t, nil, map[string]string{"DATABASE_URL": "postgres://user:hunter2@db/x", "JWT_SECRET": "top-secret-signing-key"})
	o := newObserver(t)
	svc, _ := config.NewConfigService[appCfg](config.WithObserver(o))
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, sm := range o.Snapshot() {
		blob := sm.Name
		for k, v := range sm.Attrs {
			blob += " " + k + "=" + v
		}
		for _, leak := range []string{"hunter2", "top-secret", "DATABASE_URL", "JWT_SECRET", ".env"} {
			if strings.Contains(blob, leak) {
				t.Fatalf("%q leaked into the registry: %s", leak, blob)
			}
		}
	}
}

// No observer, and a bare struct, are fine.
func TestObserver_NoneIsFine(t *testing.T) {
	setup(t, nil, required)
	svc, _ := config.NewConfigService[appCfg]()
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	var zero config.ConfigService[appCfg]
	if err := zero.Start(context.Background()); err == nil {
		t.Fatal("a zero service must return an error, not panic")
	}
}
