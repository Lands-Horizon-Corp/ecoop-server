package api_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/api"
)

// freeAddr returns a loopback address that was free a moment ago.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// The module reads the environment, registers nothing it was not given, serves
// after OnStart and stops cleanly at OnStop. These tests set environment
// variables, so they do not run in parallel.
func TestModule_ServesAfterStartAndStopsCleanly(t *testing.T) {
	addr := freeAddr(t)
	t.Setenv("API_ADDR", addr)

	var svc api.IAPIService
	app := fxtest.New(t, fx.NopLogger, api.Module, fx.Populate(&svc))
	if err := svc.RegisterRoute(api.Route{Route: "/hello", Method: "GET"}, nil); !errors.Is(err, api.ErrNilHandler) {
		t.Fatalf("the module should hand out the service before it starts: %v", err)
	}
	app.RequireStart()

	resp, err := http.Get("http://" + addr + api.LivenessPath)
	if err != nil {
		t.Fatalf("the module did not start serving: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("%s = %d", api.LivenessPath, resp.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := app.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	fresh := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	if r, err := fresh.Get("http://" + addr + api.LivenessPath); err == nil {
		r.Body.Close()
		t.Error("the server still accepts connections after the application stopped")
	}
}

// An unsafe environment stops the application at start; nothing listens.
func TestModule_AnUnsafeEnvironmentStopsStartup(t *testing.T) {
	addr := freeAddr(t)
	t.Setenv("API_ADDR", addr)
	t.Setenv("API_SECURED", "true") // no origins: refused

	app := fx.New(fx.NopLogger, api.Module)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := app.Start(ctx)
	if err == nil {
		_ = app.Stop(ctx)
		t.Fatal("a Secured API with no origins started")
	}
	if !errors.Is(err, api.ErrNoOrigins) {
		t.Errorf("start error = %v, want ErrNoOrigins", err)
	}
	if c, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		c.Close()
		t.Error("something is listening although startup was refused")
	}
}

func TestModule_AMalformedEnvironmentValueStopsStartup(t *testing.T) {
	t.Setenv("API_READ_TIMEOUT", "fifteen")
	app := fx.New(fx.NopLogger, api.Module)
	err := app.Err()
	if err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = app.Start(ctx)
		if err == nil {
			_ = app.Stop(ctx)
		}
	}
	if !errors.Is(err, api.ErrInvalidLimit) {
		t.Fatalf("a malformed API_READ_TIMEOUT gave %v, want ErrInvalidLimit", err)
	}
}
