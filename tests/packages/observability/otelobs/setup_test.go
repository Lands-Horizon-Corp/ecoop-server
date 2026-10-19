package otelobs_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	obs "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability/otelobs"
)

func TestSetup_WithNothingConfiguredStillWorks(t *testing.T) {
	t.Parallel()
	svc, be, logger, err := otelobs.Setup(env(map[string]string{}))
	if err != nil {
		t.Fatalf("Setup with an empty environment: %v", err)
	}
	if svc == nil || be == nil || logger == nil {
		t.Fatal("Setup returned a nil")
	}
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, op := svc.Begin(context.Background(), obs.ComponentSQL, "ping")
	op.End(nil)
	if err := svc.Stop(context.Background()); err != nil {
		t.Fatalf("Stop = %v", err)
	}
}

// A typo in any setting is an error at startup, and the message names the setting.
func TestSetup_RejectsBadSettings(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		vars map[string]string
		want string
	}{
		"log level":  {map[string]string{"LOG_LEVEL": "verbose"}, "log level"},
		"sampler":    {map[string]string{"OTEL_TRACES_SAMPLER": "sometimes"}, "sampler"},
		"endpoint":   {map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "collector:4318"}, "endpoint"},
		"plain http": {map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://c:4318"}, "http"},
	} {
		if _, _, _, err := otelobs.Setup(env(tc.vars)); err == nil {
			t.Errorf("%s: Setup accepted a bad setting", name)
		} else if !strings.Contains(strings.ToLower(err.Error()), tc.want) {
			t.Errorf("%s: error %q does not name the problem (%q)", name, err, tc.want)
		}
	}
}

// Under fx, the service starts with the app and Stop runs on shutdown: spans
// recorded just before the process exits are flushed, not lost.
func TestModule_FlushesOnShutdown(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_SDK_DISABLED", "")
	t.Setenv("LOG_LEVEL", "error")
	var svc obs.IObservabilityService
	var observer obs.IObserver
	app := fxtest.New(t, otelobs.Module, fx.Populate(&svc, &observer))
	app.RequireStart()
	if svc == nil || observer == nil {
		t.Fatal("the module did not provide the service and the observer")
	}
	_, op := observer.Begin(context.Background(), obs.ComponentSQL, "ping")
	op.End(nil)
	app.RequireStop()
	// Stop is idempotent: a second shutdown is not an error.
	if err := svc.Stop(context.Background()); err != nil {
		t.Errorf("second Stop = %v", err)
	}
}

// A bad configuration stops the application from starting, with the cause.
func TestModule_BadConfigurationFailsTheApp(t *testing.T) {
	t.Setenv("LOG_LEVEL", "verbose")
	app := fx.New(otelobs.Module, fx.Invoke(func(obs.IObserver) {}))
	err := app.Err()
	if err == nil {
		t.Fatal("the application started with LOG_LEVEL=verbose")
	}
	if !errors.Is(err, obs.ErrInvalidLogLevel) && !strings.Contains(err.Error(), "log level") {
		t.Errorf("the failure does not say why: %v", err)
	}
}

// A collector that never answers delays shutdown by at most ShutdownTimeout (10s
// by default), and does not make the application's shutdown fail.
func TestModule_StopIsBoundedWithADeadCollector(t *testing.T) {
	c := newCollector(t)
	c.delay.Store(int64(time.Minute))
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", c.srv.URL)
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "true")
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "300")
	t.Setenv("OTEL_BSP_SCHEDULE_DELAY", "10")
	t.Setenv("LOG_LEVEL", "error")
	var observer obs.IObserver
	app := fxtest.New(t, otelobs.Module, fx.Populate(&observer), fx.StopTimeout(20*time.Second))
	app.RequireStart()
	for range 100 {
		_, op := observer.Begin(context.Background(), obs.ComponentSQL, "ping")
		op.End(nil)
	}
	start := time.Now()
	app.RequireStop() // fails the test if the hook returned the flush error
	if took := time.Since(start); took > otelobs.DefaultShutdownTimeout+5*time.Second {
		t.Fatalf("shutdown took %v with a collector that never answers, want at most %v", took, otelobs.DefaultShutdownTimeout)
	}
}
