package otelobs

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"go.uber.org/fx"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
)

// Setup is everything main needs to turn observability on: it reads the
// environment, validates it, opens the exporter and returns the service. A
// malformed value is an error here, at startup, and never a silent default.
//
//	APP_NAME            service.name (default ecoop-server)
//	APP_VERSION         service.version
//	APP_ENV             deployment.environment.name
//	LOG_LEVEL           debug, info, warn or error (default info)
//	OTEL_*              see FromEnv
//
// With no OTEL_EXPORTER_OTLP_ENDPOINT it still returns a working service that
// keeps its metrics in memory and exports nothing, so a developer's machine
// needs no collector.
//
// The returned logger writes JSON to stderr at LOG_LEVEL, so a log shipper that
// reads the container output sees the same records. Logs are not sent over OTLP:
// the trace_id and span_id they carry are what link them to a trace.
func Setup(lookup func(string) (string, bool)) (observability.IObservabilityService, *Backend, *slog.Logger, error) {
	if lookup == nil {
		lookup = os.LookupEnv
	}
	get := func(k string) string { v, _ := lookup(k); return strings.TrimSpace(v) }

	level, err := observability.ParseLevel(get("LOG_LEVEL"))
	if err != nil {
		return nil, nil, nil, err
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	cfg, err := FromEnv(lookup)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("telemetry configuration: %w", err)
	}
	res := observability.Resource{
		ServiceName: get("APP_NAME"), Namespace: "e-coop", Version: get("APP_VERSION"),
		Environment: get("APP_ENV"), InstanceID: hostname(),
	}
	be, err := Open(cfg, WithResource(res), WithLogger(logger))
	if err != nil {
		return nil, nil, nil, err
	}
	return observability.New(res, logger, observability.WithBackend(be)), be, logger, nil
}

func hostname() string {
	h, _ := os.Hostname()
	return h
}

// Module provides the observability service to an fx application and ties it to
// the application's life: Start on boot, and on shutdown Stop flushes every
// buffered span and metric, within the stop timeout fx allows.
//
// Services that take an observability.IObserver are given this one. It is
// provided as both the full service and the IObserver so a package asks only for
// the narrower type.
var Module = fx.Module("observability",
	fx.Provide(
		func() (observability.IObservabilityService, *Backend, *slog.Logger, error) {
			return Setup(nil)
		},
		func(s observability.IObservabilityService) observability.IObserver { return s },
	),
	fx.Invoke(func(lc fx.Lifecycle, s observability.IObservabilityService, log *slog.Logger) {
		lc.Append(fx.Hook{
			OnStart: s.Start,
			// A flush that fails (the collector is down) is logged, not returned:
			// losing telemetry must not make an otherwise clean shutdown fail, and
			// it must not turn the process's exit into an error.
			OnStop: func(ctx context.Context) error {
				if err := s.Stop(ctx); err != nil {
					log.Warn("telemetry could not be flushed on shutdown; buffered data is lost", "error", err.Error())
				}
				return nil
			},
		})
	}),
)
