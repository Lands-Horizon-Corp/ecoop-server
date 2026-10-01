package services

import (
	"context"
	"errors"
	"fmt"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/fx"
)

const defaultServiceName = "e-coop-server"

type Telemetry struct {
	ServiceName string
	lp          *sdklog.LoggerProvider   // nil when OTLP is disabled
	tp          *sdktrace.TracerProvider // nil when OTLP is disabled
}

func NewTelemetry(lc fx.Lifecycle) (*Telemetry, error) {
	t := &Telemetry{ServiceName: defaultServiceName}
	if n := os.Getenv("OTEL_SERVICE_NAME"); n != "" {
		t.ServiceName = n
	}

	// No endpoint => stderr-only logging, no exporters (local without docker).
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return t, nil
	}

	ctx := context.Background()

	// Later detectors override earlier ones: default name first, env wins.
	res, err := resource.New(ctx,
		resource.WithAttributes(attribute.String("service.name", defaultServiceName)),
		resource.WithFromEnv(), // OTEL_SERVICE_NAME + OTEL_RESOURCE_ATTRIBUTES
		resource.WithTelemetrySDK(),
		resource.WithHost(),
	)
	if err != nil {
		return nil, fmt.Errorf("otel resource: %w", err)
	}

	// Print exporter failures instead of losing them silently.
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		fmt.Fprintln(os.Stderr, "otel:", err)
	}))

	// --- traces ---
	traceExp, err := otlptracegrpc.New(ctx) // honors OTEL_EXPORTER_OTLP_*
	if err != nil {
		return nil, fmt.Errorf("otlp trace exporter: %w", err)
	}
	// Sampler comes from OTEL_TRACES_SAMPLER / _ARG automatically.
	t.tp = sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(traceExp), // async: own goroutine + bounded queue
	)
	otel.SetTracerProvider(t.tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	// --- logs ---
	logExp, err := otlploggrpc.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("otlp log exporter: %w", err)
	}
	t.lp = sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExp)), // async
	)
	global.SetLoggerProvider(t.lp)

	// fx runs OnStop hooks in reverse order: LogService stops first,
	// then this flushes the last batches of logs and spans.
	lc.Append(fx.Hook{OnStop: t.Shutdown})
	return t, nil
}

func (t *Telemetry) Shutdown(ctx context.Context) error {
	var errs []error
	if t.tp != nil {
		errs = append(errs, t.tp.Shutdown(ctx))
	}
	if t.lp != nil {
		errs = append(errs, t.lp.Shutdown(ctx))
	}
	return errors.Join(errs...)
}
