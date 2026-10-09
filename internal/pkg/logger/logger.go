package logger

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelzap"
	"go.opentelemetry.io/contrib/instrumentation/host"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type state struct {
	name     string
	tp       *sdktrace.TracerProvider
	lp       *sdklog.LoggerProvider
	mp       *sdkmetric.MeterProvider
	tracer   trace.Tracer
	meter    metric.Meter
	z        *zap.Logger
	zl       *zap.Logger // z with one extra caller skip, for the shared LoggerLevel.log path
	buffered *zapcore.BufferedWriteSyncer

	jobDuration metric.Float64Histogram
	jobRuns     metric.Int64Counter
}

type loggerServiceImpl struct {
	mu        sync.Mutex
	started   bool
	state     state
	logLevel  string
	logFormat string
	Context   context.Context
}

func NewLoggerService(serviceName, logLevel, logFormat string) LoggerService {
	// Until Start runs, logs go to stderr only and spans/metrics are no-ops.
	enc := zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig())
	l := &loggerServiceImpl{
		state: state{
			name:   serviceName,
			tracer: tracenoop.NewTracerProvider().Tracer(serviceName),
			meter:  metricnoop.NewMeterProvider().Meter(serviceName),
		},
		logLevel:  logLevel,
		logFormat: logFormat,
	}
	l.setZap(zap.New(zapcore.NewCore(enc, zapcore.AddSync(os.Stderr), zapcore.DebugLevel),
		zap.AddCaller(), zap.AddCallerSkip(1), zap.WithFatalHook(noExitHook{})))
	return l
}

// noExitHook keeps zap from calling os.Exit on fatal entries. zap rewrites
// zapcore.WriteThenNoop to WriteThenFatal, so a custom hook is required.
type noExitHook struct{}

func (noExitHook) OnWrite(*zapcore.CheckedEntry, []zapcore.Field) {}

func (l *loggerServiceImpl) setZap(z *zap.Logger) {
	l.state.z = z
	l.state.zl = z.WithOptions(zap.AddCallerSkip(1))
}

func (l *loggerServiceImpl) Start(ctx context.Context) (err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.started {
		return ErrAlreadyStarted
	}
	if l.state.name == "" {
		return errors.New("logger: service name is required")
	}

	level := zapcore.InfoLevel
	if l.logLevel != "" {
		if err := level.UnmarshalText([]byte(l.logLevel)); err != nil {
			return fmt.Errorf("logger: invalid log level %q: %w", l.logLevel, err)
		}
	}
	if l.logFormat != "" && l.logFormat != "json" && l.logFormat != "console" {
		return fmt.Errorf("logger: invalid log format %q (want json or console)", l.logFormat)
	}

	// Undo whatever was created if a later step fails.
	defer func() {
		if err != nil {
			err = errors.Join(err, l.shutdown(ctx))
		}
	}()

	res, err := resource.New(ctx,
		resource.WithAttributes(attribute.String("service.name", l.state.name)),
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
	)
	if err != nil {
		return fmt.Errorf("logger: resource: %w", err)
	}

	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		fmt.Fprintln(os.Stderr, "otel:", err)
	}))

	traceExp, err := otlptracegrpc.New(ctx)
	if err != nil {
		return fmt.Errorf("logger: otlp trace exporter: %w", err)
	}
	l.state.tp = sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(traceExp),
	)
	otel.SetTracerProvider(l.state.tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	logExp, err := otlploggrpc.New(ctx)
	if err != nil {
		return fmt.Errorf("logger: otlp log exporter: %w", err)
	}
	l.state.lp = sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExp)),
	)
	global.SetLoggerProvider(l.state.lp)

	metricExp, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		return fmt.Errorf("logger: otlp metric exporter: %w", err)
	}
	l.state.mp = sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp, sdkmetric.WithInterval(10*time.Second))),
	)
	otel.SetMeterProvider(l.state.mp)

	// Go runtime (memory, GC, goroutines) and process/host (CPU, memory) metrics.
	if err := runtime.Start(runtime.WithMeterProvider(l.state.mp)); err != nil {
		return fmt.Errorf("logger: runtime metrics: %w", err)
	}
	if err := host.Start(host.WithMeterProvider(l.state.mp)); err != nil {
		return fmt.Errorf("logger: host metrics: %w", err)
	}

	l.state.tracer = otel.Tracer(l.state.name)
	l.state.meter = otel.Meter(l.state.name)

	if l.state.jobDuration, err = l.state.meter.Float64Histogram("job.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of background jobs."),
	); err != nil {
		return fmt.Errorf("logger: job.duration: %w", err)
	}
	if l.state.jobRuns, err = l.state.meter.Int64Counter("job.runs",
		metric.WithDescription("Background job runs by outcome."),
	); err != nil {
		return fmt.Errorf("logger: job.runs: %w", err)
	}

	encCfg := zap.NewProductionEncoderConfig()
	encCfg.TimeKey = "timestamp"
	encCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	var enc zapcore.Encoder
	if l.logFormat == "json" {
		enc = zapcore.NewJSONEncoder(encCfg)
	} else {
		encCfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
		enc = zapcore.NewConsoleEncoder(encCfg)
	}

	l.state.buffered = &zapcore.BufferedWriteSyncer{
		WS:            zapcore.AddSync(os.Stderr),
		FlushInterval: time.Second,
		Size:          256 * 1024,
	}

	l.setZap(zap.New(
		zapcore.NewTee(
			zapcore.NewCore(enc, l.state.buffered, level),
			otelzap.NewCore(l.state.name, otelzap.WithLoggerProvider(l.state.lp)),
		),
		zap.AddCaller(),
		zap.AddCallerSkip(1),
		zap.WithFatalHook(noExitHook{}),
	))
	l.Context = ctx
	l.started = true
	return nil
}

func (l *loggerServiceImpl) Stop(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.started {
		return ErrNotStarted
	}
	l.started = false
	return l.shutdown(ctx)
}

// shutdown flushes and closes every provider that exists, returning all failures.
func (l *loggerServiceImpl) shutdown(ctx context.Context) error {
	var errs []error
	if l.state.buffered != nil {
		if err := l.state.buffered.Stop(); err != nil {
			errs = append(errs, fmt.Errorf("logger: flush stderr: %w", err))
		}
	}
	if l.state.tp != nil {
		if err := l.state.tp.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("logger: trace provider: %w", err))
		}
	}
	if l.state.lp != nil {
		if err := l.state.lp.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("logger: log provider: %w", err))
		}
	}
	if l.state.mp != nil {
		if err := l.state.mp.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("logger: meter provider: %w", err))
		}
	}
	return errors.Join(errs...)
}

// flush pushes buffered logs, spans and metrics out now without shutting down.
func (l *loggerServiceImpl) flush(ctx context.Context) error {
	var errs []error
	if l.state.buffered != nil {
		if err := l.state.buffered.Sync(); err != nil {
			errs = append(errs, fmt.Errorf("logger: flush stderr: %w", err))
		}
	}
	if l.state.tp != nil {
		if err := l.state.tp.ForceFlush(ctx); err != nil {
			errs = append(errs, fmt.Errorf("logger: flush traces: %w", err))
		}
	}
	if l.state.lp != nil {
		if err := l.state.lp.ForceFlush(ctx); err != nil {
			errs = append(errs, fmt.Errorf("logger: flush logs: %w", err))
		}
	}
	if l.state.mp != nil {
		if err := l.state.mp.ForceFlush(ctx); err != nil {
			errs = append(errs, fmt.Errorf("logger: flush metrics: %w", err))
		}
	}
	return errors.Join(errs...)
}

func (l *loggerServiceImpl) requireStarted() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.started {
		return ErrNotStarted
	}
	return nil
}

// Meter returns the service meter for custom metrics (counters, histograms, gauges).
func (l *loggerServiceImpl) Meter() metric.Meter {
	return l.state.meter
}

func (l *loggerServiceImpl) NewContext(parent context.Context) LogContextService {
	if parent == nil {
		parent = context.Background()
	}
	return &logContextImpl{Context: parent, svc: l}
}
