package logger

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelzap"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func NewLogContextService(name string, logFormat string, logLevel string) LogContextService {
	if logFormat == "" {
		logFormat = "json"
	}
	if logLevel == "" {
		logLevel = "info"
	}
	return &logContextService{
		Context: context.Background(),
		state: &sharedState{
			name: name,
		},
		logFormat: logFormat,
		logLevel:  logLevel,
	}
}

func (l *logContextService) Start(ctx context.Context) error {
	res, err := resource.New(ctx,
		resource.WithAttributes(attribute.String("service.name", l.state.name)),
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
	)
	if err != nil {
		return err
	}

	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		fmt.Fprintln(os.Stderr, "otel:", err)
	}))

	traceExp, err := otlptracegrpc.New(ctx)
	if err != nil {
		return fmt.Errorf("otlp trace exporter: %w", err)
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
		return fmt.Errorf("otlp log exporter: %w", err)
	}
	l.state.lp = sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExp)),
	)
	global.SetLoggerProvider(l.state.lp)

	l.state.tracer = otel.Tracer(l.state.name)

	level := zapcore.InfoLevel
	_ = level.UnmarshalText([]byte(l.logLevel))

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

	buffered := &zapcore.BufferedWriteSyncer{
		WS:            zapcore.AddSync(os.Stderr),
		FlushInterval: time.Second,
		Size:          256 * 1024,
	}

	cores := []zapcore.Core{zapcore.NewCore(enc, buffered, level)}
	if l.state.lp != nil {
		cores = append(cores, otelzap.NewCore(l.state.name, otelzap.WithLoggerProvider(l.state.lp)))
	}
	l.state.z = zap.New(zapcore.NewTee(cores...), zap.AddCaller(), zap.AddCallerSkip(2))
	l.state.buffered = buffered
	l.Context = ctx
	return nil
}

func (l *logContextService) Stop(ctx context.Context) error {
	var errs []error
	if l.state.tp != nil {
		errs = append(errs, l.state.tp.Shutdown(ctx))
	}
	if l.state.lp != nil {
		errs = append(errs, l.state.lp.Shutdown(ctx))
	}
	if l.state.z != nil {
		_ = l.state.z.Sync()
	}
	if l.state.buffered != nil {
		_ = l.state.buffered.Stop()
	}
	return errors.Join(errs...)
}

func (l *logContextService) Trace(name string, attrs ...attribute.KeyValue) (LogContextService, LoggerLevel, trace.Span) {
	ctx, span := l.state.tracer.Start(l.Context, name, trace.WithAttributes(attrs...))
	childCtx := &logContextService{
		Context: ctx,
		state:   l.state,
	}
	logImpl := &loggerLevelImpl{
		ctx:  ctx,
		z:    l.state.z,
		span: span,
	}
	return childCtx, logImpl, span
}
