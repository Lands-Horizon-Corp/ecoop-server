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

type LogContextService struct {
	context.Context

	lp *sdklog.LoggerProvider
	tp *sdktrace.TracerProvider

	z        *zap.Logger
	buffered *zapcore.BufferedWriteSyncer

	tracer trace.Tracer
	name   string

	logFormat string
	logLevel  string
}

func NewLogContextService(name string, logFormat string, logLevel string) LogContextServices {
	if logFormat == "" {
		logFormat = "json"
	}
	if logLevel == "" {
		logLevel = "info"
	}
	return &LogContextService{name: name, logFormat: logFormat, logLevel: logLevel}
}

func (l *LogContextService) Start(
	ctx context.Context, otelServiceName string, otelOtlpEndpoint string) error {

	res, err := resource.New(ctx,
		resource.WithAttributes(attribute.String("service.name", l.name)),
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
	l.tp = sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(traceExp),
	)
	otel.SetTracerProvider(l.tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	logExp, err := otlploggrpc.New(ctx)
	if err != nil {
		return fmt.Errorf("otlp log exporter: %w", err)
	}
	l.lp = sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExp)),
	)
	global.SetLoggerProvider(l.lp)

	l.tracer = otel.Tracer(l.name)

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
	cores := []zapcore.Core{StderrCore{zapcore.NewCore(enc, buffered, level)}}
	if l.lp != nil {
		cores = append(cores, otelzap.NewCore(l.name, otelzap.WithLoggerProvider(l.lp)))
	}
	l.z = zap.New(zapcore.NewTee(cores...), zap.AddCaller(), zap.AddCallerSkip(2))
	l.buffered = buffered
	return nil
}

func (l *LogContextService) Stop(ctx context.Context) error {
	var errs []error
	if l.tp != nil {
		errs = append(errs, l.tp.Shutdown(ctx))
	}
	if l.lp != nil {
		errs = append(errs, l.lp.Shutdown(ctx))
	}
	if err := l.z.Sync(); err != nil {
		return nil
	}
	if err := l.buffered.Stop(); err != nil {
		return nil
	}
	return errors.Join(errs...)
}

func (l *LogContextService) Trace(
	name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return l.tracer.Start(l.Context, name, trace.WithAttributes(attrs...))
}
func (l *LogContextService) Debug(ctx context.Context, msg string, kv ...any) {
	l.log(ctx, zapcore.DebugLevel, msg, kv)
}

func (l *LogContextService) Error(ctx context.Context, err error, msg string, kv ...any) {
	l.log(ctx, zapcore.ErrorLevel, msg, kv)
}

func (l *LogContextService) Fatal(ctx context.Context, err error, msg string, kv ...any) {
	l.log(ctx, zapcore.FatalLevel, msg, kv)
}

func (l *LogContextService) Info(ctx context.Context, msg string, kv ...any) {
	l.log(ctx, zapcore.InfoLevel, msg, kv)
}

func (l *LogContextService) Warn(ctx context.Context, msg string, kv ...any) {
	l.log(ctx, zapcore.WarnLevel, msg, kv)
}

func (l *LogContextService) log(ctx context.Context, lvl zapcore.Level, msg string, kv []any) {
	ce := l.z.Check(lvl, msg)
	if ce == nil {
		return
	}
	ce.Write(toFields(ctx, kv)...)
}
