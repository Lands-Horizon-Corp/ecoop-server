package logger

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type LoggerLevel interface {
	Span() trace.Span
	Debug(msg string, kv ...any)
	Info(msg string, kv ...any)
	Warn(msg string, kv ...any)
	Error(err error, msg string, kv ...any)
	Fatal(err error, msg string, kv ...any)
}

type LogContextService interface {
	context.Context
	Trace(name string, attrs ...attribute.KeyValue) (LogContextService, LoggerLevel)
	// With returns a view that shares this logger's pipeline and tags its spans with attr.
	With(attr attribute.KeyValue) LogContextService
	Observe(name string, fn func() error, attrs ...attribute.KeyValue) error
	Emit(name string, write func(LoggerLevel))
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

type sharedState struct {
	lp       *sdklog.LoggerProvider
	tp       *sdktrace.TracerProvider
	z        *zap.Logger
	buffered *zapcore.BufferedWriteSyncer
	tracer   trace.Tracer
	name     string
}

type logContextService struct {
	context.Context
	state     *sharedState
	logFormat string
	logLevel  string
	attr      attribute.KeyValue
}
