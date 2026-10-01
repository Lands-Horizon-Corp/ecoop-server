package logger

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type LoggerLevel struct {
	Debug func(ctx context.Context, msg string, kv ...any)
	Info  func(ctx context.Context, msg string, kv ...any)
	Warn  func(ctx context.Context, msg string, kv ...any)
	Error func(ctx context.Context, err error, msg string, kv ...any)
	Fatal func(ctx context.Context, err error, msg string, kv ...any)
}
type LogContextServices interface {
	context.Context
	Trace(name string, attrs ...attribute.KeyValue) (context.Context, LoggerLevel, trace.Span)
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}
