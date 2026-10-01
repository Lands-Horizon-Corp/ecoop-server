package logger

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type LogContextServices interface {
	context.Context

	Start(ctx context.Context, otelServiceName string, otelOtlpEndpoint string) error
	Stop(ctx context.Context) error

	Trace(name string, attrs ...attribute.KeyValue) (context.Context, trace.Span)
	Debug(ctx context.Context, msg string, kv ...any)
	Info(ctx context.Context, msg string, kv ...any)
	Warn(ctx context.Context, msg string, kv ...any)
	Error(ctx context.Context, msg string, kv ...any)
	Fatal(ctx context.Context, msg string, kv ...any)
}

/*

func Encode(ctx LogContext) {
	ctx, span := ctx.Trace("sample.log")
}
*/
