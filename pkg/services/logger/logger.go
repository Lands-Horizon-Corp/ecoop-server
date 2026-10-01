package logger

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type LogContextService struct {
	context.Context
}

func NewLogContextService() LogContextServices {
	return &LogContextService{Context: context.Background()}
}

// Debug implements [LogContextServices].
func (l *LogContextService) Debug(ctx context.Context, msg string, kv ...any) {
	panic("unimplemented")
}

// Error implements [LogContextServices].
func (l *LogContextService) Error(ctx context.Context, msg string, kv ...any) {
	panic("unimplemented")
}

// Fatal implements [LogContextServices].
func (l *LogContextService) Fatal(ctx context.Context, msg string, kv ...any) {
	panic("unimplemented")
}

// Info implements [LogContextServices].
func (l *LogContextService) Info(ctx context.Context, msg string, kv ...any) {
	panic("unimplemented")
}

// Trace implements [LogContextServices].
func (l *LogContextService) Trace(name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	panic("unimplemented")
}

// Warn implements [LogContextServices].
func (l *LogContextService) Warn(ctx context.Context, msg string, kv ...any) {
	panic("unimplemented")
}
