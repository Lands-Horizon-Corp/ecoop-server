package logger

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type LogContextService struct{}

func NewLogContextService() LogContextServices {
	return &LogContextService{}
}

// Deadline implements [LogContextServices].
func (l *LogContextService) Deadline() (deadline time.Time, ok bool) {
	panic("unimplemented")
}

// Debug implements [LogContextServices].
func (l *LogContextService) Debug(ctx context.Context, msg string, kv ...any) {
	panic("unimplemented")
}

// Done implements [LogContextServices].
func (l *LogContextService) Done() <-chan struct{} {
	panic("unimplemented")
}

// Err implements [LogContextServices].
func (l *LogContextService) Err() error {
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

// Value implements [LogContextServices].
func (l *LogContextService) Value(key any) any {
	panic("unimplemented")
}

// Warn implements [LogContextServices].
func (l *LogContextService) Warn(ctx context.Context, msg string, kv ...any) {
	panic("unimplemented")
}
