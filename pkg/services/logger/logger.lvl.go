package logger

import (
	"context"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type loggerLevelImpl struct {
	ctx  context.Context
	z    *zap.Logger
	span trace.Span
}

func (l *loggerLevelImpl) Debug(msg string, kv ...any) {
	l.log(zapcore.DebugLevel, msg, kv)
}
func (l *loggerLevelImpl) Info(msg string, kv ...any) {
	l.log(zapcore.InfoLevel, msg, kv)
}
func (l *loggerLevelImpl) Warn(msg string, kv ...any) {
	l.log(zapcore.WarnLevel, msg, kv)
}

func (l *loggerLevelImpl) Error(err error, msg string, kv ...any) {
	l.span.RecordError(err)
	l.span.SetStatus(codes.Error, err.Error())
	l.log(zapcore.ErrorLevel, msg, append([]any{"error", err.Error()}, kv...))
}

func (l *loggerLevelImpl) Fatal(err error, msg string, kv ...any) {
	l.span.RecordError(err)
	l.span.SetStatus(codes.Error, err.Error())
	l.log(zapcore.FatalLevel, msg, append([]any{"error", err.Error()}, kv...))
}

func (l *loggerLevelImpl) log(lvl zapcore.Level, msg string, kv []any) {
	if ce := l.z.Check(lvl, msg); ce != nil {
		fields := toFields(kv)
		spanCtx := trace.SpanContextFromContext(l.ctx)
		if spanCtx.IsValid() {
			fields = append(fields, zap.String("trace_id", spanCtx.TraceID().String()))
			fields = append(fields, zap.String("span_id", spanCtx.SpanID().String()))
		}
		ce.Write(fields...)
	}
}
