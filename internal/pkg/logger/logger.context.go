package logger

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type logContextImpl struct {
	context.Context
	tracer trace.Tracer
	z      *zap.Logger
}

func (c *logContextImpl) Trace(name string, attrs ...attribute.KeyValue) (LogContextService, LoggerLevel) {
	ctx, span := c.tracer.Start(c.Context, name, trace.WithAttributes(attrs...))

	newLogCtx := &logContextImpl{
		Context: ctx,
		tracer:  c.tracer,
		z:       c.z,
	}

	levelLogger := &loggerLevelImpl{
		ctx:  ctx,
		span: span,
		z:    c.z,
	}

	return newLogCtx, levelLogger
}

// loggerLevelImpl handles leveled logging with span error recording and trace correlation.
type loggerLevelImpl struct {
	ctx  context.Context
	span trace.Span
	z    *zap.Logger
}

func (l *loggerLevelImpl) Span() trace.Span {
	return l.span
}

func (l *loggerLevelImpl) Debug(msg string, kv ...any) {
	l.z.Debug(msg, l.buildFields(kv...)...)
}

func (l *loggerLevelImpl) Info(msg string, kv ...any) {
	l.z.Info(msg, l.buildFields(kv...)...)
}

func (l *loggerLevelImpl) Warn(msg string, kv ...any) {
	l.z.Warn(msg, l.buildFields(kv...)...)
}

func (l *loggerLevelImpl) Error(err error, msg string, kv ...any) {
	if err != nil {
		l.span.RecordError(err)
		l.span.SetStatus(codes.Error, err.Error())
	}
	fields := append(l.buildFields(kv...), zap.Error(err))
	l.z.Error(msg, fields...)
}

func (l *loggerLevelImpl) Fatal(err error, msg string, kv ...any) {
	if err != nil {
		l.span.RecordError(err)
		l.span.SetStatus(codes.Error, err.Error())
	}
	fields := append(l.buildFields(kv...), zap.Error(err))
	l.z.Fatal(msg, fields...)
}

func (l *loggerLevelImpl) buildFields(kv ...any) []zap.Field {
	fields := make([]zap.Field, 0, len(kv)/2+2)
	spanCtx := l.span.SpanContext()
	if spanCtx.IsValid() {
		fields = append(fields,
			zap.String("trace_id", spanCtx.TraceID().String()),
			zap.String("span_id", spanCtx.SpanID().String()),
		)
	}
	for i := 0; i < len(kv); i += 2 {
		if i+1 < len(kv) {
			key := fmt.Sprintf("%v", kv[i])
			fields = append(fields, zap.Any(key, kv[i+1]))
		} else {
			key := fmt.Sprintf("%v", kv[i])
			fields = append(fields, zap.Any(key, "MISSING_VALUE"))
		}
	}
	return fields
}
