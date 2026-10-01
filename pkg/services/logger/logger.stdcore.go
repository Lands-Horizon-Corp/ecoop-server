package logger

import (
	"context"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type StderrCore struct {
	zapcore.Core
}

func (c StderrCore) With(f []zapcore.Field) zapcore.Core {
	return StderrCore{c.Core.With(cleanCtx(f))}
}

func (c StderrCore) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(e.Level) {
		return ce.AddCore(e, c)
	}
	return ce
}

func (c StderrCore) Write(e zapcore.Entry, f []zapcore.Field) error {
	return c.Core.Write(e, cleanCtx(f))
}

func cleanCtx(fields []zapcore.Field) []zapcore.Field {
	out := make([]zapcore.Field, 0, len(fields)+2)
	for _, f := range fields {
		if ctx, ok := f.Interface.(context.Context); ok {
			if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
				out = append(out, zap.String("trace_id", sc.TraceID().String()), zap.String("span_id", sc.SpanID().String()))
			}
			continue
		}
		out = append(out, f)
	}
	return out
}
