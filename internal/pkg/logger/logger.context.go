package logger

import (
	"context"
	"fmt"
	"os"
	"runtime/debug"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// fatalFlushTimeout bounds how long Fatal blocks while pushing telemetry out.
const fatalFlushTimeout = 5 * time.Second

type logContextImpl struct {
	context.Context
	svc *loggerServiceImpl
}

func (c *logContextImpl) Trace(name string, attrs ...attribute.KeyValue) (LogContextService, LoggerLevel) {
	ctx, span := c.svc.state.tracer.Start(c.Context, name, trace.WithAttributes(attrs...))
	return &logContextImpl{Context: ctx, svc: c.svc}, &loggerLevelImpl{ctx: ctx, span: span, svc: c.svc}
}

// Job implements [LoggerService].
func (l *loggerServiceImpl) Job(ctx context.Context, name string, fn JobFunc, attrs ...attribute.KeyValue) (err error) {
	if err := l.requireStarted(); err != nil {
		return err
	}
	if fn == nil {
		return ErrNilTarget
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// A job is its own trace; if it was kicked off from a request, link back to it.
	opts := []trace.SpanStartOption{
		trace.WithNewRoot(),
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(append(attrs, attribute.String("job.name", name))...),
	}
	if parent := trace.SpanContextFromContext(ctx); parent.IsValid() {
		opts = append(opts, trace.WithLinks(trace.Link{SpanContext: parent}))
	}

	start := time.Now()
	ctx, span := l.state.tracer.Start(ctx, name, opts...)
	defer span.End()
	log := &loggerLevelImpl{ctx: ctx, span: span, svc: l}

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("job %s panicked: %v\n%s", name, r, debug.Stack())
		}
		outcome := "success"
		if err != nil {
			outcome = "error"
			log.Error(err, "job failed", "job", name)
		} else {
			log.Success("job finished", "job", name)
		}
		set := metric.WithAttributes(attribute.String("job.name", name), attribute.String("outcome", outcome))
		l.state.jobDuration.Record(ctx, time.Since(start).Seconds(), set)
		l.state.jobRuns.Add(ctx, 1, set)
	}()

	return fn(&logContextImpl{Context: ctx, svc: l}, log)
}

// loggerLevelImpl handles leveled logging with span events, error recording and trace correlation.
type loggerLevelImpl struct {
	ctx  context.Context
	span trace.Span
	svc  *loggerServiceImpl
}

func (l *loggerLevelImpl) Span() trace.Span {
	return l.span
}

func (l *loggerLevelImpl) Debug(msg string, kv ...any) {
	l.log(zapcore.DebugLevel, "debug", msg, nil, kv)
}

func (l *loggerLevelImpl) Info(msg string, kv ...any) {
	l.log(zapcore.InfoLevel, "info", msg, nil, kv)
}

func (l *loggerLevelImpl) Warn(msg string, kv ...any) {
	l.log(zapcore.WarnLevel, "warn", msg, nil, kv)
}

func (l *loggerLevelImpl) Success(msg string, kv ...any) {
	l.span.SetStatus(codes.Ok, "")
	l.log(zapcore.InfoLevel, "success", msg, nil, append(kv, "outcome", "success"))
}

func (l *loggerLevelImpl) Error(err error, msg string, kv ...any) {
	l.fail(err, msg)
	l.log(zapcore.ErrorLevel, "error", msg, err, kv)
}

func (l *loggerLevelImpl) Fatal(err error, msg string, kv ...any) {
	l.fail(err, msg)
	l.log(zapcore.FatalLevel, "fatal", msg, err, kv)

	// Push everything out now: a fatal is usually followed by a crash or exit.
	ctx, cancel := context.WithTimeout(context.Background(), fatalFlushTimeout)
	defer cancel()
	if err := l.svc.flush(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "logger: flush after fatal:", err)
	}
}

func (l *loggerLevelImpl) Event(name string, kv ...any) {
	l.span.AddEvent(name, trace.WithAttributes(kvToAttrs(kv)...))
	l.svc.state.z.Debug(name, l.buildFields(kv...)...)
}

func (l *loggerLevelImpl) fail(err error, msg string) {
	if err == nil {
		l.span.SetStatus(codes.Error, msg)
		return
	}
	l.span.RecordError(err, trace.WithStackTrace(true))
	l.span.SetStatus(codes.Error, err.Error())
}

// log writes one line to zap (stderr + Loki) and mirrors it as a span event.
func (l *loggerLevelImpl) log(level zapcore.Level, severity, msg string, err error, kv []any) {
	attrs := append(kvToAttrs(kv), attribute.String("log.severity", severity))
	if err != nil {
		attrs = append(attrs, attribute.String("error.message", err.Error()))
	}
	l.span.AddEvent(msg, trace.WithAttributes(attrs...))

	fields := l.buildFields(kv...)
	if err != nil {
		fields = append(fields, zap.Error(err))
	}
	if severity == "success" {
		fields = append(fields, zap.String("severity", severity))
	}
	if ce := l.svc.state.zl.Check(level, msg); ce != nil {
		ce.Write(fields...)
	}
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
		key := fmt.Sprintf("%v", kv[i])
		if i+1 < len(kv) {
			fields = append(fields, zap.Any(key, kv[i+1]))
		} else {
			fields = append(fields, zap.Any(key, "MISSING_VALUE"))
		}
	}
	return fields
}

// kvToAttrs converts alternating key/value pairs into span attributes.
func kvToAttrs(kv []any) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, len(kv)/2+1)
	for i := 0; i < len(kv); i += 2 {
		key := fmt.Sprintf("%v", kv[i])
		if i+1 >= len(kv) {
			attrs = append(attrs, attribute.String(key, "MISSING_VALUE"))
			break
		}
		switch v := kv[i+1].(type) {
		case string:
			attrs = append(attrs, attribute.String(key, v))
		case bool:
			attrs = append(attrs, attribute.Bool(key, v))
		case int:
			attrs = append(attrs, attribute.Int(key, v))
		case int32:
			attrs = append(attrs, attribute.Int64(key, int64(v)))
		case int64:
			attrs = append(attrs, attribute.Int64(key, v))
		case float32:
			attrs = append(attrs, attribute.Float64(key, float64(v)))
		case float64:
			attrs = append(attrs, attribute.Float64(key, v))
		case time.Duration:
			attrs = append(attrs, attribute.String(key, v.String()))
		case error:
			attrs = append(attrs, attribute.String(key, v.Error()))
		case fmt.Stringer:
			attrs = append(attrs, attribute.String(key, v.String()))
		default:
			attrs = append(attrs, attribute.String(key, fmt.Sprintf("%v", v)))
		}
	}
	return attrs
}
