package services

import (
	"context"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelzap"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var sensitiveKeys = map[string]struct{}{
	"password": {}, "token": {}, "authorization": {}, "secret": {},
	"email": {}, "phone": {}, "otp": {}, "pin": {},
}

type LogService struct {
	z        *zap.Logger
	buffered *zapcore.BufferedWriteSyncer
}

func NewLogService(lc fx.Lifecycle, tel *Telemetry) *LogService {
	level := zapcore.InfoLevel
	_ = level.UnmarshalText([]byte(os.Getenv("LOG_LEVEL"))) // "" or bad => info

	encCfg := zap.NewProductionEncoderConfig()
	encCfg.TimeKey = "timestamp"
	encCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	var enc zapcore.Encoder
	if os.Getenv("LOG_FORMAT") == "json" {
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

	cores := []zapcore.Core{stderrCore{zapcore.NewCore(enc, buffered, level)}}
	if tel.lp != nil {
		// Explicit provider: does NOT depend on the global being set first.
		cores = append(cores, otelzap.NewCore(tel.ServiceName, otelzap.WithLoggerProvider(tel.lp)))
	}

	l := &LogService{
		z:        zap.New(zapcore.NewTee(cores...), zap.AddCaller(), zap.AddCallerSkip(2)),
		buffered: buffered,
	}
	lc.Append(fx.Hook{OnStop: func(context.Context) error { return l.Stop() }})
	return l
}

func (l *LogService) Debug(ctx context.Context, msg string, kv ...any) {
	l.log(ctx, zapcore.DebugLevel, msg, kv)
}
func (l *LogService) Info(ctx context.Context, msg string, kv ...any) {
	l.log(ctx, zapcore.InfoLevel, msg, kv)
}
func (l *LogService) Warn(ctx context.Context, msg string, kv ...any) {
	l.log(ctx, zapcore.WarnLevel, msg, kv)
}

// Error logs and marks the active span as failed when kv contains "error", err.
func (l *LogService) Error(ctx context.Context, msg string, kv ...any) {
	l.log(ctx, zapcore.ErrorLevel, msg, kv)
	if ctx == nil {
		return
	}
	span := trace.SpanFromContext(ctx)
	for i := 0; i+1 < len(kv); i += 2 {
		if k, _ := kv[i].(string); k == "error" {
			if err, ok := kv[i+1].(error); ok && err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}
		}
	}
}

// With returns a child logger sharing the same cores.
func (l *LogService) With(kv ...any) *LogService {
	return &LogService{z: l.z.With(toFields(nil, kv)...), buffered: l.buffered}
}

// Stop flushes stderr. (OTel batches are flushed by Telemetry.Shutdown.)
func (l *LogService) Stop() error {
	_ = l.z.Sync() // may return "invalid argument" on stderr; safe to ignore
	return l.buffered.Stop()
}

func (l *LogService) log(ctx context.Context, lvl zapcore.Level, msg string, kv []any) {
	ce := l.z.Check(lvl, msg) // cheap when the level is disabled
	if ce == nil {
		return
	}
	ce.Write(toFields(ctx, kv)...)
}

// toFields turns kv pairs into zap fields, guarding bad input and redacting secrets.
func toFields(ctx context.Context, kv []any) []zap.Field {
	fs := make([]zap.Field, 0, len(kv)/2+1)
	if ctx != nil {
		fs = append(fs, zap.Any("ctx", ctx)) // otelzap reads the span; stderrCore turns it into trace_id/span_id
	}
	for i := 0; i < len(kv); i += 2 {
		key, ok := kv[i].(string)
		if !ok {
			fs = append(fs, zap.Any("!BADKEY", kv[i]))
			i-- // re-align: treat this element as a lone value
			continue
		}
		if i+1 >= len(kv) {
			fs = append(fs, zap.String("!BADKEY", key)) // odd tail
			break
		}
		val := kv[i+1]
		if _, secret := sensitiveKeys[strings.ToLower(key)]; secret {
			val = "[REDACTED]"
		}
		fs = append(fs, zap.Any(key, val))
	}
	return fs
}

// stderrCore wraps the console/JSON core: it must not print the raw ctx object,
// so it swaps it for trace_id / span_id (greppable in the terminal).
type stderrCore struct{ zapcore.Core }

func (c stderrCore) With(f []zapcore.Field) zapcore.Core { return stderrCore{c.Core.With(cleanCtx(f))} }

func (c stderrCore) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(e.Level) {
		return ce.AddCore(e, c)
	}
	return ce
}

func (c stderrCore) Write(e zapcore.Entry, f []zapcore.Field) error {
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
