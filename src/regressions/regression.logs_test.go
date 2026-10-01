package regressions

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"e-coop-server/pkg/services"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.uber.org/fx/fxtest"
)

// Regression guards for services.LogService.
//
// Guarded behaviour: secrets are redacted, malformed kv never panics or drops
// the log line, LOG_LEVEL filters, trace_id/span_id replace the raw ctx object,
// Error() marks the span failed, and the caller points at the call site.
//
// NewLogService writes to os.Stderr (captured when it is constructed), so these
// tests swap os.Stderr for a temp file. They must not use t.Parallel.

// logCapture builds a JSON LogService whose output is read back as parsed lines.
type logCapture struct {
	*services.LogService
	file *os.File
}

func newLog(t *testing.T, level string) *logCapture {
	t.Helper()
	t.Setenv("LOG_FORMAT", "json")
	t.Setenv("LOG_LEVEL", level)

	f, err := os.CreateTemp(t.TempDir(), "stderr")
	if err != nil {
		t.Fatal(err)
	}
	orig := os.Stderr
	os.Stderr = f
	t.Cleanup(func() { os.Stderr = orig; f.Close() })

	lc := fxtest.NewLifecycle(t)
	return &logCapture{LogService: services.NewLogService(lc, &services.Telemetry{ServiceName: "test"}), file: f}
}

// lines flushes the buffered writer and returns every emitted JSON line.
func (c *logCapture) lines(t *testing.T) []map[string]any {
	t.Helper()
	if err := c.Stop(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(c.file.Name())
	if err != nil {
		t.Fatal(err)
	}
	var out []map[string]any
	for ln := range strings.SplitSeq(strings.TrimSpace(string(raw)), "\n") {
		if ln == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(ln), &m); err != nil {
			t.Fatalf("log line is not valid JSON: %q: %v", ln, err)
		}
		out = append(out, m)
	}
	return out
}

func TestLogRedactsSensitiveKeys(t *testing.T) {
	l := newLog(t, "info")

	l.Info(context.Background(), "login",
		"password", "hunter2", "Token", "abc123", "AUTHORIZATION", "Bearer x",
		"email", "a@b.c", "otp", "123456", "user", "jane")

	got := l.lines(t)
	if len(got) != 1 {
		t.Fatalf("want 1 line, got %d", len(got))
	}
	for _, k := range []string{"password", "Token", "AUTHORIZATION", "email", "otp"} {
		if got[0][k] != "[REDACTED]" {
			t.Errorf("%s = %v, want [REDACTED]", k, got[0][k])
		}
	}
	if got[0]["user"] != "jane" {
		t.Errorf("non-secret key changed: user = %v", got[0]["user"])
	}
}

// Redaction must also apply to loggers created through With.
func TestLogWithKeepsFieldsAndRedaction(t *testing.T) {
	l := newLog(t, "info")

	child := l.With("component", "qr")
	child.Info(context.Background(), "hello", "secret", "s3cr3t")

	got := l.lines(t)
	if len(got) != 1 {
		t.Fatalf("want 1 line, got %d", len(got))
	}
	if got[0]["component"] != "qr" {
		t.Errorf("With field missing: %v", got[0])
	}
	if got[0]["secret"] != "[REDACTED]" {
		t.Errorf("secret leaked via child logger: %v", got[0]["secret"])
	}
}

func TestLogBadKVNeverPanicsOrDropsLine(t *testing.T) {
	l := newLog(t, "info")
	ctx := context.Background()

	cases := map[string][]any{
		"non-string key": {42, "v", "ok", "yes"},
		"odd tail":       {"a", 1, "dangling"},
		"only key":       {"lonely"},
		"nil kv":         nil,
	}
	for name, kv := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("%s: panicked: %v", name, r)
				}
			}()
			l.Info(ctx, name, kv...)
		}()
	}

	if got := l.lines(t); len(got) != len(cases) {
		t.Fatalf("want %d lines, got %d", len(cases), len(got))
	}
}

func TestLogNilContextIsSafe(t *testing.T) {
	l := newLog(t, "info")

	//nolint:staticcheck // nil ctx is deliberately supported
	l.Info(nil, "no ctx", "k", "v")
	l.Error(nil, "no ctx err", "error", errors.New("boom"))

	got := l.lines(t)
	if len(got) != 2 {
		t.Fatalf("want 2 lines, got %d", len(got))
	}
	if _, ok := got[0]["ctx"]; ok {
		t.Error("ctx key must never be printed")
	}
}

func TestLogLevelFiltering(t *testing.T) {
	l := newLog(t, "warn")
	ctx := context.Background()

	l.Debug(ctx, "d")
	l.Info(ctx, "i")
	l.Warn(ctx, "w")
	l.Error(ctx, "e")

	got := l.lines(t)
	if len(got) != 2 || got[0]["msg"] != "w" || got[1]["msg"] != "e" {
		t.Fatalf("LOG_LEVEL=warn should emit only warn+error, got %v", got)
	}
}

func TestLogBadLevelFallsBackToInfo(t *testing.T) {
	l := newLog(t, "not-a-level")
	ctx := context.Background()

	l.Debug(ctx, "d")
	l.Info(ctx, "i")

	got := l.lines(t)
	if len(got) != 1 || got[0]["msg"] != "i" {
		t.Fatalf("bad LOG_LEVEL should default to info, got %v", got)
	}
}

func TestLogTraceIDsReplaceRawContext(t *testing.T) {
	l := newLog(t, "info")
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	ctx, span := tp.Tracer("t").Start(context.Background(), "op")
	defer span.End()

	l.Info(ctx, "traced")
	l.With("k", "v").Info(ctx, "traced child")

	sc := span.SpanContext()
	got := l.lines(t)
	if len(got) != 2 {
		t.Fatalf("want 2 lines, got %d", len(got))
	}
	for _, ln := range got {
		if ln["trace_id"] != sc.TraceID().String() || ln["span_id"] != sc.SpanID().String() {
			t.Errorf("trace/span ids missing or wrong: %v", ln)
		}
		if _, ok := ln["ctx"]; ok {
			t.Errorf("raw ctx leaked into output: %v", ln)
		}
	}
}

func TestLogErrorMarksSpanFailed(t *testing.T) {
	l := newLog(t, "info")
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	ctx, span := tp.Tracer("t").Start(context.Background(), "op")

	l.Error(ctx, "failed", "error", errors.New("boom"))
	span.End()

	ended := rec.Ended()
	if len(ended) != 1 {
		t.Fatalf("want 1 span, got %d", len(ended))
	}
	if st := ended[0].Status(); st.Code != codes.Error || st.Description != "boom" {
		t.Errorf("span status = %+v, want Error/boom", st)
	}
	if len(ended[0].Events()) == 0 {
		t.Error("error was not recorded as a span event")
	}
}

// Info/Warn must not touch the span status.
func TestLogNonErrorLeavesSpanUntouched(t *testing.T) {
	l := newLog(t, "info")
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	ctx, span := tp.Tracer("t").Start(context.Background(), "op")

	l.Warn(ctx, "careful", "error", errors.New("boom"))
	span.End()

	if st := rec.Ended()[0].Status(); st.Code != codes.Unset {
		t.Errorf("Warn changed span status: %+v", st)
	}
}

// AddCallerSkip(2) must report the caller of l.Info, not the wrapper in service.logs.go.
func TestLogCallerPointsAtCallSite(t *testing.T) {
	l := newLog(t, "info")

	l.Info(context.Background(), "where")

	got := l.lines(t)
	caller, _ := got[0]["caller"].(string)
	if !strings.Contains(caller, "regression.logs_test.go") {
		t.Errorf("caller = %q, want it to point at this test file", caller)
	}
}

func TestLogStopIsFlushing(t *testing.T) {
	l := newLog(t, "info")
	l.Info(context.Background(), "buffered")

	// Output is buffered (256KB / 1s); Stop must flush it.
	if got := l.lines(t); len(got) != 1 {
		t.Fatalf("Stop did not flush buffered output, got %d lines", len(got))
	}
}
