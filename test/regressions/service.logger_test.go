package regressions

import (
	"context"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/logger"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestLogger_ServicesShareOneStartedPipelineButTagTheirOwnSpans(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1") // exporters connect lazily; nothing listens
	root := logger.NewLogContextService("ecoop", "json", "error", attribute.String("service", "root"))
	if err := root.Start(bg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(bg, 500*time.Millisecond)
		defer cancel()
		_ = root.Stop(ctx)
	})

	sqlLog := root.With(attribute.String("service", "sql"))
	cqrsLog := root.With(attribute.String("service", "cqrs"))

	tagged := func(l logger.LogContextService, want string) {
		t.Helper()
		child, lvl := l.Trace("op", attribute.String("detail", "x"))
		defer lvl.Span().End()
		ro, ok := trace.SpanFromContext(child).(sdktrace.ReadOnlySpan)
		if !ok {
			t.Fatalf("%s: the span is not an SDK span: the view is not using the started tracer", want)
		}
		got := map[string]string{}
		for _, kv := range ro.Attributes() {
			got[string(kv.Key)] = kv.Value.AsString()
		}
		if got["service"] != want || got["detail"] != "x" {
			t.Errorf("%s span attributes = %v; want its own service tag plus the call's attributes", want, got)
		}
	}
	tagged(sqlLog, "sql")
	tagged(cqrsLog, "cqrs")
	child, lvl := sqlLog.Trace("parent")
	defer lvl.Span().End()
	tagged(child, "sql")

	if err := cqrsLog.Observe("work", func() error { return nil }); err != nil {
		t.Fatal(err)
	}
}
