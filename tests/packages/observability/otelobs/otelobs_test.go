package otelobs_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	obs "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability/otelobs"
)

// No build tag and no network: spans go to an in-memory exporter and metrics to
// a manual reader, so these tests read exactly what would be exported.

// ctxKey is a context key of this file's own, so it cannot collide with another.
type ctxKey struct{}

type rig struct {
	svc     obs.IObservabilityService
	backend *otelobs.Backend
	spans   *tracetest.InMemoryExporter
	reader  *sdkmetric.ManualReader
}

func newRig(t testing.TB, cfg otelobs.Config) *rig {
	t.Helper()
	spans := tracetest.NewInMemoryExporter()
	reader := sdkmetric.NewManualReader()
	b, err := otelobs.Open(cfg,
		otelobs.WithResource(obs.Resource{ServiceName: "ecoop-test", Namespace: "e-coop", Version: "1.2.3", Environment: "test"}),
		otelobs.WithSpanExporter(spans), otelobs.WithMetricReader(reader))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	svc := obs.New(obs.Resource{ServiceName: "ecoop-test"}, slog.New(slog.DiscardHandler), obs.WithBackend(b))
	t.Cleanup(func() { _ = svc.Stop(context.Background()) })
	return &rig{svc: svc, backend: b, spans: spans, reader: reader}
}

func (r *rig) flush(t testing.TB) {
	t.Helper()
	if err := r.backend.ForceFlush(context.Background()); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}
}

func (r *rig) collect(t testing.TB) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	return rm
}

func findMetric(rm metricdata.ResourceMetrics, name string) (metricdata.Metrics, bool) {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m, true
			}
		}
	}
	return metricdata.Metrics{}, false
}

func attrsOf(kv attribute.Set) map[string]string {
	out := map[string]string{}
	for _, a := range kv.ToSlice() {
		out[string(a.Key)] = a.Value.String()
	}
	return out
}

// An operation is one span, one count and one timed observation, all consistent.
func TestSpansAndMetricsForAnOperation(t *testing.T) {
	t.Parallel()
	r := newRig(t, otelobs.Config{Sampler: "always_on"})
	_, op := r.svc.Begin(context.Background(), obs.ComponentSQL, "ping", slog.String("db.system", "postgresql"))
	op.End(nil)
	r.flush(t)

	got := r.spans.GetSpans()
	if len(got) != 1 {
		t.Fatalf("%d spans, want 1", len(got))
	}
	sp := got[0]
	if sp.Name != "sql ping" {
		t.Errorf("span name = %q", sp.Name)
	}
	attrs := map[string]string{}
	for _, a := range sp.Attributes {
		attrs[string(a.Key)] = a.Value.String()
	}
	for k, want := range map[string]string{"ecoop.component": "sql", "ecoop.operation": "ping", "ecoop.outcome": "success", "db.system": "postgresql"} {
		if attrs[k] != want {
			t.Errorf("span attribute %s = %q, want %q", k, attrs[k], want)
		}
	}
	if sp.Status.Code == codes.Error {
		t.Error("a successful operation has an error status")
	}

	rm := r.collect(t)
	ops, ok := findMetric(rm, obs.MetricOperations)
	if !ok {
		t.Fatalf("no %s metric", obs.MetricOperations)
	}
	sum := ops.Data.(metricdata.Sum[int64])
	if len(sum.DataPoints) != 1 || sum.DataPoints[0].Value != 1 {
		t.Fatalf("operations_total = %+v", sum.DataPoints)
	}
	a := attrsOf(sum.DataPoints[0].Attributes)
	if a["component"] != "sql" || a["operation"] != "ping" || a["outcome"] != "success" {
		t.Errorf("labels = %v", a)
	}
	if _, bad := a["db.system"]; bad {
		t.Error("a span attribute leaked into a metric label")
	}
	if _, ok := findMetric(rm, obs.MetricOperationDuration); !ok {
		t.Error("no duration histogram")
	}
}

// The resource says which process this is, and the code and the environment both count.
func TestResourceIdentifiesTheProcess(t *testing.T) {
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "k8s.pod.name=pod-7")
	r := newRig(t, otelobs.Config{Sampler: "always_on"})
	_, op := r.svc.Begin(context.Background(), obs.ComponentSQL, "ping")
	op.End(nil)
	rm := r.collect(t)
	got := map[string]string{}
	for _, a := range rm.Resource.Attributes() {
		got[string(a.Key)] = a.Value.String()
	}
	for k, want := range map[string]string{
		"service.name": "ecoop-test", "service.namespace": "e-coop", "service.version": "1.2.3",
		"deployment.environment.name": "test", "k8s.pod.name": "pod-7",
	} {
		if got[k] != want {
			t.Errorf("resource %s = %q, want %q", k, got[k], want)
		}
	}
}

// An error is an error status and an exception of its TYPE: the text, which can
// carry a row value, never leaves the process.
func TestErrorSpanRecordsTheTypeNeverTheText(t *testing.T) {
	t.Parallel()
	r := newRig(t, otelobs.Config{Sampler: "always_on"})
	secret := "account 4111111111111111 balance 9999.99 for ana@example.com"
	_, op := r.svc.Begin(context.Background(), obs.ComponentSQL, "migrate")
	op.End(fmt.Errorf("wrapped: %w", errors.New(secret)))
	r.flush(t)

	sp := r.spans.GetSpans()[0]
	if sp.Status.Code != codes.Error {
		t.Fatalf("status = %v, want Error", sp.Status.Code)
	}
	if sp.Status.Description != "" {
		t.Errorf("status description = %q, want none", sp.Status.Description)
	}
	var typ string
	for _, ev := range sp.Events {
		for _, a := range ev.Attributes {
			if string(a.Key) == "exception.type" {
				typ = a.Value.String()
			}
		}
	}
	if typ == "" {
		t.Fatal("no exception event")
	}
	blob := fmt.Sprintf("%+v", sp)
	for _, leak := range []string{"4111111111111111", "9999.99", "ana@example.com", "balance"} {
		if strings.Contains(blob, leak) {
			t.Fatalf("%q reached the exported span", leak)
		}
	}
}

// Skipped, retry and dlq are answers, not faults: no error status, and the outcome says what happened.
func TestExpectedOutcomesAreNotErrorSpans(t *testing.T) {
	t.Parallel()
	r := newRig(t, otelobs.Config{Sampler: "always_on"})
	for _, o := range []obs.Outcome{obs.OutcomeSkipped, obs.OutcomeRetry, obs.OutcomeDLQ} {
		_, op := r.svc.Begin(context.Background(), obs.ComponentCDC, "deliver")
		op.SetOutcome(o)
		op.End(errors.New("x"))
	}
	r.flush(t)
	seen := map[string]bool{}
	for _, sp := range r.spans.GetSpans() {
		if sp.Status.Code == codes.Error {
			t.Errorf("a %v span has an error status", sp.Attributes)
		}
		for _, a := range sp.Attributes {
			if string(a.Key) == "ecoop.outcome" {
				seen[a.Value.String()] = true
			}
		}
	}
	for _, o := range []string{"skipped", "retry", "dlq"} {
		if !seen[o] {
			t.Errorf("no span with outcome %s", o)
		}
	}
}

// A child operation started from the parent's context is its child, in one trace.
func TestChildOperationsShareTheTrace(t *testing.T) {
	t.Parallel()
	r := newRig(t, otelobs.Config{Sampler: "always_on"})
	ctx, parent := r.svc.Begin(context.Background(), obs.ComponentAPI, "/members/:id")
	_, child := r.svc.Begin(ctx, obs.ComponentSQL, "ping")
	child.End(nil)
	parent.End(nil)
	r.flush(t)
	spans := r.spans.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("%d spans, want 2", len(spans))
	}
	var p, c tracetest.SpanStub
	for _, s := range spans {
		if s.Name == "sql ping" {
			c = s
		} else {
			p = s
		}
	}
	if c.SpanContext.TraceID() != p.SpanContext.TraceID() {
		t.Error("parent and child are in different traces")
	}
	if c.Parent.SpanID() != p.SpanContext.SpanID() {
		t.Error("the child's parent is not the parent span")
	}
}

// Concurrent siblings from one parent context are isolated and all recorded.
func TestConcurrentSiblingSpans(t *testing.T) {
	t.Parallel()
	r := newRig(t, otelobs.Config{Sampler: "always_on"})
	ctx, parent := r.svc.Begin(context.Background(), obs.ComponentAPI, "/batch")
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			_, c := r.svc.Begin(ctx, obs.ComponentCache, "get")
			c.End(nil)
		})
	}
	wg.Wait()
	parent.End(nil)
	r.flush(t)
	spans := r.spans.GetSpans()
	if len(spans) != 101 {
		t.Fatalf("%d spans, want 101", len(spans))
	}
	parentID := ""
	for _, s := range spans {
		if s.Name == "api /batch" {
			parentID = s.SpanContext.SpanID().String()
		}
	}
	for _, s := range spans {
		if s.Name == "cache get" && s.Parent.SpanID().String() != parentID {
			t.Fatal("a sibling is not a child of the shared parent")
		}
	}
}

// Trace context crosses a message boundary: injected on one side, extracted on the other.
func TestPropagationRoundTrip(t *testing.T) {
	t.Parallel()
	r := newRig(t, otelobs.Config{Sampler: "always_on"})
	ctx, pub := r.svc.Begin(context.Background(), obs.ComponentBroker, "publish")
	headers := obs.MapCarrier{}
	obs.Inject(ctx, r.svc, headers)
	pub.End(nil)
	tp := headers["traceparent"]
	if !strings.HasPrefix(tp, "00-") || len(tp) != 55 {
		t.Fatalf("traceparent = %q, want a W3C value", tp)
	}
	if _, has := headers["baggage"]; has {
		t.Error("baggage was propagated")
	}

	// The consumer is another process: a fresh context, trace continued from the headers.
	cctx := obs.Extract(context.Background(), r.svc, headers)
	_, sub := r.svc.Begin(cctx, obs.ComponentBroker, "handle")
	sub.End(nil)
	r.flush(t)
	var pubSpan, subSpan tracetest.SpanStub
	for _, s := range r.spans.GetSpans() {
		if s.Name == "broker publish" {
			pubSpan = s
		} else {
			subSpan = s
		}
	}
	if subSpan.SpanContext.TraceID() != pubSpan.SpanContext.TraceID() {
		t.Error("the consumer started a new trace instead of continuing")
	}
	if subSpan.Parent.SpanID() != pubSpan.SpanContext.SpanID() {
		t.Error("the consumer's parent is not the publisher's span")
	}
}

// Garbage in a header is ignored: it never panics and never produces a parent.
func TestExtractIgnoresHostileHeaders(t *testing.T) {
	t.Parallel()
	r := newRig(t, otelobs.Config{Sampler: "always_on"})
	for _, v := range []string{
		"", "garbage", "00-" + strings.Repeat("0", 32) + "-" + strings.Repeat("0", 16) + "-01",
		"ff-" + strings.Repeat("a", 32) + "-" + strings.Repeat("b", 16) + "-01",
		strings.Repeat("x", 1<<20), "00-zz-zz-01", "\x00\x01\x02",
	} {
		ctx := obs.Extract(context.Background(), r.svc, obs.MapCarrier{"traceparent": v, "tracestate": v, "baggage": v})
		_, op := r.svc.Begin(ctx, obs.ComponentQueue, "run")
		op.End(nil)
	}
	r.flush(t)
	for _, s := range r.spans.GetSpans() {
		if s.Parent.IsValid() {
			t.Errorf("a hostile header produced a parent: %v", s.Parent)
		}
	}
	if obs.Extract(context.Background(), r.svc, nil) == nil {
		t.Error("Extract(nil carrier) returned a nil context")
	}
}

// With no Backend, propagation does nothing and costs nothing.
func TestPropagationWithoutABackendIsANoop(t *testing.T) {
	t.Parallel()
	svc := obs.New(obs.Resource{}, slog.New(slog.DiscardHandler))
	h := obs.MapCarrier{}
	obs.Inject(context.Background(), svc, h)
	obs.Inject(context.Background(), obs.Nop(), h)
	if len(h) != 0 {
		t.Fatalf("headers written without a backend: %v", h)
	}
	ctx := context.WithValue(context.Background(), ctxKey{}, 1)
	if got := obs.Extract(ctx, obs.Nop(), obs.MapCarrier{"traceparent": "x"}); got != ctx {
		t.Error("Extract changed the context without a backend")
	}
}

// A log line carries the trace and span id of the work it belongs to.
func TestLogsCarryTheTraceID(t *testing.T) {
	t.Parallel()
	r := newRig(t, otelobs.Config{Sampler: "always_on"})
	var got []slog.Attr
	svc := obs.New(obs.Resource{ServiceName: "x"}, slog.New(captureHandler{attrs: &got}), obs.WithBackend(r.backend))
	ctx, op := svc.Begin(context.Background(), obs.ComponentSQL, "ping")
	obs.Logger(ctx, svc, obs.ComponentSQL).Info("working")
	op.End(nil)
	r.flush(t)
	var traceID, spanID string
	for _, a := range got {
		switch a.Key {
		case "trace_id":
			traceID = a.Value.String()
		case "span_id":
			spanID = a.Value.String()
		}
	}
	sp := r.spans.GetSpans()
	if len(sp) == 0 {
		t.Fatal("no span")
	}
	if traceID != sp[len(sp)-1].SpanContext.TraceID().String() || spanID != sp[len(sp)-1].SpanContext.SpanID().String() {
		t.Errorf("log ids (%q, %q) do not match the span (%v)", traceID, spanID, sp[len(sp)-1].SpanContext)
	}
	// Outside a span there is nothing to add.
	got = nil
	obs.Logger(context.Background(), svc, obs.ComponentSQL).Info("no span")
	for _, a := range got {
		if a.Key == "trace_id" {
			t.Error("a trace_id was added with no span")
		}
	}
}

// countingExporter counts what it was given, because InMemoryExporter forgets
// its spans when it is shut down, which is exactly when a flush happens.
type countingExporter struct {
	*tracetest.InMemoryExporter
	mu sync.Mutex
	n  int
}

func (c *countingExporter) ExportSpans(ctx context.Context, s []sdktrace.ReadOnlySpan) error {
	c.mu.Lock()
	c.n += len(s)
	c.mu.Unlock()
	return c.InMemoryExporter.ExportSpans(ctx, s)
}

func (c *countingExporter) exported() int { c.mu.Lock(); defer c.mu.Unlock(); return c.n }

type captureHandler struct{ attrs *[]slog.Attr }

func (captureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h captureHandler) Handle(_ context.Context, r slog.Record) error {
	r.Attrs(func(a slog.Attr) bool { *h.attrs = append(*h.attrs, a); return true })
	return nil
}
func (h captureHandler) WithAttrs(as []slog.Attr) slog.Handler {
	*h.attrs = append(*h.attrs, as...)
	return h
}
func (h captureHandler) WithGroup(string) slog.Handler { return h }

// The sampler decides what is exported, and a parent's decision is honored.
func TestSampling(t *testing.T) {
	t.Parallel()
	count := func(cfg otelobs.Config, n int) int {
		r := newRig(t, cfg)
		for range n {
			_, op := r.svc.Begin(context.Background(), obs.ComponentSQL, "ping")
			op.End(nil)
		}
		r.flush(t)
		return len(r.spans.GetSpans())
	}
	if got := count(otelobs.Config{Sampler: "always_on"}, 50); got != 50 {
		t.Errorf("always_on kept %d of 50", got)
	}
	if got := count(otelobs.Config{Sampler: "always_off"}, 50); got != 0 {
		t.Errorf("always_off kept %d of 50", got)
	}
	got := count(otelobs.Config{Sampler: "traceidratio", SamplerArg: 0.25, SamplerArgSet: true}, 4000)
	if got < 700 || got > 1300 {
		t.Errorf("traceidratio 0.25 kept %d of 4000, want about 1000", got)
	}

	// parentbased: the root is sampled at 0 (so dropped), but a remote parent that was
	// sampled makes its children sampled whatever the ratio.
	r := newRig(t, otelobs.Config{Sampler: "parentbased_traceidratio", SamplerArg: 0, SamplerArgSet: true})
	_, root := r.svc.Begin(context.Background(), obs.ComponentSQL, "root")
	root.End(nil)
	remote := obs.MapCarrier{"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}
	ctx := obs.Extract(context.Background(), r.svc, remote)
	_, child := r.svc.Begin(ctx, obs.ComponentSQL, "continued")
	child.End(nil)
	unsampled := obs.Extract(context.Background(), r.svc, obs.MapCarrier{"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"})
	_, dropped := r.svc.Begin(unsampled, obs.ComponentSQL, "dropped")
	dropped.End(nil)
	r.flush(t)
	names := map[string]bool{}
	for _, s := range r.spans.GetSpans() {
		names[s.Name] = true
	}
	if names["sql root"] {
		t.Error("a root span was kept at ratio 0")
	}
	if !names["sql continued"] {
		t.Error("a child of a sampled remote parent was dropped")
	}
	if names["sql dropped"] {
		t.Error("a child of an unsampled remote parent was kept")
	}
	// Whatever is sampled, the metrics count everything.
	rm := r.collect(t)
	m, _ := findMetric(rm, obs.MetricOperations)
	var total int64
	for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
		total += dp.Value
	}
	if total != 3 {
		t.Errorf("operations_total = %d, want 3: sampling must not drop metrics", total)
	}
}

// Histograms have real buckets, so a percentile can be computed from them.
func TestDurationHistogramHasBuckets(t *testing.T) {
	t.Parallel()
	r := newRig(t, otelobs.Config{Sampler: "always_on"})
	for range 10 {
		_, op := r.svc.Begin(context.Background(), obs.ComponentSQL, "ping")
		time.Sleep(2 * time.Millisecond)
		op.End(nil)
	}
	m, ok := findMetric(r.collect(t), obs.MetricOperationDuration)
	if !ok {
		t.Fatal("no duration histogram")
	}
	h := m.Data.(metricdata.Histogram[float64])
	dp := h.DataPoints[0]
	if dp.Count != 10 {
		t.Fatalf("count = %d, want 10", dp.Count)
	}
	if len(dp.Bounds) != len(otelobs.DurationBuckets) {
		t.Fatalf("%d bucket bounds, want %d", len(dp.Bounds), len(otelobs.DurationBuckets))
	}
	if m.Unit != "s" {
		t.Errorf("unit = %q, want s", m.Unit)
	}
}

// The component's own instruments reach the exporter: a counter, a gauge, a histogram.
func TestOwnInstrumentsAreExported(t *testing.T) {
	t.Parallel()
	r := newRig(t, otelobs.Config{Sampler: "always_on"})
	m := r.svc.Meter(obs.ComponentCQRS)
	ctx := context.Background()
	m.Counter(obs.MetricCQRSBatches, "").Add(ctx, 3)
	g := m.Gauge(obs.MetricCQRSAwaiting, "")
	g.Set(ctx, 7)
	g.Set(ctx, 2)
	m.Histogram(obs.MetricCQRSBatchDuration, "", "s").Record(ctx, 0.5)
	rm := r.collect(t)
	if c, ok := findMetric(rm, obs.MetricCQRSBatches); !ok || c.Data.(metricdata.Sum[int64]).DataPoints[0].Value != 3 {
		t.Errorf("counter not exported: %+v", c)
	}
	if gg, ok := findMetric(rm, obs.MetricCQRSAwaiting); !ok || gg.Data.(metricdata.Gauge[float64]).DataPoints[0].Value != 2 {
		t.Errorf("gauge is not the last value set: %+v", gg)
	}
	if h, ok := findMetric(rm, obs.MetricCQRSBatchDuration); !ok || h.Data.(metricdata.Histogram[float64]).DataPoints[0].Count != 1 {
		t.Errorf("histogram not exported: %+v", h)
	}
}

// Spans never carry a secret, however it is passed.
func TestSpanAttributesAreScrubbed(t *testing.T) {
	t.Parallel()
	r := newRig(t, otelobs.Config{Sampler: "always_on"})
	_, op := r.svc.Begin(context.Background(), obs.ComponentAuth, "load_session",
		slog.String("password", "hunter2"),
		slog.String("Authorization", "Bearer abcdefghijklmnop"),
		slog.String("x-csrf-token", "tok-123"),
		slog.String("note", "contact ana@example.com about card 4111111111111111"),
		slog.String("jwt", "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abc"),
		slog.String("db.query.text", "SELECT * FROM accounts WHERE owner = 'Ana' AND balance > 1000 AND id = $1"),
		slog.Any("claim", struct{ Email string }{"ana@example.com"}),
		slog.String("ok", "plain"),
		slog.Int("count", 3),
	)
	op.SetAttributes(slog.String("session_id", "s-abc"), slog.String("late", "bearer zzzzzzzzzzzz"))
	op.End(nil)
	r.flush(t)
	attrs := map[string]string{}
	for _, a := range r.spans.GetSpans()[0].Attributes {
		attrs[string(a.Key)] = a.Value.String()
	}
	blob := fmt.Sprint(attrs)
	for _, leak := range []string{"hunter2", "abcdefghijklmnop", "tok-123", "ana@example.com", "4111111111111111", "eyJhbGci", "Ana", "1000", "s-abc", "zzzzzzzzzzzz"} {
		if strings.Contains(blob, leak) {
			t.Errorf("%q reached the span: %v", leak, attrs)
		}
	}
	for _, k := range []string{"password", "Authorization", "x-csrf-token", "session_id"} {
		if attrs[k] != "[redacted]" {
			t.Errorf("%s = %q, want [redacted]", k, attrs[k])
		}
	}
	if q := attrs["db.query.text"]; q != "SELECT * FROM accounts WHERE owner = ? AND balance > ? AND id = $1" {
		t.Errorf("db.query.text = %q, want literals removed and the $1 placeholder kept", q)
	}
	if attrs["ok"] != "plain" || attrs["count"] != "3" {
		t.Errorf("innocent attributes were changed: %v", attrs)
	}
	if attrs["claim"] == "" || strings.Contains(attrs["claim"], "ana@") {
		t.Errorf("a struct attribute exposed its content: %q", attrs["claim"])
	}
}

// A hostile attribute volume is bounded.
func TestAttributeVolumeIsBounded(t *testing.T) {
	t.Parallel()
	r := newRig(t, otelobs.Config{Sampler: "always_on"})
	attrs := make([]slog.Attr, 5000)
	for i := range attrs {
		attrs[i] = slog.String(fmt.Sprintf("k%d", i), strings.Repeat("v", 100_000))
	}
	_, op := r.svc.Begin(context.Background(), obs.ComponentSQL, "ping", attrs...)
	op.End(nil)
	r.flush(t)
	sp := r.spans.GetSpans()[0]
	if len(sp.Attributes) > 64+8 {
		t.Errorf("%d attributes on a span, want at most 72", len(sp.Attributes))
	}
	for _, a := range sp.Attributes {
		if len(a.Value.String()) > 256 {
			t.Fatalf("an attribute value of %d bytes was exported", len(a.Value.String()))
		}
	}
}

// Stop flushes: nothing recorded just before it is lost.
func TestStopFlushesBufferedSpans(t *testing.T) {
	t.Parallel()
	spans := &countingExporter{InMemoryExporter: tracetest.NewInMemoryExporter()}
	b, err := otelobs.Open(otelobs.Config{Sampler: "always_on", BatchTimeout: time.Hour},
		otelobs.WithSpanExporter(spans), otelobs.WithMetricReader(sdkmetric.NewManualReader()))
	if err != nil {
		t.Fatal(err)
	}
	svc := obs.New(obs.Resource{}, slog.New(slog.DiscardHandler), obs.WithBackend(b))
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for range 25 {
		_, op := svc.Begin(context.Background(), obs.ComponentSQL, "ping")
		op.End(nil)
	}
	if n := spans.exported(); n != 0 {
		t.Fatalf("%d spans were exported before Stop with a one-hour batch timeout", n)
	}
	if err := svc.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if n := spans.exported(); n != 25 {
		t.Fatalf("%d spans exported by Stop, want all 25: Stop did not flush", n)
	}
	if err := svc.Stop(context.Background()); err != nil {
		t.Errorf("second Stop = %v, want nil", err)
	}
}
