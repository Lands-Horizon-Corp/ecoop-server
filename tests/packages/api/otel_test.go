package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/api"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability/otelobs"
)

// No build tag: the OTel backend writes to memory, the engine is in process.

const (
	sampledParent   = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	sampledTraceID  = "4bf92f3577b34da6a3ce929d0e0e4736"
	attackerSpanID  = "00f067aa0ba902b7"
	unsampledParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"
)

func otelService(t testing.TB, cfg otelobs.Config) (observability.IObservabilityService, *tracetest.InMemoryExporter, *otelobs.Backend) {
	t.Helper()
	spans := tracetest.NewInMemoryExporter()
	b, err := otelobs.Open(cfg, otelobs.WithSpanExporter(spans), otelobs.WithMetricReader(sdkmetric.NewManualReader()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	svc := observability.New(observability.Resource{ServiceName: "ecoop-test"}, slog.New(slog.DiscardHandler), observability.WithBackend(b))
	t.Cleanup(func() { _ = svc.Stop(context.Background()) })
	return svc, spans, b
}

func otelEngine(o observability.IObserver, opts ...api.ObserveOption) *server.Hertz {
	h := server.Default(server.WithHostPorts("127.0.0.1:0"))
	h.Use(api.Observe(o, opts...))
	h.GET("/members/:id", func(_ context.Context, c *app.RequestContext) { c.String(200, "ok") })
	h.GET(api.LivenessPath, api.Liveness())
	return h
}

func get(h *server.Hertz, path string, headers ...ut.Header) *ut.ResponseRecorder {
	return ut.PerformRequest(h.Engine, "GET", path, nil, headers...)
}

// By default a traceparent from the request is ignored: a stranger cannot pick
// the trace id, force a trace to be sampled, or hang their traffic on someone's trace.
func TestOTel_RequestTraceparentIsIgnoredByDefault(t *testing.T) {
	t.Parallel()
	svc, spans, b := otelService(t, otelobs.Config{Sampler: "parentbased_traceidratio", SamplerArg: 0, SamplerArgSet: true})
	h := otelEngine(svc)
	for range 20 {
		get(h, "/members/1", ut.Header{Key: "traceparent", Value: sampledParent}, ut.Header{Key: "tracestate", Value: "evil=1"})
	}
	_ = b.ForceFlush(context.Background())
	if n := len(spans.GetSpans()); n != 0 {
		t.Fatalf("%d spans exported: a hostile traceparent forced sampling at ratio 0", n)
	}
}

func TestOTel_NoRequestEverChoosesTheTraceID(t *testing.T) {
	t.Parallel()
	svc, spans, b := otelService(t, otelobs.Config{Sampler: "always_on"})
	h := otelEngine(svc)
	get(h, "/members/1", ut.Header{Key: "traceparent", Value: sampledParent})
	_ = b.ForceFlush(context.Background())
	for _, s := range spans.GetSpans() {
		if s.SpanContext.TraceID().String() == sampledTraceID || s.Parent.IsValid() {
			t.Fatalf("the request's trace was adopted: trace %s parent %s", s.SpanContext.TraceID(), s.Parent.SpanID())
		}
	}
}

// With TrustTraceHeaders (an internal hop), the trace continues, and the sampling decision is the caller's.
func TestOTel_TrustedHopContinuesTheTrace(t *testing.T) {
	t.Parallel()
	svc, spans, b := otelService(t, otelobs.Config{Sampler: "parentbased_traceidratio", SamplerArg: 0, SamplerArgSet: true})
	h := otelEngine(svc, api.TrustTraceHeaders())
	get(h, "/members/1", ut.Header{Key: "traceparent", Value: sampledParent})
	get(h, "/members/2", ut.Header{Key: "traceparent", Value: unsampledParent})
	get(h, "/members/3") // no header: a root at ratio 0, so dropped
	_ = b.ForceFlush(context.Background())
	got := spans.GetSpans()
	if len(got) != 1 {
		t.Fatalf("%d spans, want 1 (only the sampled parent's)", len(got))
	}
	if got[0].SpanContext.TraceID().String() != sampledTraceID || got[0].Parent.SpanID().String() != attackerSpanID {
		t.Errorf("the span did not continue the trace: %v parent %v", got[0].SpanContext, got[0].Parent)
	}
}

// A malformed header from a trusted hop is ignored, and the request still works.
func TestOTel_TrustedHopWithGarbageHeaderStillServes(t *testing.T) {
	t.Parallel()
	svc, spans, b := otelService(t, otelobs.Config{Sampler: "always_on"})
	h := otelEngine(svc, api.TrustTraceHeaders())
	for _, v := range []string{"garbage", strings.Repeat("a", 100000), "00-0-0-0", "\x00"} {
		if got := get(h, "/members/1", ut.Header{Key: "traceparent", Value: v}).Result().StatusCode(); got != 200 {
			t.Fatalf("status = %d for a malformed traceparent", got)
		}
	}
	_ = b.ForceFlush(context.Background())
	for _, s := range spans.GetSpans() {
		if s.Parent.IsValid() {
			t.Errorf("a malformed header produced a parent: %v", s.Parent)
		}
	}
}

// The response carries the trace id only when asked to.
func TestOTel_TraceIDIsExposedOnlyWhenAsked(t *testing.T) {
	t.Parallel()
	svc, _, _ := otelService(t, otelobs.Config{Sampler: "always_on"})
	if got := string(get(otelEngine(svc), "/members/1").Result().Header.Peek("traceparent")); got != "" {
		t.Errorf("traceparent = %q in a response by default", got)
	}
	got := string(get(otelEngine(svc, api.ExposeTraceID()), "/members/1").Result().Header.Peek("traceparent"))
	if !strings.HasPrefix(got, "00-") || len(got) != 55 {
		t.Errorf("traceparent = %q, want a W3C value", got)
	}
}

// Probes are left out, everything else is observed.
func TestOTel_SkippedRoutesAreNotObserved(t *testing.T) {
	t.Parallel()
	o := newObserver(t)
	h := server.Default(server.WithHostPorts("127.0.0.1:0"))
	h.Use(api.Observe(o, api.SkipRoutes(api.LivenessPath)))
	h.GET(api.LivenessPath, api.Liveness())
	h.GET("/members/:id", func(_ context.Context, c *app.RequestContext) { c.String(200, "ok") })
	for range 10 {
		get(h, api.LivenessPath)
	}
	get(h, "/members/1")
	if got := apiCount(o, api.LivenessPath, "success"); got != 0 {
		t.Errorf("the liveness probe was counted %v times", got)
	}
	if got := apiCount(o, "/members/:id", "success"); got != 1 {
		t.Errorf("a normal route was counted %v times, want 1", got)
	}
}

// The API span is the parent of what the handler does, so a trace shows the whole request.
func TestOTel_HandlerWorkIsAChildOfTheRequestSpan(t *testing.T) {
	t.Parallel()
	svc, spans, b := otelService(t, otelobs.Config{Sampler: "always_on"})
	h := server.Default(server.WithHostPorts("127.0.0.1:0"))
	h.Use(api.Observe(svc))
	h.GET("/members/:id", func(ctx context.Context, c *app.RequestContext) {
		_, op := svc.Begin(ctx, observability.ComponentSQL, "ping")
		op.End(nil)
		c.String(200, "ok")
	})
	get(h, "/members/9")
	_ = b.ForceFlush(context.Background())
	var req, child int
	var reqID, childParent string
	for _, s := range spans.GetSpans() {
		switch s.Name {
		case "api /members/:id":
			req++
			reqID = s.SpanContext.SpanID().String()
		case "sql ping":
			child++
			childParent = s.Parent.SpanID().String()
		}
	}
	if req != 1 || child != 1 || reqID != childParent {
		t.Fatalf("request spans %d, child spans %d, child parent %q vs request %q", req, child, childParent, reqID)
	}
}

// ---- health ---------------------------------------------------------------

type healthBody struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks"`
}

func probe(t testing.TB, h *server.Hertz, path string) (int, healthBody, string) {
	t.Helper()
	w := get(h, path)
	var b healthBody
	raw := string(w.Result().Body())
	_ = json.Unmarshal([]byte(raw), &b)
	return w.Result().StatusCode(), b, raw
}

func TestHealth_LivenessAlwaysAnswers(t *testing.T) {
	t.Parallel()
	h := server.Default(server.WithHostPorts("127.0.0.1:0"))
	h.GET(api.LivenessPath, api.Liveness())
	code, body, _ := probe(t, h, api.LivenessPath)
	if code != 200 || body.Status != "alive" {
		t.Fatalf("liveness = %d %+v", code, body)
	}
}

func readyEngine(timeout time.Duration, checks ...api.Check) *server.Hertz {
	h := server.Default(server.WithHostPorts("127.0.0.1:0"))
	h.GET(api.ReadinessPath, api.Readiness(nil, timeout, checks...))
	return h
}

func TestHealth_ReadyWhenEveryCheckPasses(t *testing.T) {
	t.Parallel()
	h := readyEngine(0,
		api.Check{Name: "database", Fn: func(context.Context) error { return nil }},
		api.Check{Name: "cache", Fn: func(context.Context) error { return nil }})
	code, body, _ := probe(t, h, api.ReadinessPath)
	if code != 200 || body.Status != "ready" || body.Checks["database"] != "healthy" || body.Checks["cache"] != "healthy" {
		t.Fatalf("readiness = %d %+v", code, body)
	}
}

// One failing dependency makes the service not ready, names which, and leaks nothing.
func TestHealth_NotReadyNamesTheCheckAndHidesTheError(t *testing.T) {
	t.Parallel()
	h := readyEngine(0,
		api.Check{Name: "database", Fn: func(context.Context) error {
			return errors.New(`dial tcp 10.1.2.3:5432: password authentication failed for user "ecoop_admin"`)
		}},
		api.Check{Name: "cache", Fn: func(context.Context) error { return nil }})
	code, body, raw := probe(t, h, api.ReadinessPath)
	if code != 503 || body.Status != "not_ready" {
		t.Fatalf("readiness = %d %+v, want 503", code, body)
	}
	if body.Checks["database"] != "unhealthy" || body.Checks["cache"] != "healthy" {
		t.Errorf("checks = %v", body.Checks)
	}
	for _, leak := range []string{"10.1.2.3", "5432", "ecoop_admin", "password", "dial tcp"} {
		if strings.Contains(raw, leak) {
			t.Errorf("%q leaked into the readiness response: %s", leak, raw)
		}
	}
}

// A hung dependency cannot hang the probe, and a panicking check is just a failure.
func TestHealth_HungAndPanickingChecksAreBounded(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	h := readyEngine(200*time.Millisecond,
		api.Check{Name: "hung", Fn: func(ctx context.Context) error { <-release; return nil }},
		api.Check{Name: "panics", Fn: func(context.Context) error { panic("boom") }},
		api.Check{Name: "nil"},
		api.Check{Name: "fine", Fn: func(context.Context) error { return nil }})
	start := time.Now()
	code, body, _ := probe(t, h, api.ReadinessPath)
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("the probe took %v with a hung check", took)
	}
	if code != 503 {
		t.Fatalf("status = %d, want 503", code)
	}
	for _, n := range []string{"hung", "panics", "nil"} {
		if body.Checks[n] != "unhealthy" {
			t.Errorf("%s = %q, want unhealthy", n, body.Checks[n])
		}
	}
	if body.Checks["fine"] != "healthy" {
		t.Errorf("a passing check was reported %q", body.Checks["fine"])
	}
}

// With nothing to check, the service is ready, and checks run in parallel.
func TestHealth_NoChecksAndParallelChecks(t *testing.T) {
	t.Parallel()
	if code, body, _ := probe(t, readyEngine(0), api.ReadinessPath); code != 200 || body.Status != "ready" {
		t.Fatalf("no checks = %d %+v", code, body)
	}
	var running, peak atomic.Int32
	slow := func(context.Context) error {
		n := running.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
		running.Add(-1)
		return nil
	}
	h := readyEngine(0, api.Check{Name: "a", Fn: slow}, api.Check{Name: "b", Fn: slow}, api.Check{Name: "c", Fn: slow})
	probe(t, h, api.ReadinessPath)
	if peak.Load() < 2 {
		t.Errorf("at most %d checks ran at once, want them concurrent", peak.Load())
	}
}
