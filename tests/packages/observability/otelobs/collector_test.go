package otelobs_test

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	obs "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability/otelobs"
)

// blockedAfter is how long a loop of operations may take before it counts as
// blocked on the exporter. A loop that never waits for the collector takes well
// under a second even with -race; one that waits for it takes minutes (retries,
// a 500ms answer per batch, a 30s hold). The bound sits between the two on
// purpose: it must not fail on a busy machine running several packages under
// -race, only when the application really waits for telemetry. The cost per
// operation is measured by the benchmarks, not asserted here.
const blockedAfter = 30 * time.Second

// collector is a fake OTLP/HTTP collector: it records what is posted to it and
// answers as told, so the whole export path (batching, gzip, retries, timeouts,
// shutdown) runs over a real HTTP connection with no external service.
type collector struct {
	srv *httptest.Server

	mu      sync.Mutex
	paths   map[string]int
	bytes   map[string]int
	headers http.Header
	status  atomic.Int32 // 0 = 200
	delay   atomic.Int64 // nanoseconds to hold each request
	hits    atomic.Int64
	release chan struct{} // closed at cleanup so held requests return
}

func newCollector(t testing.TB) *collector {
	t.Helper()
	c := &collector{paths: map[string]int{}, bytes: map[string]int{}, release: make(chan struct{})}
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.hits.Add(1)
		if d := time.Duration(c.delay.Load()); d > 0 {
			select {
			case <-time.After(d):
			case <-r.Context().Done():
				return
			case <-c.release:
				return
			}
		}
		body := r.Body
		if r.Header.Get("Content-Encoding") == "gzip" {
			if gz, err := gzip.NewReader(r.Body); err == nil {
				body = gz
			}
		}
		n, _ := io.Copy(io.Discard, body)
		c.mu.Lock()
		c.paths[r.URL.Path]++
		c.bytes[r.URL.Path] += int(n)
		if c.headers == nil {
			c.headers = r.Header.Clone()
		}
		c.mu.Unlock()
		if s := int(c.status.Load()); s != 0 {
			w.WriteHeader(s)
			return
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
	}))
	// Cleanups run last-in first-out: release the held requests, then close.
	t.Cleanup(c.srv.Close)
	t.Cleanup(func() { close(c.release) })
	return c
}

func (c *collector) count(path string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.paths[path]
}

func (c *collector) cfg() otelobs.Config {
	return otelobs.Config{
		Endpoint: c.srv.URL, Insecure: true, Sampler: "always_on",
		BatchTimeout: 20 * time.Millisecond, MetricInterval: 50 * time.Millisecond,
		ExportTimeout: 2 * time.Second, ShutdownTimeout: 3 * time.Second,
	}
}

func openOTLP(t testing.TB, cfg otelobs.Config) (obs.IObservabilityService, *otelobs.Backend) {
	t.Helper()
	b, err := otelobs.Open(cfg, otelobs.WithResource(obs.Resource{ServiceName: "ecoop-test"}), otelobs.WithLogger(slog.New(slog.DiscardHandler)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	svc := obs.New(obs.Resource{ServiceName: "ecoop-test"}, slog.New(slog.DiscardHandler), obs.WithBackend(b))
	return svc, b
}

func eventually(t testing.TB, d time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Spans and metrics reach the collector over OTLP/HTTP, compressed, on the
// standard paths, with the configured headers.
func TestOTLP_ExportsSpansAndMetricsOverHTTP(t *testing.T) {
	t.Parallel()
	c := newCollector(t)
	cfg := c.cfg()
	cfg.Headers = map[string]string{"x-api-key": "k-123"}
	svc, _ := openOTLP(t, cfg)
	t.Cleanup(func() { _ = svc.Stop(context.Background()) })
	for range 20 {
		_, op := svc.Begin(context.Background(), obs.ComponentSQL, "ping")
		op.End(nil)
	}
	eventually(t, 5*time.Second, "spans at /v1/traces", func() bool { return c.count("/v1/traces") > 0 })
	eventually(t, 5*time.Second, "metrics at /v1/metrics", func() bool { return c.count("/v1/metrics") > 0 })
	c.mu.Lock()
	defer c.mu.Unlock()
	if got := c.headers.Get("x-api-key"); got != "k-123" {
		t.Errorf("x-api-key = %q, want it forwarded", got)
	}
	if c.headers.Get("Content-Encoding") != "gzip" {
		t.Errorf("Content-Encoding = %q, want gzip", c.headers.Get("Content-Encoding"))
	}
	if c.bytes["/v1/traces"] == 0 {
		t.Error("an empty trace payload was posted")
	}
}

// Collector down: the application is not slowed or failed, spans are dropped and counted.
func TestOTLP_CollectorOutageNeverBlocksTheApplication(t *testing.T) {
	t.Parallel()
	c := newCollector(t)
	cfg := c.cfg()
	c.srv.Close() // down from the start
	svc, b := openOTLP(t, cfg)

	start := time.Now()
	for range 5000 {
		_, op := svc.Begin(context.Background(), obs.ComponentSQL, "ping")
		op.End(nil)
	}
	if took := time.Since(start); took > blockedAfter {
		t.Fatalf("5000 operations took %v with the collector down (bound %v): telemetry blocked the application", took, blockedAfter)
	}
	stopped := make(chan error, 1)
	go func() { stopped <- svc.Stop(context.Background()) }()
	select {
	case <-stopped:
	case <-time.After(15 * time.Second):
		t.Fatal("Stop hung with the collector down")
	}
	if b.Dropped() == 0 {
		t.Error("spans were lost with the collector down but none were counted as dropped")
	}
}

// Collector slow: an export is bounded, Stop is bounded, and the app never waits.
func TestOTLP_SlowCollectorIsBounded(t *testing.T) {
	t.Parallel()
	c := newCollector(t)
	cfg := c.cfg()
	cfg.ExportTimeout = 300 * time.Millisecond
	cfg.ShutdownTimeout = time.Second
	c.delay.Store(int64(30 * time.Second))
	svc, _ := openOTLP(t, cfg)

	start := time.Now()
	for range 2000 {
		_, op := svc.Begin(context.Background(), obs.ComponentSQL, "ping")
		op.End(nil)
	}
	if took := time.Since(start); took > blockedAfter {
		t.Fatalf("2000 operations took %v against a hung collector (bound %v)", took, blockedAfter)
	}
	stopStart := time.Now()
	_ = svc.Stop(context.Background())
	if took := time.Since(stopStart); took > 8*time.Second {
		t.Fatalf("Stop took %v against a hung collector, want it bounded by ShutdownTimeout", took)
	}
}

// A collector that answers 5xx then recovers: the exporter retries and the data arrives.
func TestOTLP_RecoversAfterATransientOutage(t *testing.T) {
	t.Parallel()
	c := newCollector(t)
	c.status.Store(http.StatusServiceUnavailable)
	svc, _ := openOTLP(t, c.cfg())
	t.Cleanup(func() { _ = svc.Stop(context.Background()) })
	_, op := svc.Begin(context.Background(), obs.ComponentSQL, "ping")
	op.End(nil)
	eventually(t, 5*time.Second, "a first (failed) attempt", func() bool { return c.hits.Load() > 0 })
	c.status.Store(0)
	eventually(t, 20*time.Second, "delivery after recovery", func() bool { return c.count("/v1/traces") > 0 })
}

// Queue saturation under a burst: memory stays bounded and the app keeps its pace.
func TestOTLP_QueueSaturationDropsInsteadOfGrowing(t *testing.T) {
	t.Parallel()
	c := newCollector(t)
	cfg := c.cfg()
	cfg.MaxQueue, cfg.MaxBatch = 64, 32
	c.delay.Store(int64(500 * time.Millisecond))
	svc, _ := openOTLP(t, cfg)
	t.Cleanup(func() { _ = svc.Stop(context.Background()) })
	start := time.Now()
	for range 20000 {
		_, op := svc.Begin(context.Background(), obs.ComponentSQL, "ping")
		op.End(nil)
	}
	if took := time.Since(start); took > blockedAfter {
		t.Fatalf("20000 operations took %v with a saturated exporter queue (bound %v)", took, blockedAfter)
	}
	var total float64
	if s, ok := svc.(*obs.Service); ok {
		for _, sm := range s.Snapshot() {
			if sm.Name == obs.MetricOperations {
				total += sm.Value
			}
		}
	}
	if total != 20000 {
		t.Errorf("registry counted %v operations, want 20000: dropping spans must not drop counts", total)
	}
}

// No collector configured: it records in process, exports nothing, and still works.
func TestOTLP_DisabledExportsNothing(t *testing.T) {
	t.Parallel()
	svc, b := openOTLP(t, otelobs.Config{})
	_, op := svc.Begin(context.Background(), obs.ComponentSQL, "ping")
	op.End(nil)
	if err := svc.Stop(context.Background()); err != nil {
		t.Fatalf("Stop = %v", err)
	}
	if b.Dropped() != 0 {
		t.Errorf("Dropped = %d with nothing to export", b.Dropped())
	}
}

// A panicking Backend cannot reach the caller.
type panicBackend struct{ obs.Backend }

func (panicBackend) StartSpan(context.Context, string, string, []obs.Attr) (context.Context, obs.BackendSpan) {
	panic("backend down")
}
func (panicBackend) RecordOperation(context.Context, map[string]string, obs.Outcome, float64) {
	panic("backend down")
}
func (panicBackend) Inflight(context.Context, map[string]string, int64) { panic("backend down") }
func (panicBackend) Shutdown(context.Context) error                     { panic("backend down") }

func TestAPanickingBackendNeverReachesTheCaller(t *testing.T) {
	t.Parallel()
	svc := obs.New(obs.Resource{}, slog.New(slog.DiscardHandler), obs.WithBackend(panicBackend{}))
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("a Backend panic reached the caller: %v", r)
		}
	}()
	ctx, op := svc.Begin(context.Background(), obs.ComponentSQL, "ping")
	op.End(errors.New("x"))
	_, sp := svc.Tracer(obs.ComponentSQL).Start(ctx, "s")
	sp.End()
	_ = svc.Stop(context.Background())
	if s, ok := svc.(*obs.Service); ok {
		for _, sm := range s.Snapshot() {
			if sm.Name == obs.MetricOperations && sm.Value == 1 {
				return
			}
		}
		t.Error("the in-memory registry lost the count when the backend panicked")
	}
}

// ---- config ---------------------------------------------------------------

func env(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func TestConfigFromEnv(t *testing.T) {
	t.Parallel()
	good, err := otelobs.FromEnv(env(map[string]string{
		"OTEL_EXPORTER_OTLP_ENDPOINT": "https://collector.example:4318/", "OTEL_EXPORTER_OTLP_HEADERS": "authorization=Bearer abc, x-team=core",
		"OTEL_TRACES_SAMPLER": "parentbased_traceidratio", "OTEL_TRACES_SAMPLER_ARG": "0.1",
		"OTEL_EXPORTER_OTLP_TIMEOUT": "3000", "OTEL_METRIC_EXPORT_INTERVAL": "20000", "OTEL_BSP_MAX_QUEUE_SIZE": "4096", "OTEL_BSP_MAX_EXPORT_BATCH_SIZE": "256",
	}))
	if err != nil {
		t.Fatalf("a valid environment was rejected: %v", err)
	}
	if good.Endpoint != "https://collector.example:4318/" || good.Headers["authorization"] != "Bearer abc" || good.Headers["x-team"] != "core" ||
		good.SamplerArg != 0.1 || !good.SamplerArgSet || good.ExportTimeout != 3*time.Second || good.MetricInterval != 20*time.Second || good.MaxQueue != 4096 || good.MaxBatch != 256 {
		t.Errorf("FromEnv = %+v", good)
	}

	bad := map[string]map[string]string{
		"unknown sampler":     {"OTEL_EXPORTER_OTLP_ENDPOINT": "https://c:4318", "OTEL_TRACES_SAMPLER": "sometimes"},
		"ratio above one":     {"OTEL_EXPORTER_OTLP_ENDPOINT": "https://c:4318", "OTEL_TRACES_SAMPLER": "traceidratio", "OTEL_TRACES_SAMPLER_ARG": "1.5"},
		"ratio not a number":  {"OTEL_EXPORTER_OTLP_ENDPOINT": "https://c:4318", "OTEL_TRACES_SAMPLER_ARG": "lots"},
		"negative ratio":      {"OTEL_EXPORTER_OTLP_ENDPOINT": "https://c:4318", "OTEL_TRACES_SAMPLER_ARG": "-0.1"},
		"not a url":           {"OTEL_EXPORTER_OTLP_ENDPOINT": "collector:4318"},
		"wrong scheme":        {"OTEL_EXPORTER_OTLP_ENDPOINT": "ftp://c:4318"},
		"plain http, no flag": {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://c:4318"},
		"bad timeout":         {"OTEL_EXPORTER_OTLP_ENDPOINT": "https://c:4318", "OTEL_EXPORTER_OTLP_TIMEOUT": "10s"},
		"negative interval":   {"OTEL_EXPORTER_OTLP_ENDPOINT": "https://c:4318", "OTEL_METRIC_EXPORT_INTERVAL": "-5"},
		"batch above queue":   {"OTEL_EXPORTER_OTLP_ENDPOINT": "https://c:4318", "OTEL_BSP_MAX_QUEUE_SIZE": "10", "OTEL_BSP_MAX_EXPORT_BATCH_SIZE": "20"},
		"malformed header":    {"OTEL_EXPORTER_OTLP_ENDPOINT": "https://c:4318", "OTEL_EXPORTER_OTLP_HEADERS": "novalue"},
		"non numeric queue":   {"OTEL_EXPORTER_OTLP_ENDPOINT": "https://c:4318", "OTEL_BSP_MAX_QUEUE_SIZE": "many"},
	}
	for name, vars := range bad {
		if _, err := otelobs.FromEnv(env(vars)); err == nil {
			t.Errorf("%s: a malformed environment was accepted", name)
		}
	}

	// http is allowed when it is said to be intended.
	if _, err := otelobs.FromEnv(env(map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://c:4318", "OTEL_EXPORTER_OTLP_INSECURE": "true"})); err != nil {
		t.Errorf("explicit insecure http was rejected: %v", err)
	}
	// No endpoint, or the SDK disabled, is valid and means "export nothing".
	for name, vars := range map[string]map[string]string{
		"empty": {}, "disabled": {"OTEL_SDK_DISABLED": "true", "OTEL_EXPORTER_OTLP_ENDPOINT": "garbage"},
	} {
		c, err := otelobs.FromEnv(env(vars))
		if err != nil || !c.Disabled() {
			t.Errorf("%s: Disabled = %v, err = %v; want disabled and valid", name, c.Disabled(), err)
		}
	}
}

// A secret in the configuration never reaches an error message.
func TestConfigErrorsNeverContainCredentials(t *testing.T) {
	t.Parallel()
	for _, vars := range []map[string]string{
		{"OTEL_EXPORTER_OTLP_ENDPOINT": "https://user:p4ssw0rd-secret@c:4318?token=abc123"},
		{"OTEL_EXPORTER_OTLP_ENDPOINT": "https://c:4318", "OTEL_EXPORTER_OTLP_HEADERS": "secret-api-key-value"},
		{"OTEL_EXPORTER_OTLP_ENDPOINT": "ftp://user:p4ssw0rd-secret@c:4318"},
	} {
		_, err := otelobs.FromEnv(env(vars))
		if err == nil {
			continue
		}
		for _, leak := range []string{"p4ssw0rd-secret", "abc123", "secret-api-key-value"} {
			if strings.Contains(err.Error(), leak) {
				t.Errorf("error leaks %q: %v", leak, err)
			}
		}
	}
}

// Open refuses an invalid configuration, so a typo fails at startup.
func TestOpenRejectsAnInvalidConfig(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]otelobs.Config{
		"sampler":  {Sampler: "sometimes"},
		"endpoint": {Endpoint: "not a url"},
		"http":     {Endpoint: "http://c:4318"},
		"queue":    {MaxQueue: 5, MaxBatch: 50},
	} {
		if b, err := otelobs.Open(cfg); err == nil {
			_ = b.Shutdown(context.Background())
			t.Errorf("%s: Open accepted an invalid config", name)
		}
	}
}

func TestParseLevel(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]slog.Level{
		"": slog.LevelInfo, "info": slog.LevelInfo, "INFO": slog.LevelInfo, " debug ": slog.LevelDebug,
		"warn": slog.LevelWarn, "Warning": slog.LevelWarn, "error": slog.LevelError,
	} {
		got, err := obs.ParseLevel(in)
		if err != nil || got != want {
			t.Errorf("ParseLevel(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, bad := range []string{"verbose", "trace", "1", "info,debug", strings.Repeat("x", 1000)} {
		_, err := obs.ParseLevel(bad)
		if !errors.Is(err, obs.ErrInvalidLogLevel) {
			t.Errorf("ParseLevel(%q) = %v, want ErrInvalidLogLevel", bad[:min(len(bad), 10)], err)
		}
		if err != nil && len(err.Error()) > 200 {
			t.Errorf("the error for a long level is %d bytes", len(err.Error()))
		}
	}
}
