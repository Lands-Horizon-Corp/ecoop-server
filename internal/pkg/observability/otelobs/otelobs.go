package otelobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
)

// DurationBuckets are the explicit histogram boundaries, in seconds, of every
// duration instrument. They cover a cache hit (a millisecond) to a long
// migration (minutes), so p50, p95 and p99 can be read from them in Grafana.
var DurationBuckets = []float64{
	0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300,
}

// Backend implements observability.Backend over OpenTelemetry. Create it with
// Open and give it to observability.New with observability.WithBackend.
type Backend struct {
	cfg  Config
	log  *slog.Logger
	prop propagation.TextMapPropagator

	tp *sdktrace.TracerProvider
	mp *sdkmetric.MeterProvider

	tracer trace.Tracer
	meter  metric.Meter

	ops      metric.Int64Counter
	duration metric.Float64Histogram
	inflight metric.Int64UpDownCounter

	mu     sync.Mutex
	counts map[string]metric.Int64Counter
	gauges map[string]*gauge
	hists  map[string]metric.Float64Histogram

	dropped *dropCounter
	closed  bool
}

var _ observability.Backend = (*Backend)(nil)

// Option configures Open.
type Option func(*options)

type options struct {
	res      observability.Resource
	log      *slog.Logger
	traceExp sdktrace.SpanExporter
	reader   sdkmetric.Reader
}

// WithResource says which process is reporting. Every span and metric carries it.
func WithResource(r observability.Resource) Option { return func(o *options) { o.res = r } }

// WithLogger is where the exporter's own failures are logged. Default: discard.
func WithLogger(l *slog.Logger) Option { return func(o *options) { o.log = l } }

// WithSpanExporter replaces the OTLP span exporter, for a test that reads spans
// in memory.
func WithSpanExporter(e sdktrace.SpanExporter) Option { return func(o *options) { o.traceExp = e } }

// WithMetricReader replaces the periodic OTLP reader, for a test that collects
// metrics on demand.
func WithMetricReader(r sdkmetric.Reader) Option { return func(o *options) { o.reader = r } }

// Open builds the Backend. With a Disabled Config it records into the SDK's
// in-process providers and exports nothing. It does not contact the collector:
// an unreachable collector must never stop the service from starting, and
// export failures are retried and then dropped, never raised to the caller.
func Open(cfg Config, opts ...Option) (*Backend, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg = cfg.withDefaults()
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	if o.log == nil {
		o.log = slog.New(slog.DiscardHandler)
	}
	b := &Backend{
		cfg:     cfg,
		log:     o.log,
		counts:  map[string]metric.Int64Counter{},
		gauges:  map[string]*gauge{},
		hists:   map[string]metric.Float64Histogram{},
		dropped: &dropCounter{},
		// W3C trace context only. Baggage is not propagated: it is free-form
		// key/value data that rides along every hop, which is where an identifier
		// or a secret would leak across a service boundary.
		prop: propagation.TraceContext{},
	}
	res, err := newResource(o.res)
	if err != nil {
		return nil, err
	}
	if err := b.openTraces(o, res); err != nil {
		return nil, err
	}
	if err := b.openMetrics(o, res); err != nil {
		_ = b.tp.Shutdown(context.Background())
		return nil, err
	}
	b.tracer = b.tp.Tracer("github.com/Lands-Horizon-Corp/ecoop-server")
	b.meter = b.mp.Meter("github.com/Lands-Horizon-Corp/ecoop-server")
	if err := b.instruments(); err != nil {
		_ = b.Shutdown(context.Background())
		return nil, err
	}
	return b, nil
}

func newResource(r observability.Resource) (*resource.Resource, error) {
	attrs := []attribute.KeyValue{semconv.ServiceName(orDefault(r.ServiceName, "ecoop-server"))}
	add := func(k attribute.Key, v string) {
		if v != "" {
			attrs = append(attrs, k.String(v))
		}
	}
	add(semconv.ServiceNamespaceKey, r.Namespace)
	add(semconv.ServiceVersionKey, r.Version)
	add(semconv.ServiceInstanceIDKey, r.InstanceID)
	add(semconv.DeploymentEnvironmentNameKey, r.Environment)
	add("vcs.revision", r.Revision)
	// WithFromEnv lets OTEL_RESOURCE_ATTRIBUTES and OTEL_SERVICE_NAME add to, or
	// override, what the code says, as the specification requires.
	return resource.New(context.Background(),
		resource.WithAttributes(attrs...),
		resource.WithFromEnv(),
		resource.WithProcessRuntimeName(),
		resource.WithProcessRuntimeVersion(),
		resource.WithHost(),
	)
}

func orDefault(v, d string) string {
	if v == "" {
		return d
	}
	return v
}

func (b *Backend) openTraces(o options, res *resource.Resource) error {
	exp := o.traceExp
	if exp == nil && !b.cfg.Disabled() {
		hopts := []otlptracehttp.Option{
			// WithEndpointURL posts to the URL's own path, so the signal path of the
			// OTLP/HTTP specification has to be added to the collector's base URL.
			otlptracehttp.WithEndpointURL(b.cfg.Endpoint + "/v1/traces"),
			otlptracehttp.WithTimeout(b.cfg.ExportTimeout),
			otlptracehttp.WithCompression(otlptracehttp.GzipCompression),
		}
		if b.cfg.Insecure {
			hopts = append(hopts, otlptracehttp.WithInsecure())
		}
		if len(b.cfg.Headers) > 0 {
			hopts = append(hopts, otlptracehttp.WithHeaders(b.cfg.Headers))
		}
		e, err := otlptracehttp.New(context.Background(), hopts...)
		if err != nil {
			return fmt.Errorf("otelobs: creating the span exporter: %w", err)
		}
		exp = e
	}
	sampler, err := b.sampler()
	if err != nil {
		return err
	}
	tpOpts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res), sdktrace.WithSampler(sampler)}
	if exp != nil {
		// A batcher, never a synchronous exporter: a request must not wait for the
		// collector. It is not blocking either, so a full queue drops spans (and
		// counts them) instead of slowing the application.
		tpOpts = append(tpOpts, sdktrace.WithBatcher(&countingExporter{SpanExporter: exp, dropped: b.dropped, q: &quietLog{log: b.log}},
			sdktrace.WithMaxQueueSize(b.cfg.MaxQueue),
			sdktrace.WithMaxExportBatchSize(b.cfg.MaxBatch),
			sdktrace.WithBatchTimeout(b.cfg.BatchTimeout),
			sdktrace.WithExportTimeout(b.cfg.ExportTimeout),
		))
	}
	b.tp = sdktrace.NewTracerProvider(tpOpts...)
	return nil
}

// sampler builds the sampler from the Config, so a typo is an error at startup.
func (b *Backend) sampler() (sdktrace.Sampler, error) {
	ratio := 1.0
	if b.cfg.SamplerArgSet {
		ratio = b.cfg.SamplerArg
	}
	switch b.cfg.Sampler {
	case "always_on":
		return sdktrace.AlwaysSample(), nil
	case "always_off":
		return sdktrace.NeverSample(), nil
	case "traceidratio":
		return sdktrace.TraceIDRatioBased(ratio), nil
	case "parentbased_always_off":
		return sdktrace.ParentBased(sdktrace.NeverSample()), nil
	case "parentbased_traceidratio":
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio)), nil
	case "parentbased_always_on", "":
		return sdktrace.ParentBased(sdktrace.AlwaysSample()), nil
	}
	return nil, fmt.Errorf("%w: %q", ErrInvalidSampler, b.cfg.Sampler)
}

func (b *Backend) openMetrics(o options, res *resource.Resource) error {
	reader := o.reader
	if reader == nil && !b.cfg.Disabled() {
		mopts := []otlpmetrichttp.Option{
			otlpmetrichttp.WithEndpointURL(b.cfg.Endpoint + "/v1/metrics"),
			otlpmetrichttp.WithTimeout(b.cfg.ExportTimeout),
			otlpmetrichttp.WithCompression(otlpmetrichttp.GzipCompression),
		}
		if b.cfg.Insecure {
			mopts = append(mopts, otlpmetrichttp.WithInsecure())
		}
		if len(b.cfg.Headers) > 0 {
			mopts = append(mopts, otlpmetrichttp.WithHeaders(b.cfg.Headers))
		}
		exp, err := otlpmetrichttp.New(context.Background(), mopts...)
		if err != nil {
			return fmt.Errorf("otelobs: creating the metric exporter: %w", err)
		}
		reader = sdkmetric.NewPeriodicReader(&quietMetricExporter{Exporter: exp, q: &quietLog{log: b.log}},
			sdkmetric.WithInterval(b.cfg.MetricInterval),
			sdkmetric.WithTimeout(b.cfg.ExportTimeout))
	}
	mpOpts := []sdkmetric.Option{
		sdkmetric.WithResource(res),
		// Explicit buckets for every duration: the default boundaries are made for
		// milliseconds and would put every operation in one bucket.
		sdkmetric.WithView(sdkmetric.NewView(
			sdkmetric.Instrument{Kind: sdkmetric.InstrumentKindHistogram},
			sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{Boundaries: DurationBuckets}},
		)),
	}
	if reader != nil {
		mpOpts = append(mpOpts, sdkmetric.WithReader(reader))
	}
	b.mp = sdkmetric.NewMeterProvider(mpOpts...)
	return nil
}

func (b *Backend) instruments() error {
	var err error
	if b.ops, err = b.meter.Int64Counter(observability.MetricOperations,
		metric.WithDescription("Operations finished, by component, operation and outcome")); err != nil {
		return err
	}
	if b.duration, err = b.meter.Float64Histogram(observability.MetricOperationDuration,
		metric.WithDescription("How long operations took"), metric.WithUnit("s")); err != nil {
		return err
	}
	if b.inflight, err = b.meter.Int64UpDownCounter(observability.MetricInflight,
		metric.WithDescription("Operations started and not yet ended")); err != nil {
		return err
	}
	return nil
}

// ---- observability.Backend ------------------------------------------------.

// StartSpan implements observability.Backend.
//
//nolint:spancheck // the returned span wraps sp, and its owner ends it through BackendSpan.End
func (b *Backend) StartSpan(ctx context.Context, component, operation string, attrs []observability.Attr) (context.Context, observability.BackendSpan) {
	ctx, sp := b.tracer.Start(ctx, component+" "+operation,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(append(kvs(attrs),
			attribute.String("ecoop.component", component),
			attribute.String("ecoop.operation", operation))...))
	return ctx, &span{sp: sp}
}

type span struct{ sp trace.Span }

func (s *span) SetAttributes(attrs []observability.Attr) { s.sp.SetAttributes(kvs(attrs)...) }

// End closes the span. A fault is an Error status and a recorded exception of
// its type only: the text of err can carry a row value and is never exported. A
// retry, a dead-lettered record and an expected answer (skipped) are not faults
// of the operation, so they leave the status unset and say what happened in an
// attribute.
func (s *span) End(outcome observability.Outcome, err error) {
	s.sp.SetAttributes(attribute.String("ecoop.outcome", string(outcome)))
	if outcome == observability.OutcomeError || (err != nil && outcome != observability.OutcomeSkipped && outcome != observability.OutcomeRetry && outcome != observability.OutcomeDLQ) {
		typ := "error"
		if err != nil {
			typ = fmt.Sprintf("%T", unwrapAll(err))
		}
		s.sp.SetStatus(codes.Error, "")
		s.sp.AddEvent("exception", trace.WithAttributes(attribute.String("exception.type", typ)))
	}
	s.sp.End()
}

func unwrapAll(err error) error {
	for {
		u := errors.Unwrap(err)
		if u == nil {
			return err
		}
		err = u
	}
}

// RecordOperation implements observability.Backend.
func (b *Backend) RecordOperation(ctx context.Context, labels map[string]string, outcome observability.Outcome, seconds float64) {
	set := metric.WithAttributes(attrSet(labels)...)
	b.ops.Add(ctx, 1, set)
	b.duration.Record(ctx, seconds, set)
}

// Inflight implements observability.Backend.
func (b *Backend) Inflight(ctx context.Context, labels map[string]string, delta int64) {
	b.inflight.Add(ctx, delta, metric.WithAttributes(attrSet(labels)...))
}

// Add implements observability.Backend.
func (b *Backend) Add(ctx context.Context, name string, labels map[string]string, delta int64) {
	b.mu.Lock()
	c, ok := b.counts[name]
	if !ok {
		var err error
		if c, err = b.meter.Int64Counter(name); err != nil {
			b.mu.Unlock()
			return
		}
		b.counts[name] = c
	}
	b.mu.Unlock()
	c.Add(ctx, delta, metric.WithAttributes(attrSet(labels)...))
}

// Record implements observability.Backend.
func (b *Backend) Record(ctx context.Context, name string, labels map[string]string, value float64) {
	b.mu.Lock()
	h, ok := b.hists[name]
	if !ok {
		var err error
		if h, err = b.meter.Float64Histogram(name, metric.WithUnit("s")); err != nil {
			b.mu.Unlock()
			return
		}
		b.hists[name] = h
	}
	b.mu.Unlock()
	h.Record(ctx, value, metric.WithAttributes(attrSet(labels)...))
}

// Set implements observability.Backend. A gauge is observed, not added to, so
// the exported value is the last one set.
func (b *Backend) Set(_ context.Context, name string, labels map[string]string, value float64) {
	b.mu.Lock()
	g, ok := b.gauges[name]
	if !ok {
		g = &gauge{vals: map[string]gaugeVal{}}
		inst, err := b.meter.Float64ObservableGauge(name, metric.WithFloat64Callback(g.observe))
		if err != nil {
			b.mu.Unlock()
			return
		}
		g.inst = inst
		b.gauges[name] = g
	}
	b.mu.Unlock()
	g.set(labels, value)
}

// Inject implements observability.Propagator.
func (b *Backend) Inject(ctx context.Context, c observability.Carrier) {
	b.prop.Inject(ctx, carrier{c})
}

// Extract implements observability.Propagator.
func (b *Backend) Extract(ctx context.Context, c observability.Carrier) context.Context {
	return b.prop.Extract(ctx, carrier{c})
}

// SpanContext implements observability.Backend.
func (b *Backend) SpanContext(ctx context.Context) (string, string, bool) {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return "", "", false
	}
	return sc.TraceID().String(), sc.SpanID().String(), sc.IsSampled()
}

// Dropped is how many spans the exporter failed to deliver and gave up on, and
// how many the queue refused because it was full.
func (b *Backend) Dropped() int64 { return b.dropped.total() }

// Shutdown implements observability.Backend: it flushes spans and metrics and
// stops the exporters. It is bounded by ctx and by Config.ShutdownTimeout,
// whichever ends first. The cap applies even when ctx allows longer: a process
// stops many things in one budget, and a dead collector must not spend all of it.
func (b *Backend) Shutdown(ctx context.Context) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	b.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, b.cfg.ShutdownTimeout)
	defer cancel()
	return errors.Join(b.tp.Shutdown(ctx), b.mp.Shutdown(ctx))
}

// ForceFlush exports what is buffered now, without stopping.
func (b *Backend) ForceFlush(ctx context.Context) error {
	return errors.Join(b.tp.ForceFlush(ctx), b.mp.ForceFlush(ctx))
}

// ---- helpers --------------------------------------------------------------.

// kvs converts scrubbed attributes. The Service has already removed secrets and
// cut long values, so this only maps types.
func kvs(attrs []observability.Attr) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(attrs))
	for _, a := range attrs {
		switch a.Value.Kind() {
		case slog.KindString:
			out = append(out, attribute.String(a.Key, a.Value.String()))
		case slog.KindInt64:
			out = append(out, attribute.Int64(a.Key, a.Value.Int64()))
		case slog.KindUint64:
			out = append(out, attribute.Int64(a.Key, int64(a.Value.Uint64()&0x7fffffffffffffff)))
		case slog.KindFloat64:
			out = append(out, attribute.Float64(a.Key, a.Value.Float64()))
		case slog.KindBool:
			out = append(out, attribute.Bool(a.Key, a.Value.Bool()))
		case slog.KindDuration:
			out = append(out, attribute.Float64(a.Key, a.Value.Duration().Seconds()))
		default:
			out = append(out, attribute.String(a.Key, a.Value.String()))
		}
	}
	return out
}

func attrSet(labels map[string]string) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(labels))
	for k, v := range labels {
		out = append(out, attribute.String(k, v))
	}
	return out
}

// carrier adapts an observability.Carrier to the OpenTelemetry TextMapCarrier.
type carrier struct{ c observability.Carrier }

func (c carrier) Get(k string) string { return c.c.Get(k) }
func (c carrier) Set(k, v string)     { c.c.Set(k, v) }
func (c carrier) Keys() []string      { return nil }

// gauge keeps the last value per label set for an observable gauge.
type gauge struct {
	mu   sync.Mutex
	inst metric.Float64ObservableGauge
	vals map[string]gaugeVal
}

type gaugeVal struct {
	attrs []attribute.KeyValue
	v     float64
}

func (g *gauge) set(labels map[string]string, v float64) {
	a := attrSet(labels)
	set := attribute.NewSet(a...)
	g.mu.Lock()
	g.vals[set.Encoded(attribute.DefaultEncoder())] = gaugeVal{attrs: a, v: v}
	g.mu.Unlock()
}

func (g *gauge) observe(_ context.Context, o metric.Float64Observer) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, gv := range g.vals {
		o.Observe(gv.v, metric.WithAttributes(gv.attrs...))
	}
	return nil
}

// ---- counting exporter ------------------------------------------------------.

// dropCounter counts spans that were not delivered.
type dropCounter struct {
	mu sync.Mutex
	n  int64
}

func (d *dropCounter) add(n int) {
	d.mu.Lock()
	d.n += int64(n)
	d.mu.Unlock()
}
func (d *dropCounter) total() int64 { d.mu.Lock(); defer d.mu.Unlock(); return d.n }

// quietLog logs a failure at most once a minute, so a dead collector shows up in
// the logs without filling them.
type quietLog struct {
	log *slog.Logger

	mu      sync.Mutex
	lastLog time.Time
}

func (q *quietLog) warn(msg string, args ...any) {
	q.mu.Lock()
	quiet := time.Since(q.lastLog) < time.Minute
	if !quiet {
		q.lastLog = time.Now()
	}
	q.mu.Unlock()
	if !quiet {
		q.log.Warn(msg, args...)
	}
}

// countingExporter counts what the wrapped exporter failed to deliver and
// handles the failure itself. It returns nil on purpose: the SDK passes an
// error from an exporter to a process-wide error handler that prints one line
// per failure, which is how a dead collector floods a log and how a global gets
// into a package that has none. The exporter has already retried by then, so
// the spans are dropped either way; here the drop is counted and logged once a
// minute.
type countingExporter struct {
	sdktrace.SpanExporter
	dropped *dropCounter
	q       *quietLog
}

func (e *countingExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if err := e.SpanExporter.ExportSpans(ctx, spans); err != nil {
		e.dropped.add(len(spans))
		// The error of an exporter names the endpoint and the HTTP status, never a
		// span's content.
		e.q.warn("telemetry export failed; spans are dropped", "spans", len(spans), "error", err.Error())
	}
	return nil
}

// quietMetricExporter is the same for metrics: a failed export is logged once a
// minute and not raised.
type quietMetricExporter struct {
	sdkmetric.Exporter
	q *quietLog
}

func (e *quietMetricExporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	if err := e.Exporter.Export(ctx, rm); err != nil {
		e.q.warn("telemetry export failed; metrics are dropped", "error", err.Error())
	}
	return nil
}
