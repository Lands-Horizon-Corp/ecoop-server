package observability

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Nop is an observer that does nothing. It is what a package uses when it was
// not given one, so a package never has to check for nil before it observes.
func Nop() IObserver { return nopObserver{} }

// Pick is the first observer that is set, or Nop. Constructors use it on their
// optional observer argument.
func Pick(obs ...IObserver) IObserver {
	for _, o := range obs {
		if !utils.IsNil(o) {
			return o
		}
	}
	return Nop()
}

// IsNop reports whether o does nothing, for a package that would otherwise
// replace a logger it was configured with.
func IsNop(o IObserver) bool {
	if utils.IsNil(o) {
		return true
	}
	_, ok := o.(nopObserver)
	return ok
}

var knownOutcomes = []Outcome{OutcomeSuccess, OutcomeError, OutcomeRetry, OutcomeDLQ, OutcomeSkipped}

// Kind is what a Sample measures.
type Kind string

const (
	KindCounter   Kind = "counter"
	KindGauge     Kind = "gauge"
	KindHistogram Kind = "histogram"
)

// Sample is one series of the in-memory registry: a name, its attributes, and
// what was recorded.
type Sample struct {
	Name  string
	Kind  Kind
	Attrs map[string]string
	Value float64 // a counter's total, or a gauge's value
	Count uint64  // histogram: observations
	Sum   float64 // histogram: their total
}

// labelKeys are the only attributes that become part of a metric series. The
// rest of what a caller passes goes to the span and is dropped from metrics, so
// a row key or a request ID can never create a series, however it is passed.
var labelKeys = map[string]struct{}{
	KeyComponent: {}, KeyOperation: {}, KeyOutcome: {}, KeyTopic: {},
	KeyConsumerGroup: {}, KeyTable: {}, KeyCacheFamily: {},
}

// Service is an observer that keeps its metrics in memory and logs through
// slog. It is the implementation behind IObservabilityService until an exporter
// exists: Snapshot reads what was recorded, which is what a test, a health
// endpoint or a future exporter needs. Spans are not recorded yet.
type Service struct {
	res     Resource
	log     *slog.Logger
	backend Backend // optional: spans and exported metrics; nil keeps the registry only

	mu         sync.Mutex
	series     map[string]*series
	started    atomic.Bool
	overflowed atomic.Uint64
}

type series struct {
	name  string
	kind  Kind
	attrs map[string]string

	mu    sync.Mutex
	value float64
	count uint64
	sum   float64
}

// New creates the service. logger may be nil (slog.Default is used). Without
// WithBackend nothing is exported: the registry is kept for tests and local
// inspection, and Start and Stop only log. With one, spans and metrics are
// exported through it, and Stop flushes it.
func New(res Resource, logger *slog.Logger, opts ...ServiceOption) IObservabilityService {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Service{
		res: res,
		log: logger.With(
			"service.name", res.ServiceName,
			"service.namespace", res.Namespace,
			"service.version", res.Version,
			"deployment.environment.name", res.Environment,
		),
		series: map[string]*series{},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	return s
}

// Start implements IObservabilityService.
func (s *Service) Start(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	if s == nil || s.started.Swap(true) {
		return nil
	}
	s.info("observability started")
	return nil
}

// info logs without ever failing the caller: a broken log handler must not
// stop the process from starting or stopping.
func (s *Service) info(msg string) {
	if s == nil || s.log == nil {
		return
	}
	defer func() { _ = recover() }()
	s.log.Info(msg)
}

// Overflowed is how many records were folded into an overflow series because
// the registry already held MaxSeries series. A non-zero value means a label
// that should be bounded is not.
func (s *Service) Overflowed() uint64 {
	if s == nil {
		return 0
	}
	return s.overflowed.Load()
}

// Stop implements IObservabilityService.
func (s *Service) Stop(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	if s == nil || !s.started.Swap(false) {
		return nil
	}
	// Flush what is buffered before saying so: a process that exits right after
	// Stop must not lose its last spans and metrics.
	var err error
	if s.backend != nil {
		s.guard(func() { err = s.backend.Shutdown(ctx) })
	}
	s.info("observability stopped")
	return err
}

// Resource is the process the service reports for.
func (s *Service) Resource() Resource {
	if s == nil {
		return Resource{}
	}
	return s.res
}

// Logger implements IObserver.
func (s *Service) Logger(component Component) *slog.Logger {
	if s == nil || s.log == nil {
		return slog.New(slog.DiscardHandler)
	}
	return s.log.With(KeyComponent, cleanLabel(string(component)))
}

// Meter implements IObserver.
func (s *Service) Meter(component Component) Meter {
	if s == nil {
		return nopMeter{}
	}
	return meter{s: s, component: component}
}

// Tracer implements IObserver. Spans are recorded when the Service has a
// Backend; without one they are no-ops.
func (s *Service) Tracer(component Component) Tracer {
	if s == nil || s.backend == nil {
		return nopTracer{}
	}
	return backendTracer{s: s, component: component}
}

// Begin implements IObserver.
func (s *Service) Begin(ctx context.Context, component Component, operation string, attrs ...Attr) (context.Context, Operation) {
	if ctx == nil {
		ctx = context.Background()
	}
	if s == nil {
		return ctx, nopOperation{}
	}
	op := &activeOperation{
		s:         s,
		component: component,
		name:      operation,
		attrs:     slices.Clone(attrs),
		started:   time.Now(),
	}
	// The in-flight gauge is raised on these labels and must be lowered on the
	// same ones, whatever attributes the operation gains before it ends.
	op.began = op.labels()
	s.add(MetricInflight, KindGauge, maps.Clone(op.began), 1)
	if s.backend != nil {
		s.guard(func() {
			c, sp := s.backend.StartSpan(ctx, op.began[KeyComponent], op.began[KeyOperation], scrubAttrs(attrs))
			if c != nil {
				ctx = c
			}
			op.span = sp
			s.backend.Inflight(ctx, maps.Clone(op.began), 1)
		})
	}
	op.ctx = ctx
	return ctx, op
}

// Snapshot is every series recorded so far, ordered by name and attributes.
func (s *Service) Snapshot() []Sample {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	all := make([]*series, 0, len(s.series))
	for _, sr := range s.series {
		all = append(all, sr)
	}
	s.mu.Unlock()
	out := make([]Sample, 0, len(all))
	for _, sr := range all {
		sr.mu.Lock()
		out = append(out, Sample{Name: sr.name, Kind: sr.kind, Attrs: sr.attrs, Value: sr.value, Count: sr.count, Sum: sr.sum})
		sr.mu.Unlock()
	}
	slices.SortFunc(out, func(a, b Sample) int {
		if c := strings.Compare(a.Name, b.Name); c != 0 {
			return c
		}
		return strings.Compare(attrKey(a.Attrs), attrKey(b.Attrs))
	})
	return out
}

// Find is the series with this name whose attributes include want, or false.
func (s *Service) Find(name string, want map[string]string) (Sample, bool) {
	for _, sm := range s.Snapshot() {
		if sm.Name != name {
			continue
		}
		match := true
		for k, v := range want {
			if sm.Attrs[k] != v {
				match = false
				break
			}
		}
		if match {
			return sm, true
		}
	}
	return Sample{}, false
}

// series finds or creates the series for name and attrs.
//
// The registry holds at most MaxSeries series. A label that was meant to be
// bounded but is not (an identifier that slipped in) would otherwise grow it
// without limit, in memory here and in the metrics backend later. Past the
// cap, new series are folded into one overflow series per component, so no
// count is lost and the drop is visible (KeyOverflow, Overflowed).
func (s *Service) get(name string, kind Kind, attrs map[string]string) *series {
	key := name + "|" + attrKey(attrs)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.series == nil {
		s.series = map[string]*series{}
	}
	if sr, ok := s.series[key]; ok {
		return sr
	}
	if len(s.series) >= MaxSeries {
		s.overflowed.Add(1)
		attrs = map[string]string{KeyComponent: attrs[KeyComponent], KeyOverflow: "true"}
		key = name + "|" + attrKey(attrs)
		if sr, ok := s.series[key]; ok {
			return sr
		}
	}
	sr := &series{name: name, kind: kind, attrs: attrs}
	s.series[key] = sr
	return sr
}

func (s *Service) add(name string, kind Kind, attrs map[string]string, delta float64) {
	sr := s.get(name, kind, attrs)
	sr.mu.Lock()
	sr.value += delta
	sr.mu.Unlock()
}

func (s *Service) set(name string, attrs map[string]string, v float64) {
	sr := s.get(name, KindGauge, attrs)
	sr.mu.Lock()
	sr.value = v
	sr.mu.Unlock()
}

func (s *Service) record(name string, attrs map[string]string, v float64) {
	sr := s.get(name, KindHistogram, attrs)
	sr.mu.Lock()
	sr.count++
	sr.sum += v
	sr.mu.Unlock()
}

// labelsOf keeps the attributes allowed as labels, with the component first.
func labelsOf(component Component, attrs []Attr) map[string]string {
	out := map[string]string{KeyComponent: cleanLabel(string(component))}
	for _, a := range attrs {
		if _, ok := labelKeys[a.Key]; ok {
			out[a.Key] = cleanLabel(a.Value.String())
		}
	}
	return out
}

// cleanLabel makes a value safe as a label: valid UTF-8, no control
// characters (a newline or NUL would break a text exposition format or a log
// line), and at most MaxLabelValueLen bytes, cut on a character boundary.
func cleanLabel(v string) string {
	if len(v) <= MaxLabelValueLen && utf8.ValidString(v) && !strings.ContainsFunc(v, unicode.IsControl) {
		return v
	}
	var b strings.Builder
	for _, r := range v {
		if r == utf8.RuneError || unicode.IsControl(r) {
			continue
		}
		if b.Len()+utf8.RuneLen(r) > MaxLabelValueLen {
			break
		}
		b.WriteRune(r)
	}
	return b.String()
}

// attrKey is the identity of a label set. Values are quoted, so a value that
// looks like another label ("x,topic=y") cannot collide with a real one.
func attrKey(attrs map[string]string) string {
	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "%s=%q,", k, attrs[k])
	}
	return b.String()
}

// activeOperation is one Operation of the service.
type activeOperation struct {
	s         *Service
	component Component
	name      string
	started   time.Time
	outcome   atomic.Pointer[Outcome]
	ended     atomic.Bool
	began     map[string]string // the labels the in-flight gauge was raised on; read only
	span      BackendSpan       // nil without a Backend; read only after Begin
	ctx       context.Context   // the context Begin returned, for the Backend's metrics

	mu    sync.Mutex // guards attrs: SetAttributes may be called from other goroutines
	attrs []Attr
}

func (o *activeOperation) labels() map[string]string {
	o.mu.Lock()
	l := labelsOf(o.component, o.attrs)
	o.mu.Unlock()
	l[KeyOperation] = cleanLabel(o.name)
	return l
}

func (o *activeOperation) SetAttributes(attrs ...Attr) {
	o.mu.Lock()
	o.attrs = append(o.attrs, attrs...)
	o.mu.Unlock()
	if o.span != nil {
		o.s.guard(func() { o.span.SetAttributes(scrubAttrs(attrs)) })
	}
}

func (o *activeOperation) SetOutcome(outcome Outcome) { o.outcome.Store(&outcome) }

func (o *activeOperation) End(err error) {
	if o.ended.Swap(true) {
		return
	}
	outcome := OutcomeSuccess
	if err != nil {
		outcome = OutcomeError
	}
	if set := o.outcome.Load(); set != nil {
		outcome = *set
	}
	// Outcome is a bounded label. A value outside the set cannot create a
	// series, and it must not hide a failure either: it counts as an error.
	if !slices.Contains(knownOutcomes, outcome) {
		outcome = OutcomeError
	}
	o.s.add(MetricInflight, KindGauge, maps.Clone(o.began), -1)
	l := o.labels()
	l[KeyOutcome] = string(outcome)
	o.s.add(MetricOperations, KindCounter, l, 1)
	seconds := time.Since(o.started).Seconds()
	o.s.record(MetricOperationDuration, l, seconds)
	if o.s.backend != nil {
		ctx := o.ctx
		o.s.guard(func() {
			o.s.backend.Inflight(ctx, maps.Clone(o.began), -1)
			o.s.backend.RecordOperation(ctx, maps.Clone(l), outcome, seconds)
		})
		if o.span != nil {
			o.s.guard(func() { o.span.End(outcome, err) })
		}
	}
}

// meter creates instruments that add to the service's registry, each carrying
// the component attribute.
type meter struct {
	s         *Service
	component Component
}

func (m meter) Counter(name, _ string) Counter {
	return instrument{m: m, name: name, kind: KindCounter}
}
func (m meter) Gauge(name, _ string) Gauge { return instrument{m: m, name: name, kind: KindGauge} }
func (m meter) Histogram(name, _, _ string) Histogram {
	return instrument{m: m, name: name, kind: KindHistogram}
}

type instrument struct {
	m    meter
	name string
	kind Kind
}

// Add ignores a negative delta: a counter only goes up.
func (i instrument) Add(ctx context.Context, delta int64, attrs ...Attr) {
	if delta < 0 {
		return
	}
	labels := labelsOf(i.m.component, attrs)
	i.m.s.add(i.name, KindCounter, labels, float64(delta))
	if b := i.m.s.backend; b != nil {
		i.m.s.guard(func() { b.Add(ctx, i.name, maps.Clone(labels), delta) })
	}
}

// Set ignores NaN and infinity, so one bad reading cannot replace a good one.
func (i instrument) Set(ctx context.Context, v float64, attrs ...Attr) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return
	}
	labels := labelsOf(i.m.component, attrs)
	i.m.s.set(i.name, labels, v)
	if b := i.m.s.backend; b != nil {
		i.m.s.guard(func() { b.Set(ctx, i.name, maps.Clone(labels), v) })
	}
}

// Record ignores NaN, infinity and negative values: one of them would poison
// the sum for the life of the process.
func (i instrument) Record(ctx context.Context, v float64, attrs ...Attr) {
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
		return
	}
	labels := labelsOf(i.m.component, attrs)
	i.m.s.record(i.name, labels, v)
	if b := i.m.s.backend; b != nil {
		i.m.s.guard(func() { b.Record(ctx, i.name, maps.Clone(labels), v) })
	}
}

// nopObserver does nothing; every call is cheap and safe.
type nopObserver struct{}

func (nopObserver) Logger(Component) *slog.Logger { return slog.New(slog.DiscardHandler) }
func (nopObserver) Meter(Component) Meter         { return nopMeter{} }
func (nopObserver) Tracer(Component) Tracer       { return nopTracer{} }
func (nopObserver) Begin(ctx context.Context, _ Component, _ string, _ ...Attr) (context.Context, Operation) {
	return ctx, nopOperation{}
}

type nopMeter struct{}

func (nopMeter) Counter(string, string) Counter             { return nopInstrument{} }
func (nopMeter) Gauge(string, string) Gauge                 { return nopInstrument{} }
func (nopMeter) Histogram(string, string, string) Histogram { return nopInstrument{} }

type nopInstrument struct{}

func (nopInstrument) Add(context.Context, int64, ...Attr)      {}
func (nopInstrument) Set(context.Context, float64, ...Attr)    {}
func (nopInstrument) Record(context.Context, float64, ...Attr) {}

type nopTracer struct{}

func (nopTracer) Start(ctx context.Context, _ string, _ ...Attr) (context.Context, Span) {
	return ctx, nopSpan{}
}

type nopSpan struct{}

func (nopSpan) SetAttributes(...Attr) {}
func (nopSpan) RecordError(error)     {}
func (nopSpan) End()                  {}

type nopOperation struct{}

func (nopOperation) SetAttributes(...Attr) {}
func (nopOperation) SetOutcome(Outcome)    {}
func (nopOperation) End(error)             {}
