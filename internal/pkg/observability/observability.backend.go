package observability

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// The observability package never imports a telemetry library. An exporter
// (internal/pkg/observability/otelobs) plugs in here, through a Backend, so the
// in-memory registry, its limits and its safety rules apply whatever sits
// behind it, and a Backend can be replaced or faked without touching a package.

// Carrier is the headers of a message (an HTTP request, a Kafka record) that
// trace context travels in.
type Carrier interface {
	Get(key string) string
	Set(key, value string)
}

// MapCarrier is a Carrier over a map, for tests and simple transports.
type MapCarrier map[string]string

func (m MapCarrier) Get(key string) string { return m[key] }
func (m MapCarrier) Set(key, value string) { m[key] = value }

// Propagator moves trace context in and out of a Carrier. *Service implements
// it when it has a Backend.
type Propagator interface {
	Inject(ctx context.Context, c Carrier)
	Extract(ctx context.Context, c Carrier) context.Context
}

// Inject writes ctx's trace context into c. It does nothing when o does not
// propagate (Nop, or a Service with no Backend).
func Inject(ctx context.Context, o IObserver, c Carrier) {
	if p, ok := o.(Propagator); ok && c != nil && ctx != nil {
		p.Inject(ctx, c)
	}
}

// Extract returns ctx carrying the trace context found in c, or ctx unchanged.
// Only extract from a source that is trusted to start the trace: a message from
// another of our services, never a request from the internet.
func Extract(ctx context.Context, o IObserver, c Carrier) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if p, ok := o.(Propagator); ok && c != nil {
		return p.Extract(ctx, c)
	}
	return ctx
}

// BackendSpan is one span of a Backend.
type BackendSpan interface {
	// SetAttributes adds attributes. They are already scrubbed.
	SetAttributes(attrs []Attr)
	// End closes the span. err is for its type only: never record its text, which
	// may carry row values.
	End(outcome Outcome, err error)
}

// Backend receives everything the Service records, to export it. Every method
// may be called from any goroutine. A Backend must not block for long, and a
// panic in it is recovered by the Service, so telemetry cannot take a caller
// down.
type Backend interface {
	// StartSpan starts the span of an operation and returns a context carrying
	// it, so a span started from that context is its child.
	StartSpan(ctx context.Context, component, operation string, attrs []Attr) (context.Context, BackendSpan)
	// RecordOperation adds one to operations_total and the duration to
	// operation_duration_seconds, with labels (component, operation, outcome...).
	RecordOperation(ctx context.Context, labels map[string]string, outcome Outcome, seconds float64)
	// Inflight raises or lowers inflight_operations.
	Inflight(ctx context.Context, labels map[string]string, delta int64)
	// The component's own instruments (cqrs.Stats and the like).
	Add(ctx context.Context, name string, labels map[string]string, delta int64)
	Set(ctx context.Context, name string, labels map[string]string, value float64)
	Record(ctx context.Context, name string, labels map[string]string, value float64)
	Propagator
	// SpanContext is the trace and span id of the span in ctx, and whether the
	// trace is sampled. Both ids are empty when ctx has no valid span.
	SpanContext(ctx context.Context) (traceID, spanID string, sampled bool)
	// Shutdown flushes what is buffered and releases the exporters, within ctx.
	Shutdown(ctx context.Context) error
}

// Logger is the component's logger for work done under ctx: the same logger
// o.Logger gives, plus trace_id and span_id when ctx carries a span, so a log
// line can be found from its trace and the trace from its log line. Create it
// where the work starts (after Begin) and pass it down; do not store it.
func Logger(ctx context.Context, o IObserver, component Component) *slog.Logger {
	o = Pick(o)
	l := o.Logger(component)
	if ctx == nil {
		return l
	}
	if s, ok := o.(*Service); ok && s != nil && s.backend != nil {
		var traceID, spanID string
		s.guard(func() { traceID, spanID, _ = s.backend.SpanContext(ctx) })
		if traceID != "" {
			return l.With("trace_id", traceID, "span_id", spanID)
		}
	}
	return l
}

// ServiceOption configures a Service built by New.
type ServiceOption func(*Service)

// WithBackend makes the Service export through b. A nil b is ignored.
func WithBackend(b Backend) ServiceOption {
	return func(s *Service) {
		if !utils.IsNil(b) {
			s.backend = b
		}
	}
}

// guard runs fn and swallows a panic: a Backend that panics must never reach
// the caller of Begin, End or an instrument.
func (s *Service) guard(fn func()) {
	defer func() { _ = recover() }()
	fn()
}

// Inject implements Propagator.
func (s *Service) Inject(ctx context.Context, c Carrier) {
	if s == nil || s.backend == nil || c == nil || ctx == nil {
		return
	}
	s.guard(func() { s.backend.Inject(ctx, c) })
}

// Extract implements Propagator.
func (s *Service) Extract(ctx context.Context, c Carrier) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if s == nil || s.backend == nil || c == nil {
		return ctx
	}
	out := ctx
	s.guard(func() {
		if got := s.backend.Extract(ctx, c); got != nil {
			out = got
		}
	})
	return out
}

// backendTracer starts spans through the Backend.
type backendTracer struct {
	s         *Service
	component Component
}

func (t backendTracer) Start(ctx context.Context, name string, attrs ...Attr) (context.Context, Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	var sp BackendSpan
	out := ctx
	t.s.guard(func() {
		c, got := t.s.backend.StartSpan(ctx, cleanLabel(string(t.component)), cleanLabel(name), scrubAttrs(attrs))
		if c != nil {
			out = c
		}
		sp = got
	})
	if sp == nil {
		return ctx, nopSpan{}
	}
	return out, &spanAdapter{s: t.s, sp: sp}
}

// spanAdapter is a Span over a BackendSpan: the error it is given decides the
// outcome it ends with.
type spanAdapter struct {
	s     *Service
	sp    BackendSpan
	mu    sync.Mutex
	err   error
	ended atomic.Bool
}

func (a *spanAdapter) SetAttributes(attrs ...Attr) {
	a.s.guard(func() { a.sp.SetAttributes(scrubAttrs(attrs)) })
}

func (a *spanAdapter) RecordError(err error) {
	if err == nil {
		return
	}
	a.mu.Lock()
	if a.err == nil {
		a.err = err
	}
	a.mu.Unlock()
}

func (a *spanAdapter) End() {
	if a.ended.Swap(true) {
		return
	}
	a.mu.Lock()
	err := a.err
	a.mu.Unlock()
	outcome := OutcomeSuccess
	if err != nil {
		outcome = OutcomeError
	}
	a.s.guard(func() { a.sp.End(outcome, err) })
}

// ---- scrubbing ------------------------------------------------------------.

const (
	redacted     = "[redacted]"
	maxAttrValue = 256 // bytes of a string attribute
	maxAttrs     = 64  // attributes passed to a span at once
)

var (
	// sensitiveKey matches attribute names that carry a secret.
	sensitiveKey = regexp.MustCompile(`(?i)(pass(word|wd)?|secret|token|authorization|cookie|session|credential|api[_.-]?key|private|otp|csrf|pin$|ssn|card[_.-]?number|cvv)`)
	// sensitiveValue matches values that look like a credential or personal data
	// whatever their key is called.
	sensitiveValue = regexp.MustCompile(`(?i)(eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.|bearer\s+[A-Za-z0-9._~+/=-]{8,}|[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}|\b\d{13,19}\b)`)
	// sqlKey names the attributes that carry a SQL statement.
	sqlKey    = regexp.MustCompile(`(?i)^db\.(query\.text|statement)$`)
	sqlString = regexp.MustCompile(`'(?:[^']|'')*'`)
	// sqlNumber matches a numeric literal but not the number of a $1 placeholder,
	// which the preceding character rules out.
	sqlNumber = regexp.MustCompile(`(^|[^$\w.])\d+(?:\.\d+)?\b`)
)

// ObfuscateSQL replaces the literals of a statement with ?, so a span can say
// what ran without saying who it ran for. Placeholders ($1, ?) are kept.
func ObfuscateSQL(q string) string {
	q = sqlString.ReplaceAllString(q, "?")
	return sqlNumber.ReplaceAllString(q, "${1}?")
}

// scrubAttrs is what a span may carry: at most maxAttrs attributes, secrets and
// personal data replaced, SQL literals removed, strings cut. Metrics need none
// of it, because only the bounded label keys ever reach them.
func scrubAttrs(attrs []Attr) []Attr {
	if len(attrs) == 0 {
		return nil
	}
	if len(attrs) > maxAttrs {
		attrs = attrs[:maxAttrs]
	}
	out := make([]Attr, 0, len(attrs))
	for _, a := range attrs {
		if a.Key == "" {
			continue
		}
		out = append(out, scrubAttr(a))
	}
	return out
}

func scrubAttr(a Attr) Attr {
	if sensitiveKey.MatchString(a.Key) {
		return slog.String(a.Key, redacted)
	}
	switch a.Value.Kind() {
	case slog.KindString:
		v := a.Value.String()
		if sqlKey.MatchString(a.Key) {
			v = ObfuscateSQL(v)
		}
		if sensitiveValue.MatchString(v) {
			return slog.String(a.Key, redacted)
		}
		return slog.String(a.Key, cutString(v, maxAttrValue))
	case slog.KindInt64, slog.KindUint64, slog.KindFloat64, slog.KindBool, slog.KindDuration:
		return a
	case slog.KindTime:
		return slog.String(a.Key, a.Value.Time().UTC().Format("2006-01-02T15:04:05.000Z07:00"))
	default:
		// A struct, a map, an error: record what it is, never what it holds.
		return slog.String(a.Key, fmt.Sprintf("%T", a.Value.Any()))
	}
}

func cutString(v string, limit int) string {
	if len(v) <= limit {
		return strings.ToValidUTF8(v, "")
	}
	return strings.ToValidUTF8(cleanCut(v, limit), "")
}

func cleanCut(v string, limit int) string {
	for limit > 0 && limit < len(v) && !isRuneStart(v[limit]) {
		limit--
	}
	return v[:limit]
}

func isRuneStart(b byte) bool { return b&0xC0 != 0x80 }

// ---- log level ------------------------------------------------------------.

// ErrInvalidLogLevel is returned by ParseLevel for a name it does not know.
var ErrInvalidLogLevel = errors.New("observability: invalid log level")

// ParseLevel reads a LOG_LEVEL value: debug, info, warn, warning or error, in
// any case. An empty value is info. Anything else is an error, so a typo fails
// at startup instead of silently logging at the wrong level.
func ParseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	}
	return slog.LevelInfo, fmt.Errorf("%w: %q (want debug, info, warn or error)", ErrInvalidLogLevel, cutString(s, 32))
}
