package observability_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"

	obs "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/tests/internal/obstest"
)

// No build tag: the observer needs no infrastructure.

func newService(t testing.TB) *obs.Service { return obstest.New(t) }

func find(t testing.TB, s *obs.Service, name string, want map[string]string) obs.Sample {
	t.Helper()
	sm, ok := s.Find(name, want)
	if !ok {
		t.Fatalf("no series %s %v in %+v", name, want, s.Snapshot())
	}
	return sm
}

func TestOperation_SuccessAndError(t *testing.T) {
	t.Parallel()
	s := newService(t)
	for range 3 {
		_, op := s.Begin(context.Background(), obs.ComponentCache, "get")
		op.End(nil)
	}
	_, op := s.Begin(context.Background(), obs.ComponentCache, "get")
	op.End(errors.New("boom"))

	ok := find(t, s, obs.MetricOperations, map[string]string{obs.KeyComponent: "cache", obs.KeyOperation: "get", obs.KeyOutcome: "success"})
	if ok.Value != 3 {
		t.Fatalf("success count = %v, want 3", ok.Value)
	}
	bad := find(t, s, obs.MetricOperations, map[string]string{obs.KeyOutcome: "error"})
	if bad.Value != 1 {
		t.Fatalf("error count = %v, want 1", bad.Value)
	}
	d := find(t, s, obs.MetricOperationDuration, map[string]string{obs.KeyOutcome: "success"})
	if d.Count != 3 || d.Sum < 0 {
		t.Fatalf("duration = %+v, want 3 observations", d)
	}
}

func TestOperation_SetOutcomeOverrides(t *testing.T) {
	t.Parallel()
	s := newService(t)
	for outcome, err := range map[obs.Outcome]error{
		obs.OutcomeSkipped: nil,
		obs.OutcomeDLQ:     errors.New("parked"),
		obs.OutcomeRetry:   errors.New("again"),
	} {
		_, op := s.Begin(context.Background(), obs.ComponentCDC, "deliver")
		op.SetOutcome(outcome)
		op.End(err)
		if got := find(t, s, obs.MetricOperations, map[string]string{obs.KeyOutcome: string(outcome)}); got.Value != 1 {
			t.Fatalf("outcome %s counted %v, want 1", outcome, got.Value)
		}
	}
}

func TestOperation_EndTwiceCountsOnce(t *testing.T) {
	t.Parallel()
	s := newService(t)
	_, op := s.Begin(context.Background(), obs.ComponentSQL, "ping")
	op.End(nil)
	op.End(errors.New("late"))
	if got := find(t, s, obs.MetricOperations, map[string]string{obs.KeyOperation: "ping"}); got.Value != 1 {
		t.Fatalf("counted %v times, want once", got.Value)
	}
	if _, ok := s.Find(obs.MetricOperations, map[string]string{obs.KeyOutcome: "error"}); ok {
		t.Fatal("the second End changed the outcome")
	}
}

func TestOperation_InflightReturnsToZero(t *testing.T) {
	t.Parallel()
	s := newService(t)
	ops := make([]obs.Operation, 5)
	for i := range ops {
		_, ops[i] = s.Begin(context.Background(), obs.ComponentQueue, "run")
	}
	if got := find(t, s, obs.MetricInflight, map[string]string{obs.KeyOperation: "run"}); got.Value != 5 {
		t.Fatalf("inflight = %v, want 5", got.Value)
	}
	for _, op := range ops {
		op.End(nil)
	}
	if got := find(t, s, obs.MetricInflight, map[string]string{obs.KeyOperation: "run"}); got.Value != 0 {
		t.Fatalf("inflight = %v after all ended, want 0", got.Value)
	}
}

// Only the bounded keys become labels, so a row key or a request ID passed as
// an attribute cannot create a series.
func TestLabels_OnlyBoundedKeysBecomeSeries(t *testing.T) {
	t.Parallel()
	s := newService(t)
	for i := range 50 {
		_, op := s.Begin(context.Background(), obs.ComponentPagination, "count",
			slog.String(obs.KeyTable, "accounts"),
			slog.String("request_id", fmt.Sprintf("req-%d", i)),
			slog.Int("member_id", i),
		)
		op.End(nil)
	}
	n := 0
	for _, sm := range s.Snapshot() {
		if sm.Name == obs.MetricOperations {
			n++
			if _, leaked := sm.Attrs["request_id"]; leaked {
				t.Fatalf("request_id became a label: %+v", sm)
			}
			if _, leaked := sm.Attrs["member_id"]; leaked {
				t.Fatalf("member_id became a label: %+v", sm)
			}
		}
	}
	if n != 1 {
		t.Fatalf("%d series for one operation, want 1", n)
	}
	if got := find(t, s, obs.MetricOperations, map[string]string{obs.KeyTable: "accounts"}); got.Value != 50 {
		t.Fatalf("count = %v, want 50", got.Value)
	}
}

func TestMeter_InstrumentsAddToOneSeries(t *testing.T) {
	t.Parallel()
	s := newService(t)
	m := s.Meter(obs.ComponentCQRS)
	a := m.Counter(obs.MetricCQRSBatches, "batches")
	b := m.Counter(obs.MetricCQRSBatches, "batches again")
	a.Add(context.Background(), 2)
	b.Add(context.Background(), 3)
	if got := find(t, s, obs.MetricCQRSBatches, map[string]string{obs.KeyComponent: "cqrs"}); got.Value != 5 {
		t.Fatalf("counter = %v, want 5", got.Value)
	}
	g := m.Gauge(obs.MetricCQRSAwaiting, "waiting")
	g.Set(context.Background(), 7)
	g.Set(context.Background(), 2)
	if got := find(t, s, obs.MetricCQRSAwaiting, nil); got.Value != 2 {
		t.Fatalf("gauge = %v, want the last value 2", got.Value)
	}
	h := m.Histogram(obs.MetricCQRSBatchDuration, "duration", "s")
	h.Record(context.Background(), 0.5)
	h.Record(context.Background(), 1.5)
	if got := find(t, s, obs.MetricCQRSBatchDuration, nil); got.Count != 2 || got.Sum != 2 {
		t.Fatalf("histogram = %+v, want 2 observations summing to 2", got)
	}
}

func TestLogger_CarriesTheComponent(t *testing.T) {
	t.Parallel()
	var got []slog.Attr
	h := captureHandler{attrs: &got}
	s := obs.New(obs.Resource{ServiceName: "x"}, slog.New(h))
	s.Logger(obs.ComponentSMTP).Info("hello")
	found := false
	for _, a := range got {
		if a.Key == obs.KeyComponent && a.Value.String() == "smtp" {
			found = true
		}
	}
	if !found {
		t.Fatalf("component attribute missing: %v", got)
	}
}

func TestLifecycle(t *testing.T) {
	t.Parallel()
	s := obs.New(obs.Resource{ServiceName: "x"}, nil)
	if err := s.Start(nil); !errors.Is(err, obs.ErrNilContext) {
		t.Fatalf("Start(nil) = %v, want ErrNilContext", err)
	}
	if err := s.Stop(nil); !errors.Is(err, obs.ErrNilContext) {
		t.Fatalf("Stop(nil) = %v, want ErrNilContext", err)
	}
	ctx := context.Background()
	for range 2 {
		if err := s.Start(ctx); err != nil {
			t.Fatalf("Start: %v", err)
		}
	}
	for range 2 {
		if err := s.Stop(ctx); err != nil {
			t.Fatalf("Stop: %v", err)
		}
	}
}

// A package that was given no observer uses Nop and must never panic or block.
func TestNop_IsSafeEverywhere(t *testing.T) {
	t.Parallel()
	n := obs.Nop()
	ctx, op := n.Begin(context.Background(), obs.ComponentAuth, "load_session", slog.String("token", "secret"))
	op.SetAttributes(slog.String("a", "b"))
	op.SetOutcome(obs.OutcomeRetry)
	op.End(errors.New("x"))
	op.End(nil)
	if ctx == nil {
		t.Fatal("Begin returned a nil context")
	}
	n.Logger(obs.ComponentAuth).Error("quiet")
	n.Meter(obs.ComponentAuth).Counter("c", "").Add(ctx, 1)
	n.Meter(obs.ComponentAuth).Gauge("g", "").Set(ctx, 1)
	n.Meter(obs.ComponentAuth).Histogram("h", "", "s").Record(ctx, 1)
	c2, span := n.Tracer(obs.ComponentAuth).Start(ctx, "s")
	span.SetAttributes()
	span.RecordError(errors.New("x"))
	span.End()
	if c2 == nil {
		t.Fatal("Tracer.Start returned a nil context")
	}
}

func TestPick(t *testing.T) {
	t.Parallel()
	var typedNil *obs.Service
	cases := map[string]obs.IObserver{
		"no arguments":    obs.Pick(),
		"nil interface":   obs.Pick(nil),
		"typed nil":       obs.Pick(typedNil),
		"nil then a real": obs.Pick(nil, newService(t)),
	}
	for name, got := range cases {
		if got == nil {
			t.Fatalf("%s: Pick returned nil", name)
		}
	}
	real := newService(t)
	if got := obs.Pick(nil, real); got != obs.IObserver(real) {
		t.Fatal("Pick did not return the first set observer")
	}
	if !obs.IsNop(obs.Nop()) || !obs.IsNop(nil) || !obs.IsNop(typedNil) || obs.IsNop(real) {
		t.Fatal("IsNop is wrong")
	}
}

func TestRace_ConcurrentOperations(t *testing.T) {
	t.Parallel()
	s := newService(t)
	const workers, each = 16, 200
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for i := range each {
				_, op := s.Begin(context.Background(), obs.ComponentSQL, "ping")
				if (w+i)%5 == 0 {
					op.End(errors.New("x"))
				} else {
					op.End(nil)
				}
				_ = s.Snapshot()
			}
		})
	}
	wg.Wait()
	var total float64
	for _, sm := range s.Snapshot() {
		if sm.Name == obs.MetricOperations {
			total += sm.Value
		}
	}
	if total != workers*each {
		t.Fatalf("counted %v operations, want %d", total, workers*each)
	}
	if got := find(t, s, obs.MetricInflight, map[string]string{obs.KeyOperation: "ping"}); got.Value != 0 {
		t.Fatalf("inflight = %v, want 0", got.Value)
	}
}

// captureHandler records the attributes of every record, and of every WithAttrs.
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
