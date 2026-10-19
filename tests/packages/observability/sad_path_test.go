package observability_test

import (
	"context"
	"errors"
	"math"
	"testing"

	obs "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
)

// A nil context is repaired, not a panic: telemetry never takes the caller down.
func TestSad_BeginWithNilContext(t *testing.T) {
	t.Parallel()
	s := newService(t)
	ctx, op := s.Begin(nil, obs.ComponentSQL, "ping")
	if ctx == nil {
		t.Fatal("Begin returned a nil context")
	}
	op.End(nil)
}

// Stopping what was never started, and finding what was never recorded, are quiet answers.
func TestSad_StopBeforeStartAndFindMissing(t *testing.T) {
	t.Parallel()
	s := newService(t)
	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("Stop before Start = %v, want nil", err)
	}
	if _, ok := s.Find("nope_total", nil); ok {
		t.Fatal("Find returned a series that was never recorded")
	}
	_, op := s.Begin(context.Background(), obs.ComponentSQL, "ping")
	op.End(nil)
	if _, ok := s.Find(obs.MetricOperations, map[string]string{obs.KeyComponent: "cache"}); ok {
		t.Fatal("Find matched the wrong component")
	}
}

// An outcome set after End changes nothing: the count was already taken.
func TestSad_SetOutcomeAfterEndIsIgnored(t *testing.T) {
	t.Parallel()
	s := newService(t)
	_, op := s.Begin(context.Background(), obs.ComponentSQL, "ping")
	op.End(nil)
	op.SetOutcome(obs.OutcomeError)
	op.End(errors.New("late"))
	if _, ok := s.Find(obs.MetricOperations, map[string]string{obs.KeyOutcome: "error"}); ok {
		t.Fatal("an outcome set after End was counted")
	}
}

// A counter only goes up: a negative delta must not take a total down.
func TestSad_CounterIgnoresNegativeDelta(t *testing.T) {
	t.Parallel()
	s := newService(t)
	c := s.Meter(obs.ComponentCQRS).Counter(obs.MetricCQRSBatches, "")
	c.Add(context.Background(), 5)
	c.Add(context.Background(), -3)
	if got := find(t, s, obs.MetricCQRSBatches, nil); got.Value != 5 {
		t.Fatalf("counter = %v after a negative Add, want 5", got.Value)
	}
}

// One NaN or Inf must not poison a histogram's sum for the life of the process.
func TestSad_HistogramIgnoresNonsense(t *testing.T) {
	t.Parallel()
	s := newService(t)
	h := s.Meter(obs.ComponentCQRS).Histogram(obs.MetricCQRSBatchDuration, "", "s")
	ctx := context.Background()
	for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), -1} {
		h.Record(ctx, v)
	}
	h.Record(ctx, 1)
	got := find(t, s, obs.MetricCQRSBatchDuration, nil)
	if got.Count != 1 || got.Sum != 1 {
		t.Fatalf("histogram = %+v, want exactly the one valid observation", got)
	}
}

// The same for a gauge: a non-finite value is dropped and the last good one stays.
func TestSad_GaugeIgnoresNonFinite(t *testing.T) {
	t.Parallel()
	s := newService(t)
	g := s.Meter(obs.ComponentCQRS).Gauge(obs.MetricCQRSAwaiting, "")
	g.Set(context.Background(), 3)
	g.Set(context.Background(), math.NaN())
	g.Set(context.Background(), math.Inf(1))
	if got := find(t, s, obs.MetricCQRSAwaiting, nil); got.Value != 3 {
		t.Fatalf("gauge = %v, want the last finite value 3", got.Value)
	}
}
