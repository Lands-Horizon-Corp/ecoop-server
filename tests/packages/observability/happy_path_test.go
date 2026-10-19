package observability_test

import (
	"context"
	"log/slog"
	"sort"
	"testing"
	"time"

	obs "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
)

type ctxKey struct{}

// An operation measures real elapsed time, from a clock that cannot run backwards.
func TestHappy_DurationIsMeasured(t *testing.T) {
	t.Parallel()
	s := newService(t)
	_, op := s.Begin(context.Background(), obs.ComponentSQL, "ping")
	time.Sleep(20 * time.Millisecond)
	op.End(nil)
	d := find(t, s, obs.MetricOperationDuration, map[string]string{obs.KeyOperation: "ping"})
	if d.Count != 1 || d.Sum < 0.02 || d.Sum > 30 {
		t.Fatalf("duration = %+v, want 1 observation of at least 20ms", d)
	}
}

// Begin hands back the caller's context: its values, cancellation and deadline all survive.
func TestHappy_ContextIsPreserved(t *testing.T) {
	t.Parallel()
	s := newService(t)
	base, cancel := context.WithTimeout(context.WithValue(context.Background(), ctxKey{}, "v"), time.Minute)
	defer cancel()
	ctx, op := s.Begin(base, obs.ComponentQueue, "run")
	defer op.End(nil)
	if ctx.Value(ctxKey{}) != "v" {
		t.Fatal("the context value was lost")
	}
	if _, ok := ctx.Deadline(); !ok {
		t.Fatal("the deadline was lost")
	}
	cancel()
	if ctx.Err() == nil {
		t.Fatal("cancelling the parent did not cancel the returned context")
	}
}

// A bounded attribute set on an operation becomes a label of its series.
func TestHappy_BoundedAttributeBecomesALabel(t *testing.T) {
	t.Parallel()
	s := newService(t)
	_, op := s.Begin(context.Background(), obs.ComponentPagination, "count")
	op.SetAttributes(slog.String(obs.KeyTable, "accounts"))
	op.End(nil)
	find(t, s, obs.MetricOperations, map[string]string{obs.KeyTable: "accounts", obs.KeyOutcome: "success"})
}

// A realistic mix is counted exactly, outcome by outcome.
func TestHappy_MixedOutcomesAreCountedExactly(t *testing.T) {
	t.Parallel()
	s := newService(t)
	want := map[obs.Outcome]int{obs.OutcomeSuccess: 50, obs.OutcomeError: 7, obs.OutcomeRetry: 3, obs.OutcomeDLQ: 2, obs.OutcomeSkipped: 11}
	for o, n := range want {
		for range n {
			_, op := s.Begin(context.Background(), obs.ComponentCDC, "deliver")
			op.SetOutcome(o)
			op.End(nil)
		}
	}
	for o, n := range want {
		if got := find(t, s, obs.MetricOperations, map[string]string{obs.KeyOutcome: string(o)}); got.Value != float64(n) {
			t.Fatalf("%s = %v, want %d", o, got.Value, n)
		}
	}
}

// The same instrument name in two components is two series.
func TestHappy_ComponentsDoNotShareInstruments(t *testing.T) {
	t.Parallel()
	s := newService(t)
	ctx := context.Background()
	s.Meter(obs.ComponentCache).Counter("things_total", "").Add(ctx, 1)
	s.Meter(obs.ComponentSQL).Counter("things_total", "").Add(ctx, 2)
	if got := find(t, s, "things_total", map[string]string{obs.KeyComponent: "cache"}); got.Value != 1 {
		t.Fatalf("cache = %v, want 1", got.Value)
	}
	if got := find(t, s, "things_total", map[string]string{obs.KeyComponent: "sql"}); got.Value != 2 {
		t.Fatalf("sql = %v, want 2", got.Value)
	}
}

// Snapshot is stable and ordered, so an exporter and a diff see the same thing every time.
func TestHappy_SnapshotIsOrdered(t *testing.T) {
	t.Parallel()
	s := newService(t)
	for _, c := range allComponents {
		_, op := s.Begin(context.Background(), c, "op")
		op.End(nil)
	}
	snap := s.Snapshot()
	if !sort.SliceIsSorted(snap, func(i, j int) bool { return snap[i].Name < snap[j].Name }) {
		t.Fatal("Snapshot is not ordered by name")
	}
	again := s.Snapshot()
	if len(again) != len(snap) {
		t.Fatalf("two Snapshots differ in length: %d, %d", len(snap), len(again))
	}
	for i := range snap {
		if snap[i].Name != again[i].Name {
			t.Fatalf("Snapshot order is not stable at %d", i)
		}
	}
}
