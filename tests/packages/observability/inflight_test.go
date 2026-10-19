package observability_test

import (
	"context"
	"log/slog"
	"testing"

	obs "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
)

// A label added after Begin must not move the in-flight count to another
// series: every in-flight series returns to zero.
func TestInflight_LateAttributeDoesNotLeaveADrift(t *testing.T) {
	t.Parallel()
	s := newService(t)
	for range 10 {
		_, op := s.Begin(context.Background(), obs.ComponentPagination, "count")
		op.SetAttributes(slog.String(obs.KeyTable, "accounts"))
		op.End(nil)
	}
	for _, sm := range s.Snapshot() {
		if sm.Name == obs.MetricInflight && sm.Value != 0 {
			t.Fatalf("in-flight series %v = %v after every operation ended, want 0", sm.Attrs, sm.Value)
		}
	}
}

// While an operation is running it is in flight on the series it began on.
func TestInflight_CountsWhileRunning(t *testing.T) {
	t.Parallel()
	s := newService(t)
	_, op := s.Begin(context.Background(), obs.ComponentSQL, "ping")
	op.SetAttributes(slog.String(obs.KeyTable, "t"))
	if got := find(t, s, obs.MetricInflight, map[string]string{obs.KeyOperation: "ping"}); got.Value != 1 {
		t.Fatalf("in-flight = %v while running, want 1", got.Value)
	}
	op.End(nil)
}

// An outcome outside the bounded set can neither create a series nor hide a
// failure: it ends as an error.
func TestOutcome_UnknownValueIsAnError(t *testing.T) {
	t.Parallel()
	s := newService(t)
	for _, bad := range []string{"member-1234", "success\nerror", "", "SUCCESS"} {
		_, op := s.Begin(context.Background(), obs.ComponentSQL, "ping")
		op.SetOutcome(obs.Outcome(bad))
		op.End(nil)
	}
	known := map[string]bool{"success": true, "error": true, "retry": true, "dlq": true, "skipped": true}
	for _, sm := range s.Snapshot() {
		if sm.Name == obs.MetricOperations && !known[sm.Attrs[obs.KeyOutcome]] {
			t.Fatalf("an unbounded outcome became a label: %q", sm.Attrs[obs.KeyOutcome])
		}
	}
	if got := find(t, s, obs.MetricOperations, map[string]string{obs.KeyOutcome: "error"}); got.Value != 4 {
		t.Fatalf("error = %v, want all 4 unknown outcomes counted as errors", got.Value)
	}
}
