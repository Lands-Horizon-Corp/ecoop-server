package observability_test

import (
	"context"
	"log/slog"
	"regexp"
	"testing"

	obs "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
)

var allComponents = []obs.Component{
	obs.ComponentAPI, obs.ComponentAuth, obs.ComponentBroadcast, obs.ComponentBroker, obs.ComponentCache,
	obs.ComponentCDC, obs.ComponentConfig, obs.ComponentCQRS, obs.ComponentCron, obs.ComponentOTP,
	obs.ComponentPagination, obs.ComponentPDF, obs.ComponentQR, obs.ComponentQueue, obs.ComponentSecurity,
	obs.ComponentSMS, obs.ComponentSMTP, obs.ComponentSQL, obs.ComponentStorage,
}

var allOutcomes = []obs.Outcome{obs.OutcomeSuccess, obs.OutcomeError, obs.OutcomeRetry, obs.OutcomeDLQ, obs.OutcomeSkipped}

// A deployed process starts, records, reads back and stops.
func TestSmoke_StartRecordSnapshotStop(t *testing.T) {
	t.Parallel()
	s := newService(t)
	ctx := context.Background()
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	_, op := s.Begin(ctx, obs.ComponentSQL, "ping")
	op.End(nil)
	if len(s.Snapshot()) == 0 {
		t.Fatal("nothing recorded after one operation")
	}
	if err := s.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if got := s.Resource().ServiceName; got != "ecoop-test" {
		t.Fatalf("Resource().ServiceName = %q", got)
	}
}

// Every component and outcome is its own series, so no package can hide another's errors.
func TestSmoke_EveryComponentAndOutcomeIsItsOwnSeries(t *testing.T) {
	t.Parallel()
	s := newService(t)
	for _, c := range allComponents {
		for _, o := range allOutcomes {
			_, op := s.Begin(context.Background(), c, "op")
			op.SetOutcome(o)
			op.End(nil)
		}
	}
	n := 0
	for _, sm := range s.Snapshot() {
		if sm.Name == obs.MetricOperations {
			n++
			if sm.Value != 1 {
				t.Fatalf("%v counted %v, want 1", sm.Attrs, sm.Value)
			}
		}
	}
	if want := len(allComponents) * len(allOutcomes); n != want {
		t.Fatalf("%d operation series, want %d", n, want)
	}
}

// Names are Prometheus-safe as they are: snake_case, no dots, counters end in _total, durations in _seconds.
func TestSmoke_MetricNamesAreExportReady(t *testing.T) {
	t.Parallel()
	valid := regexp.MustCompile(`^[a-z][a-z0-9]*(_[a-z0-9]+)*$`)
	counters := []string{
		obs.MetricOperations, obs.MetricCQRSBatches, obs.MetricCQRSRecords, obs.MetricCQRSApplied,
		obs.MetricCQRSDuplicates, obs.MetricCQRSTombstones, obs.MetricCQRSSkipped, obs.MetricCQRSDeadLettered,
		obs.MetricCQRSRetries, obs.MetricCQRSStale, obs.MetricCQRSCommitFails, obs.MetricCQRSHooks,
		obs.MetricCQRSHooksDropped, obs.MetricCQRSRepaired,
	}
	gauges := []string{obs.MetricInflight, obs.MetricCQRSAwaiting}
	histograms := []string{obs.MetricOperationDuration, obs.MetricCQRSBatchDuration}
	suffix := regexp.MustCompile(`_total$`)
	seconds := regexp.MustCompile(`_seconds$`)
	seen := map[string]bool{}
	for _, n := range append(append(counters, gauges...), histograms...) {
		if !valid.MatchString(n) {
			t.Errorf("metric %q is not snake_case", n)
		}
		if seen[n] {
			t.Errorf("metric %q is defined twice", n)
		}
		seen[n] = true
	}
	for _, n := range counters {
		if !suffix.MatchString(n) {
			t.Errorf("counter %q does not end in _total", n)
		}
	}
	for _, n := range append(append([]string{}, gauges...), histograms...) {
		if suffix.MatchString(n) {
			t.Errorf("%q is not a counter but ends in _total", n)
		}
	}
	for _, n := range histograms {
		if !seconds.MatchString(n) {
			t.Errorf("duration %q does not end in _seconds", n)
		}
	}
}

// Every log line carries the process identity, so a log is attributable without a trace.
func TestSmoke_LogsCarryTheResource(t *testing.T) {
	t.Parallel()
	var got []slog.Attr
	svc := obs.New(obs.Resource{ServiceName: "ecoop-server", Namespace: "e-coop", Version: "1.2.3", Environment: "production"}, slog.New(captureHandler{attrs: &got}))
	svc.Logger(obs.ComponentSQL).Info("hello")
	want := map[string]string{
		"service.name": "ecoop-server", "service.namespace": "e-coop",
		"service.version": "1.2.3", "deployment.environment.name": "production", obs.KeyComponent: "sql",
	}
	for _, a := range got {
		if v, ok := want[a.Key]; ok && a.Value.String() == v {
			delete(want, a.Key)
		}
	}
	if len(want) != 0 {
		t.Fatalf("log is missing %v; got %v", want, got)
	}
}
