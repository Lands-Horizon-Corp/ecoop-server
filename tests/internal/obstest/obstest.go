// Package obstest is what every package's observer tests share: an in-memory
// observer, a way to read how often an operation ended with an outcome, and a
// check that nothing sensitive reached the registry. It needs no build tag and
// no infrastructure, so any test may use it.
package obstest

import (
	"log/slog"
	"strings"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
)

// New is an observer that keeps its metrics in memory and logs nowhere.
func New(t testing.TB) *observability.Service {
	t.Helper()
	o, ok := observability.New(observability.Resource{ServiceName: "ecoop-test"}, slog.New(slog.DiscardHandler)).(*observability.Service)
	if !ok {
		t.Fatal("observability.New does not return a *observability.Service")
	}
	return o
}

// Count is how many operations of component ended with outcome, or 0 when none
// did. op is the operation name; pass "" to count every operation of component.
func Count(o *observability.Service, component, op, outcome string) float64 {
	want := map[string]string{
		observability.KeyComponent: component,
		observability.KeyOutcome:   outcome,
	}
	if op != "" {
		want[observability.KeyOperation] = op
	}
	if op != "" {
		sm, ok := o.Find(observability.MetricOperations, want)
		if !ok {
			return 0
		}
		return sm.Value
	}
	var total float64
	for _, sm := range o.Snapshot() {
		if sm.Name != observability.MetricOperations || sm.Attrs[observability.KeyComponent] != component || sm.Attrs[observability.KeyOutcome] != outcome {
			continue
		}
		total += sm.Value
	}
	return total
}

// Counter binds Count to one component, for a package's own helper:
//
//	var smtpCount = obstest.Counter("smtp")
func Counter(component string) func(o *observability.Service, op, outcome string) float64 {
	return func(o *observability.Service, op, outcome string) float64 {
		return Count(o, component, op, outcome)
	}
}

// NoLeak fails the test if any of leaks appears in a series name, a label name
// or a label value of the registry. Pass the secrets, keys, names, addresses
// and identifiers the test put through the service.
func NoLeak(t testing.TB, o *observability.Service, leaks ...string) {
	t.Helper()
	for _, sm := range o.Snapshot() {
		var blob strings.Builder
		blob.WriteString(sm.Name)
		for k, v := range sm.Attrs {
			blob.WriteString(" " + k + "=" + v)
		}
		for _, leak := range leaks {
			if leak != "" && strings.Contains(blob.String(), leak) {
				t.Fatalf("%q leaked into the registry: %s", leak, blob.String())
			}
		}
	}
}

// NoSeries fails the test if the registry holds a series for component and
// op with this outcome: for a call that must be refused before any work, so it
// is not counted at all.
func NoSeries(t testing.TB, o *observability.Service, component string, op string, outcomes ...string) {
	t.Helper()
	for _, outcome := range outcomes {
		if got := Count(o, component, op, outcome); got != 0 {
			t.Fatalf("%s %s %s was counted %v times, want 0", component, op, outcome, got)
		}
	}
}

// NothingInFlight fails the test if any operation is still running.
func NothingInFlight(t testing.TB, o *observability.Service) {
	t.Helper()
	for _, sm := range o.Snapshot() {
		if sm.Name == observability.MetricInflight && sm.Value != 0 {
			t.Errorf("still in flight: %v = %v", sm.Attrs, sm.Value)
		}
	}
}
