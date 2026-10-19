package observability_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	obs "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
)

// panickingHandler is a log backend that has gone wrong.
type panickingHandler struct{}

func (panickingHandler) Enabled(context.Context, slog.Level) bool  { return true }
func (panickingHandler) Handle(context.Context, slog.Record) error { panic("log backend down") }
func (h panickingHandler) WithAttrs([]slog.Attr) slog.Handler      { return h }
func (h panickingHandler) WithGroup(string) slog.Handler           { return h }

// failingHandler returns an error from every write, as a full disk or a closed pipe does.
type failingHandler struct{ calls *int }

func (failingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h failingHandler) Handle(context.Context, slog.Record) error {
	*h.calls++
	return errors.New("write failed")
}
func (h failingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h failingHandler) WithGroup(string) slog.Handler      { return h }

// A log backend that panics must not stop the process from starting or stopping.
func TestChaos_PanickingLogBackendDoesNotBreakLifecycle(t *testing.T) {
	t.Parallel()
	s := obs.New(obs.Resource{ServiceName: "x"}, slog.New(panickingHandler{}))
	ctx := context.Background()
	mustNotPanic(t, "Start", func() {
		if err := s.Start(ctx); err != nil {
			t.Errorf("Start = %v", err)
		}
	})
	_, op := s.Begin(ctx, obs.ComponentSQL, "ping")
	op.End(nil)
	mustNotPanic(t, "Stop", func() {
		if err := s.Stop(ctx); err != nil {
			t.Errorf("Stop = %v", err)
		}
	})
}

// A log backend that errors changes nothing about the metrics.
func TestChaos_FailingLogBackendKeepsMetrics(t *testing.T) {
	t.Parallel()
	calls := 0
	s := obs.New(obs.Resource{ServiceName: "x"}, slog.New(failingHandler{calls: &calls})).(*obs.Service)
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start = %v", err)
	}
	for range 10 {
		_, op := s.Begin(context.Background(), obs.ComponentSQL, "ping")
		op.End(nil)
	}
	if got := find(t, s, obs.MetricOperations, nil); got.Value != 10 {
		t.Fatalf("counted %v, want 10", got.Value)
	}
}

// A cancelled or expired context never stops an operation from being counted: a request that timed out is exactly what must show up.
func TestChaos_CancelledContextStillCounts(t *testing.T) {
	t.Parallel()
	s := newService(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, op := s.Begin(ctx, obs.ComponentSQL, "ping")
	op.End(ctx.Err())
	exp, cancel2 := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel2()
	<-exp.Done()
	_, op2 := s.Begin(exp, obs.ComponentSQL, "ping")
	op2.End(exp.Err())
	if got := find(t, s, obs.MetricOperations, map[string]string{obs.KeyOutcome: "error"}); got.Value != 2 {
		t.Fatalf("counted %v, want 2", got.Value)
	}
}

// A panic in the caller, recovered above, still ends the operation when End is deferred, so inflight returns to zero.
func TestChaos_PanicInsideAnOperation(t *testing.T) {
	t.Parallel()
	s := newService(t)
	func() {
		defer func() { _ = recover() }()
		_, op := s.Begin(context.Background(), obs.ComponentSQL, "ping")
		var err error
		defer func() {
			if r := recover(); r != nil {
				op.End(errors.New("panic"))
				panic(r)
			}
			op.End(err)
		}()
		panic("boom")
	}()
	if got := find(t, s, obs.MetricInflight, nil); got.Value != 0 {
		t.Fatalf("inflight = %v after a panic, want 0", got.Value)
	}
	if got := find(t, s, obs.MetricOperations, map[string]string{obs.KeyOutcome: "error"}); got.Value != 1 {
		t.Fatalf("the panic was counted %v times, want 1", got.Value)
	}
}

// A huge error and a huge attribute never end up in the registry: only the bounded labels are kept.
func TestChaos_HugeErrorIsNotStored(t *testing.T) {
	t.Parallel()
	s := newService(t)
	_, op := s.Begin(context.Background(), obs.ComponentSQL, "ping", slog.String("sql", strings.Repeat("SELECT 1;", 1<<18)))
	op.End(errors.New(strings.Repeat("secret-row-value ", 1<<18)))
	for _, sm := range s.Snapshot() {
		for k, v := range sm.Attrs {
			if len(v) > obs.MaxLabelValueLen || strings.Contains(k+v, "secret-row-value") || strings.Contains(v, "SELECT") {
				t.Fatalf("hostile data reached the registry: %s=%q", k, v)
			}
		}
	}
}

// With the series cap reached and many goroutines racing to create new series, no count is lost and the registry stays bounded.
func TestChaos_OverflowUnderContention(t *testing.T) {
	t.Parallel()
	s := newService(t)
	const workers, each = 16, 400
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for i := range each {
				_, op := s.Begin(context.Background(), obs.ComponentPagination, "count",
					slog.String(obs.KeyTable, "t-"+strings.Repeat("x", w)+"-"+string(rune('a'+i%26))+strings.Repeat("y", i)))
				op.End(nil)
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
		t.Fatalf("counted %v, want %d", total, workers*each)
	}
	if n := len(s.Snapshot()); n > obs.MaxSeries+16 {
		t.Fatalf("%d series, want at most %d", n, obs.MaxSeries+16)
	}
}
