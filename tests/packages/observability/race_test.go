package observability_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"testing"

	obs "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
)

// Counter.Add under heavy contention is exact: no increment is lost.
func TestRace_CounterIsExactUnderContention(t *testing.T) {
	t.Parallel()
	s := newService(t)
	c := s.Meter(obs.ComponentCQRS).Counter(obs.MetricCQRSRecords, "")
	const workers, each = 64, 1000
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for range each {
				c.Add(context.Background(), 1)
			}
		})
	}
	wg.Wait()
	if got := find(t, s, obs.MetricCQRSRecords, nil); got.Value != workers*each {
		t.Fatalf("counter = %v, want %d", got.Value, workers*each)
	}
}

// Gauge and histogram stay consistent while being read and written at once.
func TestRace_GaugeHistogramAndSnapshot(t *testing.T) {
	t.Parallel()
	s := newService(t)
	m := s.Meter(obs.ComponentCQRS)
	g := m.Gauge(obs.MetricCQRSAwaiting, "")
	h := m.Histogram(obs.MetricCQRSBatchDuration, "", "s")
	const workers, each = 16, 500
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for i := range each {
				g.Set(context.Background(), float64(w*each+i))
				h.Record(context.Background(), 1)
				_ = s.Snapshot()
			}
		})
	}
	wg.Wait()
	if got := find(t, s, obs.MetricCQRSBatchDuration, nil); got.Count != workers*each || got.Sum != workers*each {
		t.Fatalf("histogram = %+v, want %d observations", got, workers*each)
	}
}

// Many child operations from one parent context, in parallel, each counted once.
func TestRace_SiblingOperationsFromOneParent(t *testing.T) {
	t.Parallel()
	s := newService(t)
	parent, op := s.Begin(context.Background(), obs.ComponentAPI, "GET /accounts")
	var wg sync.WaitGroup
	for i := range 200 {
		wg.Go(func() {
			_, child := s.Begin(parent, obs.ComponentSQL, "ping")
			if i%10 == 0 {
				child.End(errors.New("x"))
				return
			}
			child.End(nil)
		})
	}
	wg.Wait()
	op.End(nil)
	if got := find(t, s, obs.MetricOperations, map[string]string{obs.KeyComponent: "sql", obs.KeyOutcome: "success"}); got.Value != 180 {
		t.Fatalf("sql success = %v, want 180", got.Value)
	}
	if got := find(t, s, obs.MetricInflight, nil); got.Value != 0 {
		t.Fatalf("inflight = %v, want 0", got.Value)
	}
}

// SetAttributes and SetOutcome from other goroutines, while End runs, is race-free and counts once.
func TestRace_SetAttributesWhileEnding(t *testing.T) {
	t.Parallel()
	s := newService(t)
	for range 200 {
		_, op := s.Begin(context.Background(), obs.ComponentSQL, "ping")
		var wg sync.WaitGroup
		for range 4 {
			wg.Go(func() {
				op.SetAttributes(slog.String(obs.KeyTable, "t"))
				op.SetOutcome(obs.OutcomeRetry)
				op.End(nil)
			})
		}
		wg.Wait()
	}
	var total float64
	for _, sm := range s.Snapshot() {
		if sm.Name == obs.MetricOperations {
			total += sm.Value
		}
	}
	if total != 200 {
		t.Fatalf("counted %v, want 200: an operation was counted more than once", total)
	}
}

// Start and Stop from many goroutines at once are safe and leave a defined state.
func TestRace_ConcurrentLifecycle(t *testing.T) {
	t.Parallel()
	s := newService(t)
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(2)
		go func() { defer wg.Done(); _ = s.Start(context.Background()) }()
		go func() { defer wg.Done(); _ = s.Stop(context.Background()) }()
	}
	wg.Wait()
	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("Stop = %v", err)
	}
}

// Concurrent loggers, meters and operations from every component at once.
func TestRace_EveryComponentAtOnce(t *testing.T) {
	t.Parallel()
	s := newService(t)
	var wg sync.WaitGroup
	for _, c := range allComponents {
		for range 4 {
			wg.Go(func() {
				for i := range 100 {
					s.Logger(c).Info("tick")
					s.Meter(c).Counter(fmt.Sprintf("%s_total", c), "").Add(context.Background(), 1)
					_, op := s.Begin(context.Background(), c, "op")
					op.End(nil)
					_ = i
				}
			})
		}
	}
	wg.Wait()
	for _, c := range allComponents {
		if got := find(t, s, string(c)+"_total", nil); got.Value != 400 {
			t.Fatalf("%s counter = %v, want 400", c, got.Value)
		}
	}
}

// Recording an operation must not allocate unboundedly: a sustained burst does not grow the heap by more than the series it created.
func TestLoad_SustainedBurstDoesNotLeak(t *testing.T) {
	s := newService(t)
	run := func(n int) {
		for range n {
			_, op := s.Begin(context.Background(), obs.ComponentSQL, "ping")
			op.End(nil)
		}
	}
	run(1000) // warm up: the series exist now
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	run(200_000)
	runtime.GC()
	runtime.ReadMemStats(&after)
	if grew := int64(after.HeapAlloc) - int64(before.HeapAlloc); grew > 8<<20 {
		t.Fatalf("the heap grew by %d bytes over 200k operations on existing series", grew)
	}
	if n := len(s.Snapshot()); n > 4 {
		t.Fatalf("%d series after repeated identical operations, want at most 4", n)
	}
}

func BenchmarkOperation(b *testing.B) {
	s := obs.New(obs.Resource{ServiceName: "bench"}, slog.New(slog.DiscardHandler))
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		_, op := s.Begin(ctx, obs.ComponentSQL, "ping")
		op.End(nil)
	}
}

func BenchmarkOperationParallel(b *testing.B) {
	s := obs.New(obs.Resource{ServiceName: "bench"}, slog.New(slog.DiscardHandler))
	ctx := context.Background()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, op := s.Begin(ctx, obs.ComponentSQL, "ping")
			op.End(nil)
		}
	})
}

func BenchmarkNopOperation(b *testing.B) {
	n := obs.Nop()
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		_, op := n.Begin(ctx, obs.ComponentSQL, "ping")
		op.End(nil)
	}
}
