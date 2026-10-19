package observability_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	obs "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
)

func mustNotPanic(t *testing.T, name string, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("%s panicked: %v", name, r)
		}
	}()
	fn()
}

// A nil *Service and a zero Service are both usable: a package holding either one must not crash.
func TestZeroValue_NilAndZeroService(t *testing.T) {
	t.Parallel()
	var nilSvc *obs.Service
	zero := &obs.Service{}
	ctx := context.Background()
	for name, s := range map[string]*obs.Service{"nil": nilSvc, "zero": zero} {
		mustNotPanic(t, name, func() {
			if err := s.Start(ctx); err != nil {
				t.Errorf("%s Start = %v", name, err)
			}
			c, op := s.Begin(ctx, obs.ComponentSQL, "ping")
			if c == nil {
				t.Errorf("%s Begin returned a nil context", name)
			}
			op.SetAttributes(slog.String("a", "b"))
			op.SetOutcome(obs.OutcomeRetry)
			op.End(errors.New("x"))
			s.Logger(obs.ComponentSQL).Info("quiet")
			m := s.Meter(obs.ComponentSQL)
			m.Counter("c_total", "").Add(ctx, 1)
			m.Gauge("g", "").Set(ctx, 1)
			m.Histogram("h_seconds", "", "s").Record(ctx, 1)
			_, span := s.Tracer(obs.ComponentSQL).Start(ctx, "s")
			span.RecordError(errors.New("x"))
			span.End()
			_ = s.Snapshot()
			_, _ = s.Find("c_total", nil)
			_ = s.Resource()
			_ = s.Overflowed()
			if err := s.Stop(ctx); err != nil {
				t.Errorf("%s Stop = %v", name, err)
			}
		})
	}
}

// A zero Service still records: the registry is created on first use.
func TestZeroValue_ZeroServiceRecords(t *testing.T) {
	t.Parallel()
	s := &obs.Service{}
	_, op := s.Begin(context.Background(), obs.ComponentSQL, "ping")
	op.End(nil)
	if _, ok := s.Find(obs.MetricOperations, map[string]string{obs.KeyOutcome: "success"}); !ok {
		t.Fatal("a zero Service did not record")
	}
}

// An absent value: a missing attribute value, an empty component and an empty operation are all fine.
func TestZeroValue_AbsentValues(t *testing.T) {
	t.Parallel()
	s := newService(t)
	_, op := s.Begin(context.Background(), "", "", slog.Attr{}, slog.String(obs.KeyTable, ""))
	op.End(nil)
	if _, ok := s.Find(obs.MetricOperations, nil); !ok {
		t.Fatal("an operation with empty names was not recorded")
	}
	var typedNil *obs.Service
	if !obs.IsNop(typedNil) {
		t.Fatal("a typed nil observer is not treated as Nop")
	}
	obs.Pick(typedNil).Meter(obs.ComponentSQL).Counter("c_total", "").Add(context.Background(), 1)
}

// New with a nil logger falls back to slog.Default and a zero Resource, and still works.
func TestZeroValue_NewWithNothing(t *testing.T) {
	t.Parallel()
	s := obs.New(obs.Resource{}, nil)
	_, op := s.Begin(context.Background(), obs.ComponentSQL, "ping")
	op.End(nil)
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}
