//go:build integration

package cdc_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/tests/internal/obstest"
)

func observedService(t testing.TB) (cdc.ICDCService, *observability.Service) {
	t.Helper()
	skipUnlessKafka(t)
	o := obstest.New(t)
	s := cdc.NewCDCService([]string{addr}, fmt.Sprintf("cdc-obs-%d", runID), cfg("CDC_TOPIC_PREFIX", "ecoop"), cdc.WithObserver(o))
	if err := s.Run(ctxT(t)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Cleanup(func() { _ = s.Stop(context.Background()) })
	return s, o
}

func cdcCount(o *observability.Service, op, outcome string) float64 {
	return obstest.Count(o, "cdc", op, outcome)
}

// A delivery that succeeds is one deliver and one commit.
func TestObserver_DeliveryAndCommitAreOperations(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	s, o := observedService(t)
	var sink batchSink
	if err := s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{MaxBatch: 100}, sink.handle); err != nil {
		t.Fatal(err)
	}
	p := producer(t)
	for i := range 3 {
		if err := p.Publish(ctxT(t), topic, key(i), event(cdc.OpCreate, i, "a")); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "3 records", func() bool { return len(sink.records()) == 3 })
	waitFor(t, "a commit", func() bool { return cdcCount(o, "commit", "success") >= 1 })
	if got := cdcCount(o, "deliver", "success"); got < 1 {
		t.Fatalf("deliver success = %v, want at least 1", got)
	}
	if got := cdcCount(o, "deliver", "error"); got != 0 {
		t.Fatalf("deliver error = %v, want 0", got)
	}
}

// A failing handler is retried, and every failed attempt is a retry, not a final error.
func TestObserver_FailedAttemptsAreRetries(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	s, o := observedService(t)
	var calls atomic.Int64
	handler := func(_ context.Context, _ []cdc.Record) error {
		if calls.Add(1) <= 3 {
			return errors.New("not yet")
		}
		return nil
	}
	if err := s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{MaxBatch: 10}, handler); err != nil {
		t.Fatal(err)
	}
	if err := producer(t).Publish(ctxT(t), topic, key(1), event(cdc.OpCreate, 1, "a")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the batch to succeed after 3 failures", func() bool { return cdcCount(o, "deliver", "success") >= 1 })
	if got := cdcCount(o, "deliver", "retry"); got != 3 {
		t.Fatalf("deliver retry = %v, want 3", got)
	}
	if got := cdcCount(o, "deliver", "error"); got != 0 {
		t.Fatalf("a retried attempt was counted as %v final errors", got)
	}
}

// Parking a record is its own outcome, so a dashboard can tell it from a failure.
func TestObserver_DeadLetterIsCountedAsDLQ(t *testing.T) {
	t.Parallel()
	s, o := observedService(t)
	topic := names()
	park(t, s, topic, 0, 7, `{"id":1}`, "v")
	park(t, s, topic, 0, 8, `{"id":2}`, "v")
	if got := cdcCount(o, "dead_letter", "dlq"); got != 2 {
		t.Fatalf("dead_letter dlq = %v, want 2", got)
	}
	if got := cdcCount(o, "dead_letter", "error"); got != 0 {
		t.Fatalf("a parked record counted as %v errors", got)
	}
}

// A failed dead-letter is an error: the record was not parked.
func TestObserver_AFailedDeadLetterIsAnError(t *testing.T) {
	t.Parallel()
	skipUnlessKafka(t)
	o := obstest.New(t)
	idle := cdc.NewCDCService([]string{addr}, "g", "p", cdc.WithObserver(o)) // never Run
	err := idle.DeadLetter(ctxT(t), cdc.Record{Topic: "t"}, errors.New("x"))
	if !errors.Is(err, cdc.ErrNotRunning) {
		t.Fatalf("DeadLetter on an idle service = %v, want ErrNotRunning", err)
	}
	if got := cdcCount(o, "dead_letter", "error"); got != 1 {
		t.Fatalf("dead_letter error = %v, want 1", got)
	}
	if got := cdcCount(o, "dead_letter", "dlq"); got != 0 {
		t.Fatalf("a record that was not parked counted as dlq %v times", got)
	}
}

// No observer, and a bare struct, are fine.
func TestObserver_NoneIsFine(t *testing.T) {
	t.Parallel()
	s := service(t)
	topic := names()
	park(t, s, topic, 0, 1, `{"id":1}`, "v")
	var zero cdc.CDCService
	if err := zero.DeadLetter(ctxT(t), cdc.Record{}, errors.New("x")); err == nil {
		t.Fatal("a zero service must return an error, not panic")
	}
}
