//go:build integration

package broker_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/broker"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/tests/internal/obstest"
)

func observedBroker(t testing.TB, group string) (broker.IBrokerService, *observability.Service) {
	t.Helper()
	if skipReason != "" {
		t.Skip(skipReason)
	}
	o := obstest.New(t)
	b := broker.NewBrokerService([]string{addr}, group, nil, broker.WithObserver(o))
	if err := b.Run(ctxT(t)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop(context.Background()) })
	return b, o
}

func brokerCount(o *observability.Service, op, outcome string) float64 {
	return obstest.Count(o, "broker", op, outcome)
}

func waitCount(t testing.TB, what string, o *observability.Service, op, outcome string, want float64) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for brokerCount(o, op, outcome) < want {
		if time.Now().After(deadline) {
			t.Fatalf("never saw %s: %s/%s = %v, want %v", what, op, outcome, brokerCount(o, op, outcome), want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Publishing and handling are each counted.
func TestObserver_PublishAndHandleAreOperations(t *testing.T) {
	t.Parallel()
	topic, group := names()
	b, o := observedBroker(t, group)
	var in inbox
	if err := b.Subscribe(ctxT(t), topic, in.handle); err != nil {
		t.Fatal(err)
	}
	publish(t, b, topic, 5, "k")
	in.wait(t, 5)
	if got := brokerCount(o, "publish", "success"); got != 5 {
		t.Fatalf("publish success = %v, want 5", got)
	}
	waitCount(t, "5 handled messages", o, "handle", "success", 5)
}

// Failed attempts that will be retried are retries; only the last one is an error,
// and the parked record is its own outcome.
func TestObserver_RetriesThenDeadLetter(t *testing.T) {
	t.Parallel()
	topic, group := names()
	b, o := observedBroker(t, group)
	var calls atomic.Int64
	failing := func(context.Context, []byte, []byte) error {
		calls.Add(1)
		return errors.New("always")
	}
	if err := b.Subscribe(ctxT(t), topic, failing); err != nil {
		t.Fatal(err)
	}
	publish(t, b, topic, 1, "poison")
	waitCount(t, "the record to be parked", o, "dead_letter", "dlq", 1)
	if got := calls.Load(); got != broker.HandlerAttempts {
		t.Fatalf("handler ran %d times, want %d", got, broker.HandlerAttempts)
	}
	if got := brokerCount(o, "handle", "retry"); got != broker.HandlerAttempts-1 {
		t.Fatalf("handle retry = %v, want %d", got, broker.HandlerAttempts-1)
	}
	if got := brokerCount(o, "handle", "error"); got != 1 {
		t.Fatalf("handle error = %v, want 1 (the last attempt)", got)
	}
	if got := brokerCount(o, "dead_letter", "error"); got != 0 {
		t.Fatalf("a parked record counted as %v errors", got)
	}
}

// A publish that cannot be made is an error and leaves nothing in flight.
func TestObserver_AFailedPublishIsAnError(t *testing.T) {
	t.Parallel()
	o := obstest.New(t)
	idle := broker.NewBrokerService([]string{"127.0.0.1:1"}, "g", nil, broker.WithObserver(o)) // never Run
	if err := idle.Publish(ctxT(t), "t", nil, []byte("v")); !errors.Is(err, broker.ErrNotRunning) {
		t.Fatalf("Publish on an idle service = %v, want ErrNotRunning", err)
	}
	// Refused before an operation began, so it is not counted at all.
	if got := brokerCount(o, "publish", "error") + brokerCount(o, "publish", "success"); got != 0 {
		t.Fatalf("a refused publish was counted %v times", got)
	}
}

// No observer, and a bare struct, are fine.
func TestObserver_NoneIsFine(t *testing.T) {
	t.Parallel()
	topic, group := names()
	b := service(t, group)
	var in inbox
	_ = b.Subscribe(ctxT(t), topic, in.handle)
	publish(t, b, topic, 2, "k")
	in.wait(t, 2)
	var zero broker.BrokerService
	if err := zero.Run(ctxT(t)); err == nil {
		t.Fatal("a zero service must return an error, not panic")
	}
}
