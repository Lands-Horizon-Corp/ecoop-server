//go:build integration

package cqrs_test

import (
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/tests/internal/obstest"
)

// withObserver gives the service an in-memory observer and returns it.
func withObserver(t testing.TB) (*observability.Service, func(*cqrsSvc)) {
	t.Helper()
	o := obstest.New(t)
	return o, func(c *cqrsSvc) { c.Observer = o }
}

func total(t testing.TB, o *observability.Service, name string, want map[string]string) float64 {
	t.Helper()
	if sm, ok := o.Find(name, want); ok {
		return sm.Value
	}
	return 0
}

// The metrics the observer sees are the totals Stats reports, so a dashboard and
// the Go API can never disagree about how much was replicated.
func TestObserver_MetricsMirrorStats(t *testing.T) {
	t.Parallel()
	o, mod := withObserver(t)
	r := newRig(t, 2, mod)
	ctx := longCtx(t)
	r.w.bulkInsert(1, 30, "a")
	r.w.exec(`UPDATE accounts SET balance = balance + 1, version = version + 1 WHERE id <= 10`)
	r.w.emitRange(cdc.OpUpdate, 1, 10)
	if err := r.w.deliverNew(ctx, r.f, 10); err != nil {
		t.Fatal(err)
	}
	r.waitInSync()

	st := r.svc.Stats()
	if st.Batches == 0 || st.Records == 0 || st.Applied == 0 {
		t.Fatalf("nothing replicated: %+v", st)
	}
	cq := map[string]string{observability.KeyComponent: "cqrs"}
	for name, want := range map[string]int64{
		observability.MetricCQRSBatches: st.Batches,
		observability.MetricCQRSRecords: st.Records,
		observability.MetricCQRSApplied: st.Applied,
	} {
		if got := total(t, o, name, cq); int64(got) != want {
			t.Fatalf("%s = %v, Stats says %d", name, got, want)
		}
	}
}

// Every batch is one operation: counted by outcome and timed.
func TestObserver_EveryBatchIsAnOperation(t *testing.T) {
	t.Parallel()
	o, mod := withObserver(t)
	r := newRig(t, 1, mod)
	ctx := longCtx(t)
	r.w.bulkInsert(1, 20, "a")
	if err := r.w.deliverNew(ctx, r.f, 5); err != nil {
		t.Fatal(err)
	}
	r.waitInSync()
	st := r.svc.Stats()

	handled := total(t, o, observability.MetricOperations, map[string]string{
		observability.KeyComponent: "cqrs", observability.KeyOperation: "handle", observability.KeyOutcome: "success",
	})
	if int64(handled) != st.Batches {
		t.Fatalf("%v handle operations succeeded, Stats counted %d batches", handled, st.Batches)
	}
	d, ok := o.Find(observability.MetricCQRSBatchDuration, nil)
	if !ok || int64(d.Count) != st.Batches {
		t.Fatalf("batch duration = %+v, want %d observations", d, st.Batches)
	}
	if got := total(t, o, observability.MetricInflight, map[string]string{observability.KeyOperation: "handle"}); got != 0 {
		t.Fatalf("inflight = %v after the batches ended, want 0", got)
	}
}

// A redelivery the checkpoint already covers is "skipped", not an error.
func TestObserver_ARedeliveredBatchIsSkipped(t *testing.T) {
	t.Parallel()
	o, mod := withObserver(t)
	r := newRig(t, 1, mod)
	ctx := longCtx(t)
	r.w.bulkInsert(1, 10, "a")
	if err := r.w.deliverNew(ctx, r.f, 10); err != nil {
		t.Fatal(err)
	}
	r.waitInSync()
	// Kafka replays what the consumer had already seen: the checkpoint covers all of it.
	if err := r.w.deliverRange(ctx, r.f, 0, 0, r.f.committedOffset(0), 10); err != nil {
		t.Fatal(err)
	}
	skipped := total(t, o, observability.MetricOperations, map[string]string{
		observability.KeyOperation: "handle", observability.KeyOutcome: "skipped",
	})
	if skipped == 0 {
		t.Fatal("a fully duplicate batch was not reported as skipped")
	}
	if bad := total(t, o, observability.MetricOperations, map[string]string{observability.KeyOutcome: "error"}); bad != 0 {
		t.Fatalf("a duplicate was counted as %v errors", bad)
	}
}

// Parked rows show up on the gauge, and fall back to zero once they are repaired.
func TestObserver_AwaitingGaugeFollowsTheRepairBacklog(t *testing.T) {
	t.Parallel()
	o, mod := withObserver(t)
	r := newRig(t, 2, fastRepair, mod)
	ctx := longCtx(t)
	strictRead(r.p)
	r.w.bulkInsert(1, 20, "a")
	r.w.exec(`UPDATE accounts SET balance = 13, version = version + 1 WHERE id IN (3, 4)`)
	r.w.emitRange(cdc.OpUpdate, 3, 4)
	if err := r.w.deliverNew(ctx, r.f, 20); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the gauge to show the 2 parked rows", func() bool {
		return total(t, o, observability.MetricCQRSAwaiting, nil) == 2
	})
	if got := total(t, o, observability.MetricCQRSDeadLettered, nil); got < 2 {
		t.Fatalf("dead-lettered counter = %v, want at least 2", got)
	}
	relaxRead(r.p)
	r.waitInSync()
	eventually(t, "the gauge to fall to 0", func() bool {
		return total(t, o, observability.MetricCQRSAwaiting, nil) == 0
	})
	if got := total(t, o, observability.MetricCQRSRepaired, nil); got != 2 {
		t.Fatalf("repaired counter = %v, want 2", got)
	}
}

// With no observer the service behaves exactly as before.
func TestObserver_NoneIsFine(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	ctx := longCtx(t)
	r.w.bulkInsert(1, 10, "a")
	if err := r.w.deliverNew(ctx, r.f, 10); err != nil {
		t.Fatal(err)
	}
	r.waitInSync()
	if r.svc.Stats().Applied == 0 {
		t.Fatal("nothing applied without an observer")
	}
}
