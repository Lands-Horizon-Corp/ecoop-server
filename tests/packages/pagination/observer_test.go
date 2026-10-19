//go:build integration

package pagination_test

import (
	"errors"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pagination"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
	"github.com/Lands-Horizon-Corp/ecoop-server/tests/internal/obstest"
)

// observedService is a pagination service over the shared seeded table, with an
// in-memory observer.
func observedService(t testing.TB) (*fixture, pagination.IPaginationService[Account, int64], *observability.Service) {
	t.Helper()
	fx := need(t)
	o := obstest.New(t)
	svc := pagination.NewPaginationService(&pagination.PaginationService[Account, int64]{
		WriteSQLService: fx.sql, Observer: o,
	}, utils.NewInflight())
	return fx, svc, o
}

func pageCount(o *observability.Service, op, outcome string) float64 {
	return obstest.Count(o, "pagination", op, outcome)
}

// Paginate, Count and Exists are each counted, and labelled with the table.
func TestObserver_QueriesAreOperationsLabelledWithTheTable(t *testing.T) {
	_, svc, o := observedService(t)
	ctx := ctxT(t)
	if _, err := svc.Filter(ctx, and(f("tier", pagination.ModeEqual, "gold"))); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Count(ctx, and(f("tier", pagination.ModeEqual, "gold"))); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Exists(ctx, and(f("tier", pagination.ModeEqual, "gold"))); err != nil {
		t.Fatal(err)
	}
	for _, op := range []string{"paginate", "count", "exists"} {
		if got := pageCount(o, op, "success"); got != 1 {
			t.Errorf("%s success = %v, want 1", op, got)
		}
	}
	sm, ok := o.Find(observability.MetricOperations, map[string]string{observability.KeyOperation: "count"})
	if !ok || sm.Attrs[observability.KeyTable] == "" {
		t.Fatalf("count has no table label: %+v", sm)
	}
}

// The filter values never reach a metric: a hundred different values make one series.
func TestObserver_FilterValuesNeverBecomeLabels(t *testing.T) {
	_, svc, o := observedService(t)
	ctx := ctxT(t)
	for i := range 100 {
		_, _ = svc.Count(ctx, and(f("balance", pagination.ModeGT, i)))
	}
	n := 0
	for _, sm := range o.Snapshot() {
		if sm.Name == observability.MetricOperations && sm.Attrs[observability.KeyOperation] == "count" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("%d series for 100 different filter values, want 1", n)
	}
}

// A query that fails is an error; "no matches" is a success.
func TestObserver_AFailedQueryIsAnErrorAndNoMatchesIsNot(t *testing.T) {
	_, svc, o := observedService(t)
	ctx := ctxT(t)
	if n, err := svc.Count(ctx, and(f("name", pagination.ModeEqual, "nobody-has-this-name"))); err != nil || n != 0 {
		t.Fatalf("Count of nothing = %d, %v", n, err)
	}
	if got := pageCount(o, "count", "success"); got != 1 {
		t.Fatalf("no matches counted as success %v times, want 1", got)
	}
	if _, err := svc.Count(ctx, and(f("nope", pagination.ModeEqual, 1))); !errors.Is(err, pagination.ErrUnknownField) {
		t.Fatalf("Count on an unknown field = %v, want ErrUnknownField", err)
	}
	if got := pageCount(o, "count", "error"); got != 1 {
		t.Fatalf("count error = %v, want 1", got)
	}
}

// No observer, and a bare struct, are fine.
func TestObserver_NoneIsFine(t *testing.T) {
	fx := need(t)
	if _, err := fx.svc.Count(ctxT(t), and()); err != nil {
		t.Fatal(err)
	}
	var zero pagination.PaginationService[Account, int64]
	if _, err := zero.Count(ctxT(t), and()); err == nil {
		t.Fatal("a zero service must return an error, not panic")
	}
}
