//go:build integration

package sql_test

import (
	"context"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	ecsql "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sql"
	"github.com/Lands-Horizon-Corp/ecoop-server/tests/internal/obstest"
)

func sqlObserver(t testing.TB) *observability.Service {
	t.Helper()
	return obstest.New(t)
}

func sqlCount(o *observability.Service, op, outcome string) float64 {
	return obstest.Count(o, "sql", op, outcome)
}

// Start, Ping, Run and Migrate are each one counted, timed operation.
func TestObserver_LifecycleAndMigrationsAreOperations(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	writeReleases(t, e, 1, 2)
	o := sqlObserver(t)
	s := ecsql.NewSQLService(e.dsn, 1, 2, e.dir, true, e.out, nil, nil, ecsql.WithObserver(o))
	ctx := ctxT(t)
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer s.Stop(context.Background())
	if err := s.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	for _, op := range []string{"start", "ping", "run", "migrate"} {
		if got := sqlCount(o, op, "success"); got != 1 {
			t.Errorf("%s success = %v, want 1", op, got)
		}
	}
	d, ok := o.Find(observability.MetricOperationDuration, map[string]string{observability.KeyOperation: "migrate"})
	if !ok || d.Count != 1 {
		t.Fatalf("migrate duration = %+v, want 1 observation", d)
	}
}

// A failed Start is an error, and it never leaves an operation "in flight".
func TestObserver_AFailedStartIsAnError(t *testing.T) {
	t.Parallel()
	o := sqlObserver(t)
	bad := ecsql.NewSQLService("::not a dsn::", 1, 1, nil, false, nil, nil, nil, ecsql.WithObserver(o))
	if err := bad.Start(ctxT(t)); err == nil {
		t.Fatal("a malformed DSN started")
	}
	if got := sqlCount(o, "start", "error"); got != 1 {
		t.Fatalf("start error = %v, want 1", got)
	}
	if sm, ok := o.Find(observability.MetricInflight, map[string]string{observability.KeyOperation: "start"}); !ok || sm.Value != 0 {
		t.Fatalf("inflight = %+v, want 0", sm)
	}
	// Ping before Start is refused before any operation begins, so it is not counted.
	if err := bad.Ping(ctxT(t)); err == nil {
		t.Fatal("Ping before Start should fail")
	}
	if got := sqlCount(o, "ping", "error") + sqlCount(o, "ping", "success"); got != 0 {
		t.Fatalf("a refused Ping was counted %v times", got)
	}
}

// No observer, and the existing nil-heavy constructors, keep working.
func TestObserver_NoneIsFine(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s := e.service()
	ctx := ctxT(t)
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer s.Stop(context.Background())
	if err := s.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	var zero ecsql.SQLService
	if err := zero.Start(ctx); err == nil {
		t.Fatal("a zero service must return an error, not panic")
	}
}
