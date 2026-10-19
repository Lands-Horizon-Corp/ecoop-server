//go:build integration

package cache_test

import (
	"context"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/tests/internal/obstest"
)

func observed(t testing.TB) (cache.ICacheService, *observability.Service) {
	t.Helper()
	requireServer(t)
	o := obstest.New(t)
	svc := cache.NewCacheService([]string{testAddr}, testPassword, nil, cache.WithObserver(o))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := svc.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Cleanup(func() { _ = svc.Stop(context.Background()) })
	return svc, o
}

func count(o *observability.Service, op, outcome string) float64 {
	return obstest.Count(o, "cache", op, outcome)
}

// Every call is counted under its own operation name.
func TestObserver_CountsEachOperation(t *testing.T) {
	svc, o := observed(t)
	ctx := context.Background()
	k := key(t, "a")
	if err := svc.Set(ctx, k, "v", time.Minute); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, err := svc.Get(ctx, k); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := svc.Exists(ctx, k); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Delete(ctx, k); err != nil {
		t.Fatal(err)
	}
	for op, want := range map[string]float64{"set": 1, "get": 3, "exists": 1, "delete": 1} {
		if got := count(o, op, "success"); got != want {
			t.Errorf("%s success = %v, want %v", op, got, want)
		}
	}
	d, ok := o.Find(observability.MetricOperationDuration, map[string]string{observability.KeyOperation: "get"})
	if !ok || d.Count != 3 {
		t.Fatalf("get duration = %+v, want 3 observations", d)
	}
}

// A missing key is an answer, not a failure: it must not raise the error rate.
func TestObserver_AMissIsNotAnError(t *testing.T) {
	svc, o := observed(t)
	if _, err := svc.Get(context.Background(), key(t, "absent")); err == nil {
		t.Fatal("expected ErrNotFound")
	}
	if got := count(o, "get", "skipped"); got != 1 {
		t.Fatalf("a miss counted as skipped %v times, want 1", got)
	}
	if got := count(o, "get", "error"); got != 0 {
		t.Fatalf("a miss counted as %v errors", got)
	}
}

// Losing the SetNX race is an answer too; a real failure is an error.
func TestObserver_SetNXAndRealErrors(t *testing.T) {
	svc, o := observed(t)
	ctx := context.Background()
	k := key(t, "lock")
	if ok, err := svc.SetNX(ctx, k, "1", time.Minute); err != nil || !ok {
		t.Fatalf("first SetNX = %v, %v", ok, err)
	}
	if ok, err := svc.SetNX(ctx, k, "2", time.Minute); err != nil || ok {
		t.Fatalf("second SetNX = %v, %v", ok, err)
	}
	if got := count(o, "setnx", "success"); got != 1 {
		t.Errorf("setnx success = %v, want 1", got)
	}
	if got := count(o, "setnx", "skipped"); got != 1 {
		t.Errorf("setnx skipped = %v, want 1", got)
	}
	// A call on a service that is not running is a real error.
	idle := cache.NewCacheService([]string{testAddr}, testPassword, nil, cache.WithObserver(o))
	if err := idle.Ping(context.Background()); err == nil {
		t.Fatal("Ping on a service that never ran should fail")
	}
	if got := count(o, "ping", "error"); got != 1 {
		t.Fatalf("ping error = %v, want 1", got)
	}
}

// The key is never a label: a thousand keys make one series.
func TestObserver_KeysNeverBecomeLabels(t *testing.T) {
	svc, o := observed(t)
	ctx := context.Background()
	for i := range 40 {
		_ = svc.Set(ctx, key(t, "k", string(rune('a'+i%26)), string(rune('a'+i/26))), "v", time.Minute)
	}
	n := 0
	for _, sm := range o.Snapshot() {
		if sm.Name == observability.MetricOperations && sm.Attrs[observability.KeyOperation] == "set" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("%d series for 40 different keys, want 1", n)
	}
}

// A service with no observer, and a zero-value struct, keep working.
func TestObserver_NoneIsFine(t *testing.T) {
	svc := newRunning(t)
	ctx := context.Background()
	k := key(t, "plain")
	if err := svc.Set(ctx, k, "v", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Get(ctx, k); err != nil {
		t.Fatal(err)
	}
	var zero cache.CacheService
	if err := zero.Ping(ctx); err == nil {
		t.Fatal("a zero service must return an error, not panic")
	}
}
