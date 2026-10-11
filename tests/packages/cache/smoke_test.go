package cache_test

import (
	"context"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
)

// Smoke: the lifecycle and one basic round trip.
func TestSmoke(t *testing.T) {
	requireServer(t)
	ctx := ctxT(t)
	svc := cache.NewCacheService(testPassword, []string{testAddr}, nil)
	if err := svc.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if err := svc.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	k := key(t)
	if err := svc.Set(ctx, k, "hello", time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if v, err := svc.Get(ctx, k); err != nil || string(v) != "hello" {
		t.Fatalf("Get = %q, %v", v, err)
	}
	if err := svc.Delete(ctx, k); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := svc.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
