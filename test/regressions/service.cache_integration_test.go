package regressions

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/cache"
)

// TestCacheService_Integration runs the cache contract against a real Redis.
//
// It is skipped unless REDIS_TEST_URL is set. Locally:
//
//	docker compose up -d redis
//	REDIS_TEST_URL=redis://localhost:6379/0 go test ./test/regressions/ -run TestCacheService_Integration -v
func TestCacheService_Integration(t *testing.T) {
	url := os.Getenv("REDIS_TEST_URL")
	if url == "" {
		t.Skip("REDIS_TEST_URL not set; skipping real Redis integration tests")
	}

	factory := func(t *testing.T, prefix string) cache.CacheServices {
		t.Helper()
		svc := cache.NewCacheService([]string{url}, os.Getenv("REDIS_TEST_PASSWORD"), "", prefix)
		if err := svc.Run(context.Background()); err != nil {
			t.Fatalf("run against %s: %v", url, err)
		}
		t.Cleanup(func() { _ = svc.Stop(context.Background()) })
		return svc
	}

	runCacheContract(t, factory)

	t.Run("TTLExpiresInRealTime", func(t *testing.T) {
		ctx := context.Background()
		svc := newIsolatedCache(t, factory)
		if err := svc.Set(ctx, "short", "v", 100*time.Millisecond); err != nil {
			t.Fatalf("set: %v", err)
		}
		deadline := time.Now().Add(3 * time.Second)
		for {
			_, err := svc.Get(ctx, "short")
			if errors.Is(err, cache.ErrNotFound) {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("key still present 3s after a 100ms TTL (last err %v)", err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	})

	t.Run("SubSecondIncrWindowExpires", func(t *testing.T) {
		ctx := context.Background()
		svc := newIsolatedCache(t, factory)
		if _, err := svc.Incr(ctx, "burst", 100*time.Millisecond); err != nil {
			t.Fatalf("incr: %v", err)
		}
		time.Sleep(300 * time.Millisecond)
		n, err := svc.Incr(ctx, "burst", 100*time.Millisecond)
		if err != nil || n != 1 {
			t.Fatalf("incr after sub-second window = %d, %v; want 1", n, err)
		}
	})
}
