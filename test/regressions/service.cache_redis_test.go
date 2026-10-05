package regressions

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/cache"
	"github.com/redis/go-redis/v9"
)

// TestCacheService_Docker runs the cache contract against the real Redis in docker-compose.yml.
//
//	make test-up
//	go test ./test/regressions/ -run TestCacheService_Docker -v
//
// REDIS_TEST_URL (default redis://localhost:6379/0) and REDIS_TEST_PASSWORD override the target.
func TestCacheService_Docker(t *testing.T) {
	url := envOr("REDIS_TEST_URL", "redis://localhost:6379/0")
	opt, err := redis.ParseURL(url)
	if err != nil {
		t.Fatalf("REDIS_TEST_URL: %v", err)
	}
	requireReachable(t, opt.Addr)

	factory := func(t *testing.T, prefix string) cache.CacheServices {
		t.Helper()
		svc := cache.NewCacheService([]string{url}, envOr("REDIS_TEST_PASSWORD", ""), "", prefix)
		if err := svc.Run(context.Background()); err != nil {
			t.Fatalf("run against %s: %v", url, err)
		}
		t.Cleanup(func() { _ = svc.Stop(context.Background()) })
		return svc
	}

	runCacheContract(t, factory)
	runCacheExtras(t, factory)

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

	t.Run("StopThenRunAgainRestoresAccess", func(t *testing.T) {
		ctx := context.Background()
		svc := newIsolatedCache(t, factory)
		if err := svc.Set(ctx, "k", "v", time.Minute); err != nil {
			t.Fatalf("set: %v", err)
		}
		if err := svc.Stop(ctx); err != nil {
			t.Fatalf("stop: %v", err)
		}
		if err := svc.Run(ctx); err != nil {
			t.Fatalf("second run: %v", err)
		}
		if got, err := svc.Get(ctx, "k"); err != nil || string(got) != "v" {
			t.Fatalf("Get after restart = %q, %v; want v", got, err)
		}
	})
}
