package regressions

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/cache"
	"github.com/redis/go-redis/v9"
)

// sentinelTarget describes the Sentinel stack in docker-compose.yml.
type sentinelTarget struct {
	addrs      []string
	master     string
	masterAddr string // reachable from the host, for inspecting data directly
	password   string
}

func dockerSentinel(t *testing.T) sentinelTarget {
	t.Helper()
	s := sentinelTarget{
		addrs:      strings.Split(envOr("REDIS_SENTINEL_ADDRS", "localhost:26379"), ","),
		master:     envOr("REDIS_SENTINEL_MASTER", "mymaster"),
		masterAddr: envOr("REDIS_SENTINEL_MASTER_ADDR", "localhost:6380"),
		password:   envOr("REDIS_SENTINEL_MASTER_PASSWORD", "sentinel-test-pass"),
	}
	for _, a := range s.addrs {
		requireReachable(t, a)
	}
	requireReachable(t, s.masterAddr)
	return s
}

func (s sentinelTarget) service(prefix string) cache.CacheServices {
	return cache.NewSentinelCacheService(
		s.addrs, s.master, s.password, envOr("REDIS_SENTINEL_PASSWORD", ""), prefix,
	)
}

// TestCacheSentinel_Docker runs the cache contract through the real Sentinel in docker-compose.yml.
//
//	make test-up
//	go test ./test/regressions/ -run TestCacheSentinel_Docker -v
func TestCacheSentinel_Docker(t *testing.T) {
	s := dockerSentinel(t)

	factory := func(t *testing.T, prefix string) cache.CacheServices {
		t.Helper()
		svc := s.service(prefix)
		if err := svc.Run(context.Background()); err != nil {
			t.Fatalf("run against sentinel %v (master %q): %v", s.addrs, s.master, err)
		}
		t.Cleanup(func() { _ = svc.Stop(context.Background()) })
		return svc
	}

	runCacheContract(t, factory)
	runCacheExtras(t, factory)

	t.Run("WritesLandOnTheMonitoredMaster", func(t *testing.T) {
		ctx := context.Background()
		svc := newIsolatedCache(t, factory)
		if err := svc.Set(ctx, "where", "master", time.Minute); err != nil {
			t.Fatalf("set: %v", err)
		}
		keys, err := svc.Keys(ctx, "where")
		if err != nil || len(keys) != 1 {
			t.Fatalf("Keys = %v, %v; want [where]", keys, err)
		}

		// Read it back straight from the master, bypassing Sentinel.
		direct := redis.NewClient(&redis.Options{Addr: s.masterAddr, Password: s.password})
		defer direct.Close()
		var found int
		var cursor uint64
		for {
			page, next, err := direct.Scan(ctx, cursor, "*:where", 100).Result()
			if err != nil {
				t.Fatalf("direct scan: %v", err)
			}
			found += len(page)
			if next == 0 {
				break
			}
			cursor = next
		}
		if found != 1 {
			t.Fatalf("found %d matching keys on the master; want 1", found)
		}
	})

	t.Run("MasterPasswordIsEnforced", func(t *testing.T) {
		for name, password := range map[string]string{"wrong": "nope", "missing": ""} {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			svc := cache.NewSentinelCacheService(s.addrs, s.master, password, "", "")
			if err := svc.Run(ctx); err == nil {
				_ = svc.Stop(ctx)
				cancel()
				t.Fatalf("Run with a %s master password succeeded; want error", name)
			}
			cancel()
		}
	})

	t.Run("RunRejectsBadConfig", func(t *testing.T) {
		cases := map[string]cache.CacheServices{
			"empty master name": cache.NewSentinelCacheService(s.addrs, "", s.password, "", ""),
			"unknown master":    cache.NewSentinelCacheService(s.addrs, "no-such-master", s.password, "", ""),
			"no live sentinel":  cache.NewSentinelCacheService([]string{closedAddr(t)}, s.master, s.password, "", ""),
		}
		for name, svc := range cases {
			// go-redis retries a dead Sentinel for ~30s without a deadline.
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			if err := svc.Run(ctx); err == nil {
				_ = svc.Stop(ctx)
				cancel()
				t.Errorf("%s: Run succeeded; want error", name)
				continue
			}
			cancel()
		}
	})

	t.Run("FallsBackToNextSentinel", func(t *testing.T) {
		ctx := context.Background()
		svc := cache.NewSentinelCacheService(
			append([]string{closedAddr(t)}, s.addrs...), s.master, s.password, "", uniquePrefix(t),
		)
		if err := svc.Run(ctx); err != nil {
			t.Fatalf("Run with a dead Sentinel listed first: %v", err)
		}
		defer func() { _ = svc.Stop(ctx) }()
		defer func() { _ = svc.Flush(ctx) }()
		if err := svc.Ping(ctx); err != nil {
			t.Fatalf("Ping: %v", err)
		}
	})
}
