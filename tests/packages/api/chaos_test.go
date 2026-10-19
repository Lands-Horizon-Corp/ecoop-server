package api_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/api"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
)

// The shared rate limit lives in the cache. These tests break the cache the way
// production does (refuses, hangs, comes back) and check that the API stays up,
// keeps its local protection, and says nothing about what failed.

// flakyCache implements only Incr, the one call the limiter makes; any other call
// panics on the nil embedded interface, which would show as a test failure.
type flakyCache struct {
	cache.ICacheService
	mu     sync.Mutex
	counts map[string]int64
	down   atomic.Bool
	hung   atomic.Bool
	calls  atomic.Int64
}

func (c *flakyCache) Incr(ctx context.Context, key string, _ time.Duration) (int64, error) {
	c.calls.Add(1)
	if c.hung.Load() {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(5 * time.Second):
			return 0, errors.New("i/o timeout")
		}
	}
	if c.down.Load() {
		return 0, errors.New("dial tcp 10.9.9.9:6379: connect: connection refused")
	}
	// The key ends in the one-second slot; count per client, so a test is not
	// sensitive to the clock ticking over.
	base := key[:strings.LastIndex(key, ":")]
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.counts == nil {
		c.counts = map[string]int64{}
	}
	c.counts[base]++
	return c.counts[base], nil
}

func withCache(t testing.TB, cfg api.Config, c cache.ICacheService) *server.Hertz {
	t.Helper()
	svc := api.NewAPIService(0, c, nil, api.WithConfig(cfg)).(*api.APIService)
	mustRegister(t, svc.RegisterRoute(api.Route{Route: "/members/:id", Method: "GET"}, func(_ context.Context, c *app.RequestContext) error {
		c.String(200, "ok")
		return nil
	}))
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return svc.Handler()
}

func countCodes(h *server.Hertz, n int) (ok, limited, other int) {
	for range n {
		switch code, _, _ := as(h, peerIP, "GET", "/members/1", nil); code {
		case 200:
			ok++
		case 429:
			limited++
		default:
			other++
		}
	}
	return
}

// Two replicas that share a cache share one limit: a client cannot double its
// budget by landing on both.
func TestChaos_SharedLimitHoldsAcrossReplicas(t *testing.T) {
	t.Parallel()
	shared := &flakyCache{}
	cfg := api.Config{RateLimit: 1000, RateBurst: 5}
	a, b := withCache(t, cfg, shared), withCache(t, cfg, shared)
	okA, limA, _ := countCodes(a, 4)
	okB, limB, _ := countCodes(b, 4)
	if ok := okA + okB; ok != 5 {
		t.Errorf("two replicas let %d requests through, want the shared burst of 5 (A: %d/%d, B: %d/%d)", ok, okA, limA, okB, limB)
	}
	if limA+limB != 3 {
		t.Errorf("%d were limited, want 3", limA+limB)
	}
}

// With the cache refusing connections the API keeps serving and keeps its local
// limit; the client learns nothing about the cache.
func TestChaos_CacheDownKeepsTheLocalLimitAndLeaksNothing(t *testing.T) {
	t.Parallel()
	down := &flakyCache{}
	down.down.Store(true)
	h := withCache(t, api.Config{RateLimit: 0.001, RateBurst: 3}, down)
	var bodies strings.Builder
	var ok, limited int
	for range 10 {
		code, hd, body := as(h, peerIP, "GET", "/members/1", nil)
		bodies.WriteString(body)
		for _, vs := range hd {
			bodies.WriteString(strings.Join(vs, ","))
		}
		switch code {
		case 200:
			ok++
		case 429:
			limited++
		default:
			t.Errorf("a cache outage turned a request into %d", code)
		}
	}
	if ok != 3 || limited != 7 {
		t.Errorf("with the cache down: %d passed, %d limited; the local limit should still hold (3 and 7)", ok, limited)
	}
	for _, leak := range []string{"10.9.9.9", "6379", "refused", "dial", "tcp"} {
		if strings.Contains(bodies.String(), leak) {
			t.Errorf("the cache failure leaked %q to the client", leak)
		}
	}
	if down.calls.Load() == 0 {
		t.Error("the cache was never asked")
	}
}

// A cache that hangs must not hang the API: the shared check is bounded, and a
// request that cannot be counted there is still served under the local limit.
func TestChaos_HungCacheCannotHangRequests(t *testing.T) {
	t.Parallel()
	hung := &flakyCache{}
	hung.hung.Store(true)
	h := withCache(t, api.Config{RateLimit: 1000, RateBurst: 1000}, hung)
	start := time.Now()
	code, _, _ := as(h, peerIP, "GET", "/members/1", nil)
	if took := time.Since(start); took > time.Second {
		t.Fatalf("a request waited %v for a hung cache; the wait must be bounded well under a second", took)
	}
	if code != 200 {
		t.Errorf("a hung cache turned a request into %d, want it served under the local limit", code)
	}
}

// When the cache comes back the shared limit applies again, with no restart.
func TestChaos_CacheRecoversWithoutARestart(t *testing.T) {
	t.Parallel()
	c := &flakyCache{}
	c.down.Store(true)
	// The local bucket refills a million tokens a second, so it never limits here
	// and only the shared window (a burst of 3 per second) can.
	h := withCache(t, api.Config{RateLimit: 1e6, RateBurst: 3}, c)
	if ok, _, _ := countCodes(h, 5); ok != 5 {
		t.Fatalf("with the cache down only the local bucket applies, which is generous here; %d of 5 passed", ok)
	}
	c.down.Store(false)
	ok, limited, _ := countCodes(h, 6)
	if ok != 3 || limited != 3 {
		t.Errorf("after recovery %d passed and %d were limited; the shared burst of 3 should apply (3 and 3)", ok, limited)
	}
}
