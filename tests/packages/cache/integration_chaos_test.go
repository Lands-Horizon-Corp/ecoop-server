package cache_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
	valkey "github.com/valkey-io/valkey-go"
)

// Integration chaos: the patterns the app builds on the cache (rate limits,
// distributed locks, sliding-window sets) under multiple instances, network
// drops and restarts.

func TestIntegrationChaos_RateLimiterNeverExceedsLimit(t *testing.T) {
	p := newProxy(t)
	nodes := []interface {
		Incr(context.Context, string, time.Duration) (int64, error)
	}{newRunningAt(t, p.Addr()), newRunningAt(t, p.Addr()), newRunning(t)}
	k := key(t)
	const limit = 50

	var allowed atomic.Int64
	var wg sync.WaitGroup
	for i := range 300 {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			n, err := nodes[i%len(nodes)].Incr(ctx, k, time.Minute)
			if err == nil && n <= limit {
				allowed.Add(1)
			}
		})
		if i%60 == 0 {
			p.DropAll()
		}
	}
	wg.Wait()
	if got := allowed.Load(); got > limit {
		t.Fatalf("%d requests allowed, limit %d", got, limit)
	}
}

// unlockScript deletes the lock only if it still holds the caller's token.
// A plain DEL is unsafe: if the reply is lost and the DEL is retried, the
// retry can delete a lock another holder has acquired in the meantime.
var unlockScript = valkey.NewLuaScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
	return redis.call('DEL', KEYS[1])
end
return 0`)

func TestIntegrationChaos_DistributedLockMutualExclusion(t *testing.T) {
	p := newProxy(t)
	nodes := []cache.CacheServices{newRunning(t), newRunning(t), newRunningAt(t, p.Addr())}
	lock := key(t, "lock")

	var holders, maxHolders, acquired atomic.Int64
	var wg sync.WaitGroup
	stopDrops := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopDrops:
				return
			case <-time.After(20 * time.Millisecond):
				p.DropAll()
			}
		}
	}()

	for w := range 24 {
		wg.Go(func() {
			node := nodes[w%len(nodes)]
			for i := range 20 {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				token := fmt.Sprintf("%d-%d", w, i)
				ok, err := node.SetNX(ctx, lock, token, 5*time.Second)
				if err == nil && ok {
					acquired.Add(1)
					h := holders.Add(1)
					for {
						m := maxHolders.Load()
						if h <= m || maxHolders.CompareAndSwap(m, h) {
							break
						}
					}
					time.Sleep(time.Millisecond)
					holders.Add(-1)
					unlock := func() error {
						return unlockScript.Exec(ctx, node.Client(), []string{lock}, []string{token}).Error()
					}
					for unlock() != nil && ctx.Err() == nil {
						time.Sleep(5 * time.Millisecond)
					}
				}
				cancel()
			}
		})
	}
	wg.Wait()
	close(stopDrops)
	if m := maxHolders.Load(); m > 1 {
		t.Fatalf("%d holders at once; lock is not exclusive", m)
	}
	if acquired.Load() == 0 {
		t.Fatal("lock was never acquired")
	}
	t.Logf("lock acquired %d times", acquired.Load())
}

func TestIntegrationChaos_SlidingWindowAcrossRestarts(t *testing.T) {
	ctx := ctxT(t)
	svc := newRunning(t)
	k := key(t)
	r := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 1))

	now := float64(0)
	for round := range 10 {
		for range 20 {
			now++
			if err := svc.ZAdd(ctx, k, now, fmt.Sprint(now)); err != nil {
				t.Fatalf("round %d ZAdd: %v", round, err)
			}
		}
		// Trim everything older than the last 30 events.
		if _, err := svc.ZRemRangeByScore(ctx, k, "-inf", fmt.Sprintf("(%v", now-29)); err != nil {
			t.Fatal(err)
		}
		if n, _ := svc.ZCard(ctx, k); n != min(30, int64(now)) {
			t.Fatalf("round %d window size %d", round, n)
		}
		if r.IntN(2) == 0 {
			_ = svc.Stop(ctx)
			if err := svc.Run(ctx); err != nil {
				t.Fatal(err)
			}
		}
	}
	got, _ := svc.ZRange(ctx, k, 0, 0)
	if len(got) != 1 || got[0] != fmt.Sprint(now-29) {
		t.Fatalf("oldest in window = %v, want %v", got, now-29)
	}
}

func TestIntegrationChaos_CacheAsideUnderDrops(t *testing.T) {
	p := newProxy(t)
	svc := newRunningAt(t, p.Addr())
	var loads atomic.Int64
	load := func(id int) string { loads.Add(1); return fmt.Sprintf("member-%d", id) }

	get := func(ctx context.Context, id int) (string, error) {
		k := key(t, fmt.Sprint(id))
		if v, err := svc.Get(ctx, k); err == nil {
			return string(v), nil
		}
		v := load(id)
		_ = svc.Set(ctx, k, v, time.Minute) // best effort
		return v, nil
	}

	var wg sync.WaitGroup
	for i := range 400 {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			id := i % 10
			if v, _ := get(ctx, id); v != fmt.Sprintf("member-%d", id) {
				t.Errorf("id %d got %q", id, v)
			}
		})
		if i%100 == 50 {
			p.DropAll()
		}
	}
	wg.Wait()
	t.Logf("source loaded %d times for 400 reads of 10 ids", loads.Load())
}
