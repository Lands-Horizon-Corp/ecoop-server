//go:build integration

package cache_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
)

// Sync & race: concurrent use of one service and of the server's atomic
// commands. Run with `go test -race`.

const workers = 32

func TestRace_IncrIsAtomic(t *testing.T) {
	t.Parallel()
	ctx := ctxT(t)
	svc := newRunning(t)
	k := key(t)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for range 100 {
				if _, err := svc.Incr(ctx, k, time.Minute); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	if n, _ := svc.Incr(ctx, k, 0); n != workers*100+1 {
		t.Fatalf("counter = %d, want %d", n-1, workers*100)
	}
}

func TestRace_SetNXHasOneWinner(t *testing.T) {
	t.Parallel()
	ctx := ctxT(t)
	svc := newRunning(t)
	for round := range 20 {
		k := key(t, fmt.Sprint(round))
		var winners atomic.Int64
		start := make(chan struct{})
		var wg sync.WaitGroup
		for w := range workers {
			wg.Go(func() {
				<-start
				if ok, err := svc.SetNX(ctx, k, w, time.Minute); err != nil {
					t.Error(err)
				} else if ok {
					winners.Add(1)
				}
			})
		}
		close(start)
		wg.Wait()
		if n := winners.Load(); n != 1 {
			t.Fatalf("round %d: %d winners", round, n)
		}
	}
}

func TestRace_ConcurrentZAddDistinctMembers(t *testing.T) {
	t.Parallel()
	ctx := ctxT(t)
	svc := newRunning(t)
	k := key(t)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for i := range 50 {
				_ = svc.ZAdd(ctx, k, float64(i), fmt.Sprintf("%d-%d", w, i))
			}
		})
	}
	wg.Wait()
	if n, _ := svc.ZCard(ctx, k); n != workers*50 {
		t.Fatalf("ZCard = %d, want %d", n, workers*50)
	}
}

func TestRace_StopDuringTraffic(t *testing.T) {
	t.Parallel()
	requireServer(t)
	svc := cache.NewCacheService([]string{testAddr}, testPassword, nil)
	ctx := context.Background()
	if err := svc.Run(ctx); err != nil {
		t.Fatal(err)
	}
	k := key(t)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for range 200 {
				mustNotPanic(t, "op during Stop", func() {
					if _, err := svc.Incr(ctx, k, time.Minute); err != nil && !isShutdownErr(err) {
						t.Errorf("unexpected error: %v", err)
					}
				})
			}
		})
	}
	time.Sleep(5 * time.Millisecond)
	if err := svc.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}

func TestRace_LifecycleChurnWithTraffic(t *testing.T) {
	t.Parallel()
	requireServer(t)
	svc := cache.NewCacheService([]string{testAddr}, testPassword, nil)
	ctx := context.Background()
	if err := svc.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer svc.Stop(ctx)
	k := key(t)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				mustNotPanic(t, "op during churn", func() {
					_ = svc.Client()
					if err := svc.Set(ctx, k, "v", time.Minute); err != nil && !isShutdownErr(err) {
						t.Errorf("Set: %v", err)
					}
					if _, err := svc.Get(ctx, k); err != nil && !isShutdownErr(err) && !errors.Is(err, cache.ErrNotFound) {
						t.Errorf("Get: %v", err)
					}
				})
			}
		})
	}
	for i := range 20 {
		if i%2 == 0 {
			_ = svc.Stop(ctx)
		} else if err := svc.Run(ctx); err != nil {
			t.Error(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
}

func TestRace_ConcurrentRunCallsLeaveOneClient(t *testing.T) {
	t.Parallel()
	requireServer(t)
	svc := cache.NewCacheService([]string{testAddr}, testPassword, nil)
	ctx := context.Background()
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if err := svc.Run(ctx); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	defer svc.Stop(ctx)
	if err := svc.Ping(ctx); err != nil {
		t.Fatalf("service unusable after concurrent Run: %v", err)
	}
}

// Stop must wait for operations already in flight: they complete
// successfully, Stop returns only after them, and later calls are refused.
func TestRace_StopWaitsForInFlightCommands(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	p := newProxy(t)
	svc := cache.NewCacheService([]string{p.Addr()}, testPassword, nil)
	if err := svc.Run(ctx); err != nil {
		t.Fatal(err)
	}
	p.SetDelay(300 * time.Millisecond)

	const n = 8
	type result struct {
		err error
		at  time.Time
	}
	results := make(chan result, n)
	for range n {
		go func() {
			err := svc.Ping(ctx)
			results <- result{err, time.Now()}
		}()
	}
	time.Sleep(30 * time.Millisecond)

	if err := svc.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	stopped := time.Now()

	for range n {
		r := <-results
		if r.err != nil {
			t.Errorf("in-flight Ping was cut off by Stop: %v", r.err)
		} else if r.at.After(stopped) {
			t.Errorf("Stop returned %v before an in-flight Ping finished", r.at.Sub(stopped))
		}
	}
	if err := svc.Ping(ctx); !errors.Is(err, cache.ErrNotRunning) {
		t.Fatalf("call after Stop = %v, want ErrNotRunning", err)
	}
}

func TestRace_StopGivesUpWhenCtxEnds(t *testing.T) {
	t.Parallel()
	p := newProxy(t)
	svc := cache.NewCacheService([]string{p.Addr()}, testPassword, nil)
	if err := svc.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	p.SetDelay(5 * time.Second) // much longer than the limit below

	inflight := make(chan error, 1)
	go func() { inflight <- svc.Ping(context.Background()) }()
	time.Sleep(30 * time.Millisecond)

	stopCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := svc.Stop(stopCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop = %v, want DeadlineExceeded", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Stop ignored its deadline and took %v", d)
	}
	if err := <-inflight; err == nil {
		t.Log("in-flight Ping finished anyway")
	}
}
