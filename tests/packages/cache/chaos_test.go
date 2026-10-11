package cache_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
)

// Chaos: dropped connections, outages, random cancellation and random
// operation sequences checked against an in-memory model.

func chaosRand(t *testing.T) *rand.Rand {
	t.Helper()
	seed := uint64(time.Now().UnixNano())
	t.Logf("chaos seed: %d", seed)
	return rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
}

// isTimeout accepts both context errors and dial/read timeouts, since a short
// deadline can expire while the client is reconnecting.
func isTimeout(err error) bool {
	var ne net.Error
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		(errors.As(err, &ne) && ne.Timeout())
}

func TestChaos_ConnectionDropsMidTraffic(t *testing.T) {
	p := newProxy(t)
	svc := newRunningAt(t, p.Addr())
	ctx := ctxT(t)
	k := key(t)

	var wg sync.WaitGroup
	var ok, failed atomic.Int64
	stop := make(chan struct{})
	for range 16 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				mustNotPanic(t, "Incr", func() {
					opCtx, cancel := context.WithTimeout(ctx, time.Second)
					defer cancel()
					if _, err := svc.Incr(opCtx, k, time.Minute); err != nil {
						failed.Add(1)
					} else {
						ok.Add(1)
					}
				})
			}
		})
	}
	for range 5 {
		time.Sleep(30 * time.Millisecond)
		p.DropAll()
	}
	close(stop)
	wg.Wait()
	t.Logf("succeeded %d, failed %d across 5 drops", ok.Load(), failed.Load())

	eventually(t, 5*time.Second, func() error { return svc.Ping(ctx) })
	n, err := svc.Incr(ctx, k, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Every acknowledged increment happened; dropped ones may or may not have.
	if n-1 < ok.Load() {
		t.Fatalf("counter %d lower than %d acknowledged increments; writes were lost", n-1, ok.Load())
	}
}

func TestChaos_OutageAndRecovery(t *testing.T) {
	p := newProxy(t)
	svc := newRunningAt(t, p.Addr())
	ctx := ctxT(t)
	k := key(t)
	_ = svc.Set(ctx, k, "before", time.Minute)

	p.SetDown(true)
	start := time.Now()
	opCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	_, err := svc.Get(opCtx, k)
	cancel()
	if err == nil {
		t.Fatal("Get succeeded during outage")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("call during outage ignored its 500ms deadline (took %v)", d)
	}

	p.SetDown(false)
	eventually(t, 5*time.Second, func() error { return svc.Ping(ctx) })
	if v, err := svc.Get(ctx, k); err != nil || string(v) != "before" {
		t.Fatalf("after recovery Get = %q, %v", v, err)
	}
}

func TestChaos_RandomCancellation(t *testing.T) {
	svc := newRunning(t)
	r := chaosRand(t)
	k := key(t)
	var acked int64
	for range 300 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(r.IntN(2000))*time.Microsecond)
		if _, err := svc.Incr(ctx, k, 0); err == nil {
			acked++
		} else if !isTimeout(err) {
			t.Fatalf("unexpected error: %v", err)
		}
		cancel()
	}
	v, err := svc.Get(ctxT(t), k)
	if err != nil && !errors.Is(err, cache.ErrNotFound) {
		t.Fatal(err)
	}
	n, _ := strconv.ParseInt(string(v), 10, 64)
	if n < acked || n > 300 {
		t.Fatalf("counter %d outside [%d acked, 300 attempted]", n, acked)
	}
}

func TestChaos_RandomOpsMatchModel(t *testing.T) {
	ctx := ctxT(t)
	svc := newRunning(t)
	r := chaosRand(t)
	model := map[string]string{}
	keys := make([]string, 8)
	for i := range keys {
		keys[i] = key(t, strconv.Itoa(i))
	}

	for step := range 2000 {
		k := keys[r.IntN(len(keys))]
		switch op := r.IntN(6); op {
		case 0:
			v := fmt.Sprint(r.IntN(1000))
			if err := svc.Set(ctx, k, v, 0); err != nil {
				t.Fatalf("step %d Set: %v", step, err)
			}
			model[k] = v
		case 1:
			got, err := svc.Get(ctx, k)
			want, exists := model[k]
			if exists != (err == nil) || (exists && string(got) != want) {
				t.Fatalf("step %d Get(%s) = %q, %v; model %q, %v", step, k, got, err, want, exists)
			}
		case 2:
			_ = svc.Delete(ctx, k)
			delete(model, k)
		case 3:
			ok, _ := svc.Exists(ctx, k)
			if _, exists := model[k]; ok != exists {
				t.Fatalf("step %d Exists(%s) = %v, model %v", step, k, ok, exists)
			}
		case 4:
			v := fmt.Sprint(r.IntN(1000))
			ok, _ := svc.SetNX(ctx, k, v, 0)
			if _, exists := model[k]; ok == exists {
				t.Fatalf("step %d SetNX(%s) = %v but model exists = %v", step, k, ok, exists)
			}
			if ok {
				model[k] = v
			}
		case 5:
			n, err := svc.Incr(ctx, k, 0)
			cur, exists := model[k]
			base, perr := strconv.ParseInt(cur, 10, 64)
			if exists && perr != nil {
				if err == nil {
					t.Fatalf("step %d Incr on %q succeeded", step, cur)
				}
				continue
			}
			if !exists {
				base = 0
			}
			if err != nil || n != base+1 {
				t.Fatalf("step %d Incr(%s) = %d, %v; want %d", step, k, n, err, base+1)
			}
			model[k] = strconv.FormatInt(n, 10)
		}
	}
}
