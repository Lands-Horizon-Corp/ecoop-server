package cache_test

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
)

func TestSadPath_RunFailures(t *testing.T) {
	requireServer(t)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	cases := map[string]struct {
		svc cache.CacheServices
		ctx context.Context
	}{
		"wrong password":  {cache.NewCacheService([]string{testAddr}, "wrong-"+testPassword, nil), ctxT(t)},
		"unreachable":     {cache.NewCacheService([]string{"127.0.0.1:1"}, testPassword, nil), ctxT(t)},
		"cancelled ctx":   {cache.NewCacheService([]string{testAddr}, testPassword, nil), cancelled},
		"garbage address": {cache.NewCacheService([]string{"not an address"}, testPassword, nil), ctxT(t)},
		"no addresses":    {cache.NewCacheService(nil, testPassword, nil), ctxT(t)},
		"empty address":   {cache.NewCacheService([]string{""}, testPassword, nil), ctxT(t)},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if err := c.svc.Run(c.ctx); err == nil {
				_ = c.svc.Stop(context.Background())
				t.Fatal("Run succeeded")
			}
			if c.svc.Client() != nil {
				t.Fatal("failed Run left a client behind")
			}
			if err := c.svc.Ping(ctxT(t)); !errors.Is(err, cache.ErrNotRunning) {
				t.Fatalf("Ping after failed Run = %v, want ErrNotRunning", err)
			}
		})
	}
}

func TestSadPath_FailedRunKeepsWorkingClient(t *testing.T) {
	// A re-Run that fails must not tear down the client already serving traffic.
	ctx := ctxT(t)
	svc := newRunning(t)
	dead, cancel := context.WithCancel(ctx)
	cancel()
	if err := svc.Run(dead); err == nil {
		t.Fatal("Run with cancelled ctx succeeded")
	}
	if err := svc.Ping(ctx); err != nil {
		t.Fatalf("existing client broken by failed Run: %v", err)
	}
}

func TestSadPath_GetMissing(t *testing.T) {
	svc := newRunning(t)
	v, err := svc.Get(ctxT(t), key(t))
	if !errors.Is(err, cache.ErrNotFound) || v != nil {
		t.Fatalf("Get missing = %q, %v; want nil, ErrNotFound", v, err)
	}
}

func TestSadPath_WrongType(t *testing.T) {
	ctx := ctxT(t)
	svc := newRunning(t)
	str, zset := key(t, "str"), key(t, "zset")
	_ = svc.Set(ctx, str, "text", time.Minute)
	_ = svc.ZAdd(ctx, zset, 1, "m")

	checks := map[string]error{}
	_, checks["Get on zset"] = svc.Get(ctx, zset)
	checks["ZAdd on string"] = svc.ZAdd(ctx, str, 1, "m")
	_, checks["ZRange on string"] = svc.ZRange(ctx, str, 0, -1)
	_, checks["ZCard on string"] = svc.ZCard(ctx, str)
	_, checks["ZRem on string"] = svc.ZRem(ctx, str, "m")
	_, checks["Incr on string"] = svc.Incr(ctx, str, 0)
	_, checks["Incr on zset"] = svc.Incr(ctx, zset, 0)
	for name, err := range checks {
		if err == nil {
			t.Errorf("%s: no error", name)
		} else if errors.Is(err, cache.ErrNotFound) {
			t.Errorf("%s: WRONGTYPE reported as ErrNotFound", name)
		}
	}
}

func TestSadPath_IncrOverflow(t *testing.T) {
	ctx := ctxT(t)
	svc := newRunning(t)
	k := key(t)
	_ = svc.Set(ctx, k, int64(math.MaxInt64), time.Minute)
	if _, err := svc.Incr(ctx, k, 0); err == nil {
		t.Fatal("overflow not reported")
	}
	if v, _ := svc.Get(ctx, k); string(v) != "9223372036854775807" {
		t.Fatalf("value changed on failed Incr: %q", v)
	}
}

func TestSadPath_BadScores(t *testing.T) {
	ctx := ctxT(t)
	svc := newRunning(t)
	k := key(t)
	_ = svc.ZAdd(ctx, k, 1, "m")

	if err := svc.ZAdd(ctx, k, math.NaN(), "nan"); err == nil {
		t.Error("NaN score accepted")
	}
	for _, b := range [][2]string{{"abc", "1"}, {"1", "xyz"}, {"((1", "2"}, {"1", "inf-"}} {
		if _, err := svc.ZRemRangeByScore(ctx, k, b[0], b[1]); err == nil {
			t.Errorf("ZRemRangeByScore(%q, %q) accepted", b[0], b[1])
		}
	}
	if n, _ := svc.ZCard(ctx, k); n != 1 {
		t.Fatalf("set changed by rejected commands: ZCard = %d", n)
	}
}

func TestSadPath_UnencodableValues(t *testing.T) {
	ctx := ctxT(t)
	svc := newRunning(t)
	k := key(t)
	for name, v := range map[string]any{
		"chan":    make(chan int),
		"func":    func() {},
		"complex": complex(1, 2),
	} {
		t.Run(name, func(t *testing.T) {
			if err := svc.Set(ctx, k, v, 0); err == nil {
				t.Error("Set accepted")
			}
			if _, err := svc.SetNX(ctx, k, v, 0); err == nil {
				t.Error("SetNX accepted")
			}
			if err := svc.ZAdd(ctx, k, 1, v); err == nil {
				t.Error("ZAdd accepted")
			}
			if _, err := svc.ZRem(ctx, k, "ok", v); err == nil {
				t.Error("ZRem accepted")
			}
		})
	}
	if ok, _ := svc.Exists(ctx, k); ok {
		t.Fatal("rejected value was written")
	}
}

func TestSadPath_DeadContexts(t *testing.T) {
	svc := newRunning(t)
	k := key(t)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancel2 := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel2()

	for name, ctx := range map[string]context.Context{"cancelled": cancelled, "expired": expired} {
		t.Run(name, func(t *testing.T) {
			errs := map[string]error{
				"Ping":   svc.Ping(ctx),
				"Set":    svc.Set(ctx, k, "v", 0),
				"Delete": svc.Delete(ctx, k),
			}
			_, errs["Get"] = svc.Get(ctx, k)
			_, errs["Incr"] = svc.Incr(ctx, k, time.Second)
			_, errs["Keys"] = svc.Keys(ctx, k)
			for op, err := range errs {
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("%s: err = %v, want context error", op, err)
				}
			}
		})
	}
	if ok, _ := svc.Exists(ctxT(t), k); ok {
		t.Fatal("write went through with a dead context")
	}
}
