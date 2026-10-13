//go:build integration

package cache_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
	valkey "github.com/valkey-io/valkey-go"
)

// Zero value & absence: unstarted/stopped services, nil and empty values,
// zero/negative/sub-millisecond TTLs, nil receivers and stale client handles.

type op struct {
	name string
	call func(cache.ICacheService) error
}

func allOps(ctx context.Context, k string) []op {
	e := func(_ any, err error) error { return err }
	return []op{
		{"Ping", func(s cache.ICacheService) error { return s.Ping(ctx) }},
		{"Flush", func(s cache.ICacheService) error { return s.Flush(ctx) }},
		{"Get", func(s cache.ICacheService) error { return e(s.Get(ctx, k)) }},
		{"Set", func(s cache.ICacheService) error { return s.Set(ctx, k, "v", 0) }},
		{"SetNX", func(s cache.ICacheService) error { return e(s.SetNX(ctx, k, "v", 0)) }},
		{"Exists", func(s cache.ICacheService) error { return e(s.Exists(ctx, k)) }},
		{"Delete", func(s cache.ICacheService) error { return e(s.Delete(ctx, k)) }},
		{"Keys", func(s cache.ICacheService) error { return e(s.Keys(ctx, k)) }},
		{"ZAdd", func(s cache.ICacheService) error { return s.ZAdd(ctx, k, 1, "m") }},
		{"ZRange", func(s cache.ICacheService) error { return e(s.ZRange(ctx, k, 0, -1)) }},
		{"ZCard", func(s cache.ICacheService) error { return e(s.ZCard(ctx, k)) }},
		{"ZRem", func(s cache.ICacheService) error { return e(s.ZRem(ctx, k, "m")) }},
		{"ZRemRangeByScore", func(s cache.ICacheService) error { return e(s.ZRemRangeByScore(ctx, k, "-inf", "+inf")) }},
		{"Incr", func(s cache.ICacheService) error { return e(s.Incr(ctx, k, time.Second)) }},
		{"Expire", func(s cache.ICacheService) error { return e(s.Expire(ctx, k, time.Second)) }},
	}
}

func TestZeroValue_NotRunningReturnsErrNotRunning(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	services := map[string]cache.ICacheService{
		"constructed only": cache.NewCacheService([]string{"127.0.0.1:1"}, "pw", nil),
		"zero struct":      &cache.CacheService{},
	}
	for name, svc := range services {
		if svc.Client() != nil {
			t.Errorf("%s: Client() non-nil before Run", name)
		}
		for _, o := range allOps(ctx, "k") {
			t.Run(name+"/"+o.name, func(t *testing.T) {
				var err error
				mustNotPanic(t, o.name, func() { err = o.call(svc) })
				if !errors.Is(err, cache.ErrNotRunning) {
					t.Fatalf("err = %v, want ErrNotRunning", err)
				}
			})
		}
	}
}

func TestZeroValue_StoppedReturnsErrNotRunning(t *testing.T) {
	t.Parallel()
	svc := newRunning(t)
	ctx := context.Background()
	_ = svc.Stop(ctx)
	if svc.Client() != nil {
		t.Fatal("Client() non-nil after Stop")
	}
	for _, o := range allOps(ctx, key(t)) {
		var err error
		mustNotPanic(t, o.name, func() { err = o.call(svc) })
		if !errors.Is(err, cache.ErrNotRunning) {
			t.Errorf("%s after Stop: err = %v, want ErrNotRunning", o.name, err)
		}
	}
}

func TestZeroValue_StopIsIdempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for name, svc := range map[string]cache.ICacheService{
		"never run":   cache.NewCacheService(nil, "", nil),
		"zero struct": &cache.CacheService{},
	} {
		mustNotPanic(t, name+" Stop", func() {
			for range 3 {
				if err := svc.Stop(ctx); err != nil {
					t.Fatalf("%s: Stop = %v", name, err)
				}
			}
		})
	}
}

func TestZeroValue_ConfigIsACopy(t *testing.T) {
	t.Parallel()
	addrs := []string{"127.0.0.1:1"}
	svc := cache.NewCacheService(addrs, "pw", nil)
	cfg := svc.Config()
	if len(cfg.Addresses) != 1 || cfg.Addresses[0] != "127.0.0.1:1" || cfg.Password != "pw" {
		t.Fatalf("Config = %+v", cfg)
	}
	cfg.Addresses[0] = "changed"
	if svc.Config().Addresses[0] != "127.0.0.1:1" {
		t.Fatal("mutating the returned Config changed the cache's settings")
	}
	if got := (&cache.CacheService{}).Config(); len(got.Addresses) != 0 || got.Password != "" {
		t.Fatalf("zero struct Config = %+v", got)
	}
}

func TestZeroValue_NoAddresses(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for name, svc := range map[string]cache.ICacheService{
		"nil slice":   cache.NewCacheService(nil, "", nil),
		"empty slice": cache.NewCacheService([]string{}, "", nil),
		"zero struct": &cache.CacheService{},
	} {
		if err := svc.Run(ctx); !errors.Is(err, cache.ErrNoAddresses) {
			t.Errorf("%s: Run = %v, want ErrNoAddresses", name, err)
		}
	}
}

func TestZeroValue_NilValues(t *testing.T) {
	t.Parallel()
	ctx := ctxT(t)
	svc := newRunning(t)
	var typedNil *struct{ A int }
	var nilMap map[string]int

	for name, v := range map[string]any{"untyped nil": nil, "nil pointer": typedNil, "nil map": nilMap} {
		t.Run(name, func(t *testing.T) {
			k := key(t)
			if err := svc.Set(ctx, k, v, 0); !errors.Is(err, cache.ErrNilValue) {
				t.Errorf("Set = %v, want ErrNilValue", err)
			}
			if _, err := svc.SetNX(ctx, k, v, 0); !errors.Is(err, cache.ErrNilValue) {
				t.Errorf("SetNX = %v, want ErrNilValue", err)
			}
			if err := svc.ZAdd(ctx, k+":z", 1, v); !errors.Is(err, cache.ErrNilValue) {
				t.Errorf("ZAdd = %v, want ErrNilValue", err)
			}
			if _, err := svc.ZRem(ctx, k+":z", v); !errors.Is(err, cache.ErrNilValue) {
				t.Errorf("ZRem = %v, want ErrNilValue", err)
			}
			if v, err := svc.Get(ctx, k); err == nil {
				t.Errorf("nil value was stored as %q", v)
			}
		})
	}
}

func TestZeroValue_EmptyValuesAreStored(t *testing.T) {
	t.Parallel()
	ctx := ctxT(t)
	svc := newRunning(t)
	for name, v := range map[string]any{"empty string": "", "empty bytes": []byte{}, "nil bytes": []byte(nil), "zero int": 0, "false": false} {
		t.Run(name, func(t *testing.T) {
			k := key(t)
			if err := svc.Set(ctx, k, v, time.Minute); err != nil {
				t.Fatal(err)
			}
			got, err := svc.Get(ctx, k)
			if err != nil {
				t.Fatalf("empty value reported as %v", err)
			}
			if got == nil {
				t.Fatal("Get returned nil slice for a stored value")
			}
		})
	}
}

func TestZeroValue_EmptyKey(t *testing.T) {
	t.Parallel()
	ctx := ctxT(t)
	svc := newRunning(t)
	if err := svc.Set(ctx, "", "v", time.Minute); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = svc.Delete(ctx, "") }()
	if v, err := svc.Get(ctx, ""); err != nil || string(v) != "v" {
		t.Fatalf("Get(\"\") = %q, %v", v, err)
	}
}

func TestZeroValue_MissingKeysGiveEmptyResults(t *testing.T) {
	t.Parallel()
	ctx := ctxT(t)
	svc := newRunning(t)
	k := key(t)

	if keys, err := svc.Keys(ctx, k+"*"); err != nil || keys == nil || len(keys) != 0 {
		t.Errorf("Keys no match = %#v, %v; want empty non-nil", keys, err)
	}
	if r, err := svc.ZRange(ctx, k, 0, -1); err != nil || len(r) != 0 {
		t.Errorf("ZRange missing = %v, %v", r, err)
	}
	if n, err := svc.ZCard(ctx, k); err != nil || n != 0 {
		t.Errorf("ZCard missing = %d, %v", n, err)
	}
	if n, err := svc.ZRem(ctx, k); err != nil || n != 0 {
		t.Errorf("ZRem no members = %d, %v", n, err)
	}
	if n, err := svc.ZRemRangeByScore(ctx, k, "-inf", "+inf"); err != nil || n != 0 {
		t.Errorf("ZRemRangeByScore missing = %d, %v", n, err)
	}
}

func TestZeroValue_TTLBoundaries(t *testing.T) {
	t.Parallel()
	ctx := ctxT(t)
	svc := newRunning(t)

	t.Run("zero and negative persist on Set", func(t *testing.T) {
		for _, ttl := range []time.Duration{0, -time.Second} {
			k := key(t)
			_ = svc.Set(ctx, k, "v", ttl)
			if ms := pttl(t, svc, k); ms != -1 {
				t.Errorf("Set ttl %v: PTTL = %d, want -1", ttl, ms)
			}
		}
	})
	t.Run("zero and negative Expire delete", func(t *testing.T) {
		for _, ttl := range []time.Duration{0, -time.Second} {
			k := key(t)
			_ = svc.Set(ctx, k, "v", 0)
			if ok, err := svc.Expire(ctx, k, ttl); !ok || err != nil {
				t.Errorf("Expire(%v) = %v, %v", ttl, ok, err)
			}
			if ok, _ := svc.Exists(ctx, k); ok {
				t.Errorf("Expire(%v) left the key in place", ttl)
			}
		}
	})
	t.Run("sub-millisecond ttl", func(t *testing.T) {
		// A positive TTL below 1ms must still be accepted (rounded up), not
		// rejected by the server or silently turned into "delete now".
		k := key(t)
		if err := svc.Set(ctx, k, "v", 500*time.Microsecond); err != nil {
			t.Errorf("Set 500µs: %v", err)
		}
		if _, err := svc.SetNX(ctx, key(t, "nx"), "v", 500*time.Microsecond); err != nil {
			t.Errorf("SetNX 500µs: %v", err)
		}
	})
}

func TestZeroValue_NilContextIsAProgrammingError(t *testing.T) {
	t.Parallel()
	// Documents behaviour: a nil ctx reaches the valkey client, which panics.
	svc := newRunning(t)
	var nilCtx context.Context
	mustPanic(t, "Get(nil ctx)", func() { _, _ = svc.Get(nilCtx, key(t)) })
}

// Dangling pointers.

func TestZeroValue_NilReceiverFailsLoudly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var ptr *cache.CacheService
	var iface cache.ICacheService
	extra := []op{
		{"Run", func(s cache.ICacheService) error { return s.Run(ctx) }},
		{"Stop", func(s cache.ICacheService) error { return s.Stop(ctx) }},
		{"Client", func(s cache.ICacheService) error { _ = s.Client(); return nil }},
	}
	for _, o := range append(allOps(ctx, "k"), extra...) {
		t.Run(o.name+"/nil pointer", func(t *testing.T) { mustPanic(t, o.name, func() { _ = o.call(ptr) }) })
		t.Run(o.name+"/nil interface", func(t *testing.T) { mustPanic(t, o.name, func() { _ = o.call(iface) }) })
	}
}

func TestZeroValue_StaleClientHandleAfterStop(t *testing.T) {
	t.Parallel()
	ctx := ctxT(t)
	svc := newRunning(t)
	stale := svc.Client()
	_ = svc.Stop(ctx)

	var err error
	mustNotPanic(t, "stale client", func() { err = stale.Do(ctx, stale.B().Ping().Build()).Error() })
	if !errors.Is(err, valkey.ErrClosing) {
		t.Fatalf("stale handle after Stop: err = %v, want valkey.ErrClosing", err)
	}
}
