package regressions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/cache"
	"github.com/alicebob/miniredis/v2"
)

// cacheFactory returns a running cache.CacheServices that uses the given key prefix.
// Services it returns share one backend, so prefix isolation can be tested.
type cacheFactory func(t *testing.T, prefix string) cache.CacheServices

// newIsolatedCache returns a running service with a prefix no other test uses.
// Its keys are flushed when the test ends. Flush only touches this prefix, so
// it is safe against a shared Redis.
func newIsolatedCache(t *testing.T, newSvc cacheFactory) cache.CacheServices {
	t.Helper()
	svc := newSvc(t, uniquePrefix(t))
	t.Cleanup(func() {
		if err := svc.Flush(context.Background()); err != nil {
			t.Errorf("cleanup flush failed: %v", err)
		}
	})
	return svc
}

func uniquePrefix(t *testing.T) string {
	return fmt.Sprintf("test:%s:%s:", t.Name(), strconv.FormatInt(time.Now().UnixNano(), 36))
}

// runCacheContract checks the behaviour every CacheServices backend must have.
func runCacheContract(t *testing.T, newSvc cacheFactory) {
	ctx := context.Background()

	t.Run("Ping", func(t *testing.T) {
		svc := newIsolatedCache(t, newSvc)
		if err := svc.Ping(ctx); err != nil {
			t.Fatalf("ping: %v", err)
		}
	})

	t.Run("SetGetRoundTrip", func(t *testing.T) {
		type user struct {
			ID   int    `json:"id"`
			Name string `json:"name"`
		}
		svc := newIsolatedCache(t, newSvc)

		if err := svc.Set(ctx, "str", "hello", time.Minute); err != nil {
			t.Fatalf("set string: %v", err)
		}
		if got, err := svc.Get(ctx, "str"); err != nil || string(got) != "hello" {
			t.Fatalf("get string = %q, %v; want %q", got, err, "hello")
		}

		if err := svc.Set(ctx, "bytes", []byte{0x01, 0x02}, time.Minute); err != nil {
			t.Fatalf("set bytes: %v", err)
		}
		if got, err := svc.Get(ctx, "bytes"); err != nil || string(got) != "\x01\x02" {
			t.Fatalf("get bytes = %q, %v", got, err)
		}

		if err := svc.Set(ctx, "num", 42, time.Minute); err != nil {
			t.Fatalf("set number: %v", err)
		}
		if got, err := svc.Get(ctx, "num"); err != nil || string(got) != "42" {
			t.Fatalf("get number = %q, %v; want %q", got, err, "42")
		}

		want := user{ID: 7, Name: "Jane"}
		if err := svc.Set(ctx, "struct", want, time.Minute); err != nil {
			t.Fatalf("set struct: %v", err)
		}
		raw, err := svc.Get(ctx, "struct")
		if err != nil {
			t.Fatalf("get struct: %v", err)
		}
		var got user
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatalf("struct stored as non-JSON %q: %v", raw, err)
		}
		if got != want {
			t.Fatalf("struct round trip = %+v; want %+v", got, want)
		}
	})

	t.Run("GetMissingReturnsErrNotFound", func(t *testing.T) {
		svc := newIsolatedCache(t, newSvc)
		_, err := svc.Get(ctx, "absent")
		if !errors.Is(err, cache.ErrNotFound) {
			t.Fatalf("get missing error = %v; want ErrNotFound", err)
		}
	})

	t.Run("ExistsAndDelete", func(t *testing.T) {
		svc := newIsolatedCache(t, newSvc)
		if ok, err := svc.Exists(ctx, "k"); err != nil || ok {
			t.Fatalf("exists before set = %v, %v; want false", ok, err)
		}
		if err := svc.Set(ctx, "k", "v", time.Minute); err != nil {
			t.Fatalf("set: %v", err)
		}
		if ok, err := svc.Exists(ctx, "k"); err != nil || !ok {
			t.Fatalf("exists after set = %v, %v; want true", ok, err)
		}
		if err := svc.Delete(ctx, "k"); err != nil {
			t.Fatalf("delete: %v", err)
		}
		if ok, _ := svc.Exists(ctx, "k"); ok {
			t.Fatal("key still exists after delete")
		}
		if err := svc.Delete(ctx, "k"); err != nil {
			t.Fatalf("deleting a missing key should not fail: %v", err)
		}
	})

	t.Run("SetNXOnlyWritesOnce", func(t *testing.T) {
		svc := newIsolatedCache(t, newSvc)
		first, err := svc.SetNX(ctx, "lock", "owner-a", time.Minute)
		if err != nil || !first {
			t.Fatalf("first SetNX = %v, %v; want true", first, err)
		}
		second, err := svc.SetNX(ctx, "lock", "owner-b", time.Minute)
		if err != nil || second {
			t.Fatalf("second SetNX = %v, %v; want false", second, err)
		}
		got, err := svc.Get(ctx, "lock")
		if err != nil || string(got) != "owner-a" {
			t.Fatalf("value after SetNX = %q, %v; want owner-a", got, err)
		}
	})

	t.Run("KeysStripsPrefixAndScopesToPrefix", func(t *testing.T) {
		svc := newIsolatedCache(t, newSvc)
		other := newIsolatedCache(t, newSvc)
		for _, k := range []string{"user:1", "user:2", "order:1"} {
			if err := svc.Set(ctx, k, "x", time.Minute); err != nil {
				t.Fatalf("set %s: %v", k, err)
			}
		}
		if err := other.Set(ctx, "user:3", "x", time.Minute); err != nil {
			t.Fatalf("set other: %v", err)
		}

		keys, err := svc.Keys(ctx, "user:*")
		if err != nil {
			t.Fatalf("keys: %v", err)
		}
		sort.Strings(keys)
		want := []string{"user:1", "user:2"}
		if fmt.Sprint(keys) != fmt.Sprint(want) {
			t.Fatalf("keys = %v; want %v (prefix must be stripped and other prefixes excluded)", keys, want)
		}
	})

	t.Run("PrefixWithGlobCharsMatchesLiterally", func(t *testing.T) {
		// "t:*[x]:" would match "t:ax:..." if the prefix were not escaped.
		base := uniquePrefix(t)
		literal := base + "*[x]:"
		wildcard := base + "ax:"
		svc := newSvc(t, literal)
		other := newSvc(t, wildcard)
		t.Cleanup(func() {
			_ = svc.Flush(ctx)
			_ = other.Flush(ctx)
		})

		if err := svc.Set(ctx, "k1", "a", time.Minute); err != nil {
			t.Fatalf("set: %v", err)
		}
		if err := other.Set(ctx, "k2", "b", time.Minute); err != nil {
			t.Fatalf("set other: %v", err)
		}
		keys, err := svc.Keys(ctx, "*")
		if err != nil {
			t.Fatalf("keys: %v", err)
		}
		if fmt.Sprint(keys) != "[k1]" {
			t.Fatalf("keys = %v; want [k1]", keys)
		}
		if err := svc.Flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
		if ok, _ := other.Exists(ctx, "k2"); !ok {
			t.Fatal("flush with glob-like prefix removed a key from another prefix")
		}
	})

	t.Run("FlushOnlyRemovesOwnPrefix", func(t *testing.T) {
		svc := newIsolatedCache(t, newSvc)
		other := newIsolatedCache(t, newSvc)
		if err := svc.Set(ctx, "a", "1", time.Minute); err != nil {
			t.Fatalf("set: %v", err)
		}
		if err := other.Set(ctx, "a", "2", time.Minute); err != nil {
			t.Fatalf("set other: %v", err)
		}
		if err := svc.Flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
		if ok, _ := svc.Exists(ctx, "a"); ok {
			t.Fatal("flush did not remove own key")
		}
		if got, err := other.Get(ctx, "a"); err != nil || string(got) != "2" {
			t.Fatalf("flush removed another prefix's key: %q, %v", got, err)
		}
	})

	t.Run("IncrCountsAndSetsTTL", func(t *testing.T) {
		svc := newIsolatedCache(t, newSvc)
		for want := int64(1); want <= 3; want++ {
			got, err := svc.Incr(ctx, "hits", time.Minute)
			if err != nil || got != want {
				t.Fatalf("incr #%d = %d, %v; want %d", want, got, err, want)
			}
		}
	})

	t.Run("IncrConcurrentIsAtomic", func(t *testing.T) {
		const workers = 50
		svc := newIsolatedCache(t, newSvc)

		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			seen    = make(map[int64]bool, workers)
			failure error
		)
		for range workers {
			wg.Go(func() {
				n, err := svc.Incr(ctx, "counter", time.Minute)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					failure = err
					return
				}
				seen[n] = true
			})
		}
		wg.Wait()

		if failure != nil {
			t.Fatalf("incr failed: %v", failure)
		}
		if len(seen) != workers {
			t.Fatalf("got %d distinct values; want %d (lost updates)", len(seen), workers)
		}
		if !seen[workers] {
			t.Fatalf("final value %d not seen", workers)
		}
	})

	t.Run("ExpireRules", func(t *testing.T) {
		svc := newIsolatedCache(t, newSvc)
		if ok, err := svc.Expire(ctx, "missing", time.Minute); err != nil || ok {
			t.Fatalf("expire missing = %v, %v; want false", ok, err)
		}
		if err := svc.Set(ctx, "k", "v", 0); err != nil {
			t.Fatalf("set: %v", err)
		}
		if ok, err := svc.Expire(ctx, "k", time.Minute); err != nil || !ok {
			t.Fatalf("expire existing = %v, %v; want true", ok, err)
		}
		if _, err := svc.Expire(ctx, "k", 0); err == nil {
			t.Fatal("expire with zero ttl should fail; it would delete the key")
		}
		if ok, _ := svc.Exists(ctx, "k"); !ok {
			t.Fatal("rejected expire still removed the key")
		}
	})

	t.Run("SortedSetOperations", func(t *testing.T) {
		svc := newIsolatedCache(t, newSvc)
		for _, m := range []struct {
			member string
			score  float64
		}{{"a", 1}, {"b", 2}, {"c", 3}} {
			if err := svc.ZAdd(ctx, "z", m.score, m.member); err != nil {
				t.Fatalf("zadd %s: %v", m.member, err)
			}
		}

		if n, err := svc.ZCard(ctx, "z"); err != nil || n != 3 {
			t.Fatalf("zcard = %d, %v; want 3", n, err)
		}
		if members, err := svc.ZRange(ctx, "z", 0, -1); err != nil || fmt.Sprint(members) != "[a b c]" {
			t.Fatalf("zrange = %v, %v; want [a b c]", members, err)
		}
		withScores, err := svc.ZRangeWithScores(ctx, "z", 0, -1)
		if err != nil || len(withScores) != 3 || withScores[2].Score != 3 {
			t.Fatalf("zrange with scores = %+v, %v", withScores, err)
		}

		if n, err := svc.ZRem(ctx, "z", "b"); err != nil || n != 1 {
			t.Fatalf("zrem = %d, %v; want 1", n, err)
		}
		if n, err := svc.ZRemRangeByScore(ctx, "z", "-inf", "(2"); err != nil || n != 1 {
			t.Fatalf("zremrangebyscore = %d, %v; want 1", n, err)
		}
		if members, _ := svc.ZRange(ctx, "z", 0, -1); fmt.Sprint(members) != "[c]" {
			t.Fatalf("remaining members = %v; want [c]", members)
		}
	})
}

// newMiniredisCache starts an in-process Redis, so these tests need no Docker.
func newMiniredisCache(t *testing.T) (cacheFactory, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	factory := func(t *testing.T, prefix string) cache.CacheServices {
		t.Helper()
		svc := cache.NewCacheService([]string{"redis://" + mr.Addr()}, "", "", prefix)
		if err := svc.Run(context.Background()); err != nil {
			t.Fatalf("run: %v", err)
		}
		t.Cleanup(func() { _ = svc.Stop(context.Background()) })
		return svc
	}
	return factory, mr
}

func TestCacheService_Miniredis(t *testing.T) {
	factory, _ := newMiniredisCache(t)
	runCacheContract(t, factory)
}

func TestCacheService_Lifecycle(t *testing.T) {
	ctx := context.Background()

	t.Run("OperationsBeforeRunFail", func(t *testing.T) {
		svc := cache.NewCacheService([]string{"redis://127.0.0.1:1"}, "", "", "p:")
		checks := map[string]error{
			"Ping":  svc.Ping(ctx),
			"Get":   second(svc.Get(ctx, "k")),
			"Set":   svc.Set(ctx, "k", "v", time.Minute),
			"Incr":  second(svc.Incr(ctx, "k", time.Minute)),
			"Keys":  second(svc.Keys(ctx, "*")),
			"Flush": svc.Flush(ctx),
		}
		for name, err := range checks {
			if !errors.Is(err, cache.ErrNotRunning) {
				t.Errorf("%s before Run = %v; want ErrNotRunning", name, err)
			}
		}
	})

	t.Run("RunRejectsBadConfig", func(t *testing.T) {
		mr := miniredis.RunT(t)
		cases := map[string][]string{
			"no url":        {},
			"multiple urls": {"redis://" + mr.Addr(), "redis://" + mr.Addr()},
			"bad scheme":    {"://nope"},
		}
		for name, urls := range cases {
			svc := cache.NewCacheService(urls, "", "", "")
			if err := svc.Run(ctx); err == nil {
				t.Errorf("%s: Run succeeded; want error", name)
				_ = svc.Stop(ctx)
			}
		}
	})

	t.Run("RunFailsWhenServerUnreachable", func(t *testing.T) {
		mr := miniredis.RunT(t)
		addr := mr.Addr()
		mr.Close()
		svc := cache.NewCacheService([]string{"redis://" + addr}, "", "", "")
		if err := svc.Run(ctx); err == nil {
			t.Fatal("Run succeeded against a closed server; want error")
		}
	})

	t.Run("RunTwiceFails", func(t *testing.T) {
		mr := miniredis.RunT(t)
		svc := cache.NewCacheService([]string{"redis://" + mr.Addr()}, "", "", "")
		if err := svc.Run(ctx); err != nil {
			t.Fatalf("first run: %v", err)
		}
		t.Cleanup(func() { _ = svc.Stop(ctx) })
		if err := svc.Run(ctx); err == nil {
			t.Fatal("second Run succeeded; want error")
		}
	})

	t.Run("StopIsIdempotentAndBlocksUse", func(t *testing.T) {
		mr := miniredis.RunT(t)
		svc := cache.NewCacheService([]string{"redis://" + mr.Addr()}, "", "", "")
		if err := svc.Stop(ctx); err != nil {
			t.Fatalf("stop before run: %v", err)
		}
		if err := svc.Run(ctx); err != nil {
			t.Fatalf("run: %v", err)
		}
		if err := svc.Stop(ctx); err != nil {
			t.Fatalf("stop: %v", err)
		}
		if err := svc.Stop(ctx); err != nil {
			t.Fatalf("second stop: %v", err)
		}
		if err := svc.Set(ctx, "k", "v", time.Minute); !errors.Is(err, cache.ErrNotRunning) {
			t.Fatalf("set after stop = %v; want ErrNotRunning", err)
		}
	})

	t.Run("StopKeepsData", func(t *testing.T) {
		mr := miniredis.RunT(t)
		url := "redis://" + mr.Addr()

		first := cache.NewCacheService([]string{url}, "", "", "app:")
		if err := first.Run(ctx); err != nil {
			t.Fatalf("run: %v", err)
		}
		if err := first.Set(ctx, "k", "persisted", time.Hour); err != nil {
			t.Fatalf("set: %v", err)
		}
		if err := first.Stop(ctx); err != nil {
			t.Fatalf("stop: %v", err)
		}

		second := cache.NewCacheService([]string{url}, "", "", "app:")
		if err := second.Run(ctx); err != nil {
			t.Fatalf("second run: %v", err)
		}
		t.Cleanup(func() { _ = second.Stop(ctx) })
		got, err := second.Get(ctx, "k")
		if err != nil || string(got) != "persisted" {
			t.Fatalf("value after restart = %q, %v; want persisted", got, err)
		}
	})

	t.Run("PasswordIsUsed", func(t *testing.T) {
		mr := miniredis.RunT(t)
		mr.RequireAuth("s3cret")
		url := "redis://" + mr.Addr()

		wrong := cache.NewCacheService([]string{url}, "nope", "", "")
		if err := wrong.Run(ctx); err == nil {
			_ = wrong.Stop(ctx)
			t.Fatal("Run with wrong password succeeded")
		}

		right := cache.NewCacheService([]string{url}, "s3cret", "", "")
		if err := right.Run(ctx); err != nil {
			t.Fatalf("Run with correct password: %v", err)
		}
		t.Cleanup(func() { _ = right.Stop(ctx) })
	})

	t.Run("SetTTLExpires", func(t *testing.T) {
		factory, mr := newMiniredisCache(t)
		svc := factory(t, "exp:")
		if err := svc.Set(ctx, "k", "v", time.Second); err != nil {
			t.Fatalf("set: %v", err)
		}
		mr.FastForward(2 * time.Second)
		if _, err := svc.Get(ctx, "k"); !errors.Is(err, cache.ErrNotFound) {
			t.Fatalf("get after ttl = %v; want ErrNotFound", err)
		}
	})

	t.Run("IncrWindowResetsAfterTTL", func(t *testing.T) {
		factory, mr := newMiniredisCache(t)
		svc := factory(t, "rate:")
		for range 3 {
			if _, err := svc.Incr(ctx, "req", time.Second); err != nil {
				t.Fatalf("incr: %v", err)
			}
		}
		mr.FastForward(2 * time.Second)
		n, err := svc.Incr(ctx, "req", time.Second)
		if err != nil || n != 1 {
			t.Fatalf("incr after window = %d, %v; want 1", n, err)
		}
	})

	t.Run("IncrZeroTTLNeverExpires", func(t *testing.T) {
		factory, mr := newMiniredisCache(t)
		svc := factory(t, "forever:")
		if _, err := svc.Incr(ctx, "n", 0); err != nil {
			t.Fatalf("incr: %v", err)
		}
		mr.FastForward(24 * time.Hour)
		if ok, _ := svc.Exists(ctx, "n"); !ok {
			t.Fatal("key with zero ttl expired")
		}
	})
}

// runCacheExtras covers behaviour beyond runCacheContract that must hold on a real server.
func runCacheExtras(t *testing.T, factory cacheFactory) {
	ctx := context.Background()

	t.Run("ValueEncodings", func(t *testing.T) {
		svc := newIsolatedCache(t, factory)
		type payload struct {
			Name string `json:"name"`
			Age  int    `json:"age"`
		}
		cases := []struct {
			name  string
			value any
			want  string
		}{
			{"string", "hello", "hello"},
			{"bytes", []byte("raw"), "raw"},
			{"int", 42, "42"},
			{"bool", true, "true"},
			{"float", 1.5, "1.5"},
			{"struct", payload{Name: "a", Age: 3}, `{"name":"a","age":3}`},
			{"map", map[string]int{"x": 1}, `{"x":1}`},
		}
		for _, tc := range cases {
			if err := svc.Set(ctx, tc.name, tc.value, time.Minute); err != nil {
				t.Fatalf("%s: Set: %v", tc.name, err)
			}
			got, err := svc.Get(ctx, tc.name)
			if err != nil || string(got) != tc.want {
				t.Errorf("%s: Get = %q, %v; want %q", tc.name, got, err, tc.want)
			}
		}
		if err := svc.Set(ctx, "bad", make(chan int), time.Minute); err == nil {
			t.Error("Set of an unencodable value succeeded; want error")
		}
	})

	t.Run("KeysSpansScanPages", func(t *testing.T) {
		svc := newIsolatedCache(t, factory)
		const n = 350
		for i := range n {
			if err := svc.Set(ctx, "k"+strconv.Itoa(i), "v", time.Minute); err != nil {
				t.Fatalf("Set: %v", err)
			}
		}
		got, err := svc.Keys(ctx, "*")
		if err != nil || len(got) != n {
			t.Fatalf("Keys(*) returned %d keys, err %v; want %d", len(got), err, n)
		}
	})

	t.Run("IncrWindowIsNotRefreshed", func(t *testing.T) {
		svc := newIsolatedCache(t, factory)
		const window = 600 * time.Millisecond
		if n, err := svc.Incr(ctx, "c", window); err != nil || n != 1 {
			t.Fatalf("first Incr = %d, %v; want 1", n, err)
		}
		time.Sleep(300 * time.Millisecond)
		if n, err := svc.Incr(ctx, "c", window); err != nil || n != 2 {
			t.Fatalf("second Incr = %d, %v; want 2", n, err)
		}
		// 700ms after creation: expired if the window is fixed, still alive if it was extended.
		time.Sleep(400 * time.Millisecond)
		if n, err := svc.Incr(ctx, "c", window); err != nil || n != 1 {
			t.Fatalf("Incr after the original window = %d, %v; want 1 (window must not be extended)", n, err)
		}
	})

	t.Run("SortedSetRemoveByScoreAndStructMembers", func(t *testing.T) {
		svc := newIsolatedCache(t, factory)
		for i, m := range []string{"a", "b", "c", "d"} {
			if err := svc.ZAdd(ctx, "z", float64(i+1), m); err != nil {
				t.Fatalf("ZAdd: %v", err)
			}
		}
		scored, err := svc.ZRangeWithScores(ctx, "z", 0, 1)
		if err != nil || len(scored) != 2 || scored[0].Member != "a" || scored[1].Score != 2 {
			t.Fatalf("ZRangeWithScores = %v, %v", scored, err)
		}
		if n, err := svc.ZRemRangeByScore(ctx, "z", "-inf", "(2"); err != nil || n != 1 {
			t.Fatalf("ZRemRangeByScore = %d, %v; want 1", n, err)
		}
		members, _ := svc.ZRange(ctx, "z", 0, -1)
		if want := []string{"b", "c", "d"}; !slices.Equal(members, want) {
			t.Fatalf("members = %v; want %v", members, want)
		}

		type event struct{ ID int }
		if err := svc.ZAdd(ctx, "events", 1, event{ID: 7}); err != nil {
			t.Fatalf("ZAdd struct: %v", err)
		}
		if n, err := svc.ZRem(ctx, "events", event{ID: 7}); err != nil || n != 1 {
			t.Fatalf("ZRem struct = %d, %v; want 1", n, err)
		}
	})
}
