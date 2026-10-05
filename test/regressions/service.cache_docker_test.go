package regressions

import (
	"context"
	"net"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"
)

// These tests run against the real Redis and Sentinel in docker-compose.yml.
// They fail (not skip) when the containers are down, so CI cannot pass without them:
//
//	docker compose up -d --wait redis redis-master redis-sentinel

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func requireReachable(t *testing.T, addr string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("%s is not reachable (%v); run: docker compose up -d --wait redis redis-master redis-sentinel", addr, err)
	}
	_ = conn.Close()
}

// closedAddr returns a local address nothing is listening on.
func closedAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
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
