package cache_test

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
)

func TestHappyPath_SetGetEncodings(t *testing.T) {
	ctx := ctxT(t)
	svc := newRunning(t)
	type member struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	cases := map[string]struct {
		in   any
		want string
	}{
		"string":  {"hello", "hello"},
		"bytes":   {[]byte{0x00, 0xff, 0x10}, "\x00\xff\x10"},
		"int":     {42, "42"},
		"int64":   {int64(-9007199254740993), "-9007199254740993"},
		"int32":   {int32(-7), "-7"},
		"uint64":  {uint64(18446744073709551615), "18446744073709551615"},
		"uint32":  {uint32(7), "7"},
		"float64": {3.14159, "3.14159"},
		"float32": {float32(0.5), "0.5"},
		"bool":    {true, "true"},
		"struct":  {member{1, "Ana"}, `{"id":1,"name":"Ana"}`},
		"map":     {map[string]int{"a": 1}, `{"a":1}`},
		"slice":   {[]string{"x", "y"}, `["x","y"]`},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			k := key(t)
			if err := svc.Set(ctx, k, c.in, time.Minute); err != nil {
				t.Fatalf("Set: %v", err)
			}
			got, err := svc.Get(ctx, k)
			if err != nil || string(got) != c.want {
				t.Fatalf("Get = %q, %v; want %q", got, err, c.want)
			}
		})
	}
}

func TestHappyPath_SetOverwritesAndTTL(t *testing.T) {
	ctx := ctxT(t)
	svc := newRunning(t)
	k := key(t)

	_ = svc.Set(ctx, k, "v1", 0)
	if ms := pttl(t, svc, k); ms != -1 {
		t.Fatalf("ttl 0 should persist, PTTL = %d", ms)
	}
	_ = svc.Set(ctx, k, "v2", 150*time.Millisecond)
	if v, _ := svc.Get(ctx, k); string(v) != "v2" {
		t.Fatalf("overwrite failed: %q", v)
	}
	if ms := pttl(t, svc, k); ms <= 0 || ms > 150 {
		t.Fatalf("PTTL = %d, want (0,150]", ms)
	}
	time.Sleep(250 * time.Millisecond)
	if _, err := svc.Get(ctx, k); !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("expired key: err = %v, want ErrNotFound", err)
	}
}

func TestHappyPath_SetNX(t *testing.T) {
	ctx := ctxT(t)
	svc := newRunning(t)
	k := key(t)

	if ok, err := svc.SetNX(ctx, k, "first", time.Minute); !ok || err != nil {
		t.Fatalf("first SetNX = %v, %v", ok, err)
	}
	if ok, err := svc.SetNX(ctx, k, "second", time.Minute); ok || err != nil {
		t.Fatalf("second SetNX = %v, %v; want false, nil", ok, err)
	}
	if v, _ := svc.Get(ctx, k); string(v) != "first" {
		t.Fatalf("value = %q, want first", v)
	}
	if ms := pttl(t, svc, k); ms <= 0 {
		t.Fatalf("SetNX ttl not applied, PTTL = %d", ms)
	}
}

func TestHappyPath_ExistsDelete(t *testing.T) {
	ctx := ctxT(t)
	svc := newRunning(t)
	k := key(t)

	if ok, _ := svc.Exists(ctx, k); ok {
		t.Fatal("missing key reported as existing")
	}
	_ = svc.Set(ctx, k, "v", 0)
	if ok, err := svc.Exists(ctx, k); !ok || err != nil {
		t.Fatalf("Exists = %v, %v", ok, err)
	}
	if deleted, err := svc.Delete(ctx, k); err != nil || !deleted {
		t.Fatalf("Delete = %v, %v; want true, nil", deleted, err)
	}
	if ok, _ := svc.Exists(ctx, k); ok {
		t.Fatal("key exists after Delete")
	}
	if deleted, err := svc.Delete(ctx, k); err != nil || deleted {
		t.Fatalf("deleting a missing key = %v, %v; want false, nil", deleted, err)
	}
}

func TestHappyPath_KeysScansPastOnePage(t *testing.T) {
	ctx := ctxT(t)
	svc := newRunning(t)
	const n = 2500 // more than one SCAN page
	want := make([]string, n)
	for i := range n {
		want[i] = key(t, fmt.Sprint(i))
		if err := svc.Set(ctx, want[i], i, time.Minute); err != nil {
			t.Fatal(err)
		}
	}
	other := "other:" + t.Name()
	cleanupKey(t, other)
	_ = svc.Set(ctx, other, "not mine", time.Minute)

	got, err := svc.Keys(ctx, key(t, "*"))
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("Keys returned %d keys, want %d", len(got), n)
	}
}

func TestHappyPath_SortedSet(t *testing.T) {
	ctx := ctxT(t)
	svc := newRunning(t)
	k := key(t)

	for score, m := range map[float64]any{3: "c", 1: "a", 2: "b", 4: 42, 5: 2.5} {
		if err := svc.ZAdd(ctx, k, score, m); err != nil {
			t.Fatalf("ZAdd: %v", err)
		}
	}
	if got, _ := svc.ZRange(ctx, k, 0, -1); !slices.Equal(got, []string{"a", "b", "c", "42", "2.5"}) {
		t.Fatalf("ZRange = %v", got)
	}
	if got, _ := svc.ZRange(ctx, k, 1, 2); !slices.Equal(got, []string{"b", "c"}) {
		t.Fatalf("ZRange(1,2) = %v", got)
	}
	if n, _ := svc.ZCard(ctx, k); n != 5 {
		t.Fatalf("ZCard = %d", n)
	}

	_ = svc.ZAdd(ctx, k, 10, "a") // re-score moves the member
	if got, _ := svc.ZRange(ctx, k, -1, -1); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("re-scored member not last: %v", got)
	}

	if n, _ := svc.ZRem(ctx, k, "b", "missing", 42); n != 2 {
		t.Fatalf("ZRem removed %d, want 2", n)
	}
	if n, _ := svc.ZRemRangeByScore(ctx, k, "(3", "+inf"); n != 2 {
		t.Fatalf("ZRemRangeByScore removed %d, want 2 (2.5@5 and a@10)", n)
	}
	if got, _ := svc.ZRange(ctx, k, 0, -1); !slices.Equal(got, []string{"c"}) {
		t.Fatalf("remaining = %v, want [c]", got)
	}
}

func TestHappyPath_IncrCounterAndWindow(t *testing.T) {
	ctx := ctxT(t)
	svc := newRunning(t)

	plain := key(t, "plain")
	for want := int64(1); want <= 5; want++ {
		if n, err := svc.Incr(ctx, plain, 0); n != want || err != nil {
			t.Fatalf("Incr = %d, %v; want %d", n, err, want)
		}
	}
	if ms := pttl(t, svc, plain); ms != -1 {
		t.Fatalf("Incr with ttl 0 set an expiry: %d", ms)
	}

	window := key(t, "window")
	_, _ = svc.Incr(ctx, window, 200*time.Millisecond)
	time.Sleep(80 * time.Millisecond)
	_, _ = svc.Incr(ctx, window, 200*time.Millisecond)
	if ms := pttl(t, svc, window); ms > 150 {
		t.Fatalf("second Incr extended the window: PTTL = %d", ms)
	}
	time.Sleep(200 * time.Millisecond)
	if n, _ := svc.Incr(ctx, window, 200*time.Millisecond); n != 1 {
		t.Fatalf("window did not reset: n = %d", n)
	}
}

func TestHappyPath_Expire(t *testing.T) {
	ctx := ctxT(t)
	svc := newRunning(t)
	k := key(t)
	_ = svc.Set(ctx, k, "v", 0)

	if ok, err := svc.Expire(ctx, k, 100*time.Millisecond); !ok || err != nil {
		t.Fatalf("Expire = %v, %v", ok, err)
	}
	time.Sleep(200 * time.Millisecond)
	if ok, _ := svc.Exists(ctx, k); ok {
		t.Fatal("key survived its expiry")
	}
	if ok, err := svc.Expire(ctx, k, time.Second); ok || err != nil {
		t.Fatalf("Expire on missing key = %v, %v; want false, nil", ok, err)
	}
}

func TestHappyPath_ClientIsUsable(t *testing.T) {
	ctx := ctxT(t)
	svc := newRunning(t)
	cl := svc.Client()
	if cl == nil {
		t.Fatal("Client() is nil while running")
	}
	k := key(t)
	if err := cl.Do(ctx, cl.B().Set().Key(k).Value("raw").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	if v, _ := svc.Get(ctx, k); string(v) != "raw" {
		t.Fatalf("value written via Client() not visible: %q", v)
	}
}

func TestHappyPath_RunWhileRunningIsNoop(t *testing.T) {
	ctx := ctxT(t)
	svc := newRunning(t)
	before := svc.Client()
	if err := svc.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if svc.Client() != before {
		t.Fatal("Run while running replaced the client")
	}
	if err := svc.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestHappyPath_Flush(t *testing.T) {
	requireFlush(t)
	ctx := ctxT(t)
	svc := newRunning(t)
	for i := range 10 {
		_ = svc.Set(ctx, key(t, fmt.Sprint(i)), i, 0)
	}
	if err := svc.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if keys, _ := svc.Keys(ctx, key(t, "*")); len(keys) != 0 {
		t.Fatalf("%d keys left after Flush", len(keys))
	}
}

func TestHappyPath_StopThenRunAgain(t *testing.T) {
	ctx := ctxT(t)
	svc := newRunning(t)
	k := key(t)
	_ = svc.Set(ctx, k, "persisted", time.Minute)
	_ = svc.Stop(ctx)
	if err := svc.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if v, err := svc.Get(ctx, k); err != nil || string(v) != "persisted" {
		t.Fatalf("data lost across restart: %q, %v", v, err)
	}
}
