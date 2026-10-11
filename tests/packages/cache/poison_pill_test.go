package cache_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
)

// Poison pill: inputs shaped to break the protocol, leak across keys, or
// corrupt data.

func TestPoisonPill_HostileKeysAreBinarySafe(t *testing.T) {
	ctx := ctxT(t)
	svc := newRunning(t)
	cases := map[string]string{
		"resp injection": "x\r\n*1\r\n$8\r\nFLUSHALL\r\n",
		"null bytes":     "a\x00b",
		"unicode":        "ключ-鍵-🔑",
		"spaces":         "  spaced key  ",
		"glob chars":     "a*b?c[d]",
		"64KiB":          strings.Repeat("k", 64<<10),
		"invalid utf8":   string([]byte{0xff, 0xfe}),
	}
	sentinel := key(t, "sentinel")
	_ = svc.Set(ctx, sentinel, "alive", time.Minute)

	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			k := key(t) + raw
			if err := svc.Set(ctx, k, raw, time.Minute); err != nil {
				t.Fatalf("Set: %v", err)
			}
			if v, err := svc.Get(ctx, k); err != nil || string(v) != raw {
				t.Fatalf("Get mismatch: %v", err)
			}
			if ok, _ := svc.Exists(ctx, k); !ok {
				t.Fatal("Exists false")
			}
		})
	}
	if v, err := svc.Get(ctx, sentinel); err != nil || string(v) != "alive" {
		t.Fatal("hostile key affected an unrelated key (protocol injection?)")
	}
}

func TestPoisonPill_HostileValues(t *testing.T) {
	ctx := ctxT(t)
	svc := newRunning(t)
	cases := map[string]string{
		"crlf":         "line1\r\nline2\r\n",
		"resp frame":   "+OK\r\n",
		"null bytes":   "\x00\x00\x00",
		"10MiB":        strings.Repeat("v", 10<<20),
		"json-ish":     `{"__proto__":{"admin":true}}`,
		"invalid utf8": string([]byte{0xc3, 0x28, 0xff}),
	}
	for name, v := range cases {
		t.Run(name, func(t *testing.T) {
			k := key(t)
			if err := svc.Set(ctx, k, v, time.Minute); err != nil {
				t.Fatal(err)
			}
			got, err := svc.Get(ctx, k)
			if err != nil || string(got) != v {
				t.Fatalf("value altered (len %d -> %d): %v", len(v), len(got), err)
			}
		})
	}
}

func TestPoisonPill_PatternCannotEscapeNamespace(t *testing.T) {
	ctx := ctxT(t)
	svc := newRunning(t)
	mine := key(t, "a")
	other := "other:" + t.Name() + ":secret"
	cleanupKey(t, other)
	_ = svc.Set(ctx, mine, 1, time.Minute)
	_ = svc.Set(ctx, other, 1, time.Minute)
	defer func() { _, _ = svc.Delete(ctx, other) }()

	for _, suffix := range []string{"*", "a", "[a-z]", "?", "\\*", "*\r\n*"} {
		keys, err := svc.Keys(ctx, key(t, suffix))
		if err != nil {
			continue
		}
		for _, k := range keys {
			if k == other {
				t.Fatalf("pattern %q leaked a key outside its prefix", suffix)
			}
		}
	}
	for _, p := range []string{"[", "\\", "[[[[", strings.Repeat("*", 1000)} {
		mustNotPanic(t, fmt.Sprintf("Keys(%q)", p), func() { _, _ = svc.Keys(ctx, key(t, p)) })
	}
}

// A failed Incr must not leave side effects. The expiry is pipelined with
// INCR, so without care a non-numeric key gets a TTL it never had and
// silently disappears later.
func TestPoisonPill_FailedIncrDoesNotExpireKey(t *testing.T) {
	ctx := ctxT(t)
	svc := newRunning(t)
	k := key(t)
	_ = svc.Set(ctx, k, "session-token", 0)

	if _, err := svc.Incr(ctx, k, time.Minute); err == nil {
		t.Fatal("Incr on non-numeric value succeeded")
	}
	if ms := pttl(t, svc, k); ms != -1 {
		t.Fatalf("failed Incr put a %dms expiry on an unrelated persistent key", ms)
	}
}

func TestPoisonPill_CyclicValue(t *testing.T) {
	type node struct{ Next *node }
	n := &node{}
	n.Next = n
	svc := newRunning(t)
	var err error
	mustNotPanic(t, "Set(cyclic)", func() { err = svc.Set(ctxT(t), key(t), n, 0) })
	if err == nil {
		t.Fatal("cyclic value accepted")
	}
}

func TestPoisonPill_ManyMembers(t *testing.T) {
	ctx := ctxT(t)
	svc := newRunning(t)
	k := key(t)
	members := make([]any, 20000)
	for i := range members {
		members[i] = i
		if i < 100 {
			_ = svc.ZAdd(ctx, k, float64(i), i)
		}
	}
	n, err := svc.ZRem(ctx, k, members...)
	if err != nil || n != 100 {
		t.Fatalf("ZRem of 20k members = %d, %v; want 100", n, err)
	}
}

func TestPoisonPill_ErrorsAreDistinguishable(t *testing.T) {
	ctx := ctxT(t)
	svc := newRunning(t)
	k := key(t)
	_ = svc.ZAdd(ctx, k, 1, "m")
	_, err := svc.Get(ctx, k)
	if errors.Is(err, cache.ErrNotFound) || errors.Is(err, cache.ErrNotRunning) {
		t.Fatalf("server error masqueraded as a sentinel: %v", err)
	}
}
