//go:build integration

package auth_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/auth"
)

// The cache never holds the token or the claim in clear: a cache dump
// cannot be replayed as a login or leak the user's details.
func TestPoison_CacheHoldsNoSecrets(t *testing.T) {
	t.Parallel()
	a, ch, name := newAuth(t)
	token := login(t, a, ana, site)
	keys, err := ch.Keys(ctxT(t), name+":*")
	if err != nil || len(keys) == 0 {
		t.Fatalf("keys = %v, %v", keys, err)
	}
	for _, k := range keys {
		if strings.Contains(k, token) {
			t.Fatalf("key %s holds the token", k)
		}
		if v, err := ch.Get(ctxT(t), k); err == nil && (strings.Contains(string(v), token) || strings.Contains(string(v), ana.Email)) {
			t.Fatalf("%s holds the token or the claim in clear", k)
		}
	}
}

// A tampered session record is an invalid session, not a crash; a hostile
// Origin cannot shape the cookie name.
func TestPoison_TamperingAndHostileOrigin(t *testing.T) {
	t.Parallel()
	a, ch, name := newAuth(t)
	token := login(t, a, ana, site)
	keys, _ := ch.Keys(ctxT(t), name+":auth:session:*")
	for _, k := range keys {
		_ = ch.Set(ctxT(t), k, `{"u":"u-1","c":"AAAA"}`, 0)
	}
	if _, err := a.VerifyCSRF(ctxT(t), token); !errors.Is(err, auth.ErrInvalidToken) {
		t.Fatalf("tampered session: %v", err)
	}
	c := request("https://evil.test/;path=/;domain=bank.test\r\nSet-Cookie: x=1")
	_ = a.SetCSRF(ctxT(t), c, ana, 3600e9)
	if set := string(c.Response.Header.Header()); strings.Contains(set, "domain=bank") || strings.Contains(set, "x=1") {
		t.Fatalf("hostile Origin shaped the cookie:\n%s", set)
	}
}
