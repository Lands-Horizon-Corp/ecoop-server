//go:build integration

package auth_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/auth"
)

// Happy path: the token works from the header or the site's cookie, and
// devices can be listed and signed out.

func TestHappy_HeaderAndCookie(t *testing.T) {
	t.Parallel()
	a, _, _ := newAuth(t)
	c := request(site)
	if err := a.SetCSRF(ctxT(t), c, ana, 3600e9); err != nil {
		t.Fatal(err)
	}
	set := string(c.Response.Header.Header())
	if !strings.Contains(set, header+"-app-ecoop-test=") || !strings.Contains(set, "HttpOnly") || !strings.Contains(set, "SameSite=Lax") {
		t.Fatalf("cookie not set as expected:\n%s", set)
	}
	token := string(c.Response.Header.Peek(header))
	byCookie := request(site)
	byCookie.Request.Header.SetCookie(header+"-app-ecoop-test", token)
	if got, err := a.GetCSRF(ctxT(t), byCookie); err != nil || got != ana {
		t.Fatalf("by cookie = %+v, %v", got, err)
	}
}

func TestHappy_Devices(t *testing.T) {
	t.Parallel()
	a, _, _ := newAuth(t)
	phone, laptop := login(t, a, ana, site), login(t, a, ana, site)
	ctx := ctxT(t)
	if other, err := a.HasActiveSessionOnOtherDevice(ctx, with(phone, site)); err != nil || !other {
		t.Fatalf("other device = %v, %v", other, err)
	}
	if claims, err := a.GetActiveSessionClaims(ctx, with(phone, site)); err != nil || len(claims) != 1 || claims[0] != ana {
		t.Fatalf("other sessions = %+v, %v", claims, err)
	}
	if err := a.RevokeOtherDeviceSessions(ctx, with(phone, site)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.VerifyCSRF(ctx, phone); err != nil {
		t.Fatalf("this device was signed out too: %v", err)
	}
	if _, err := a.VerifyCSRF(ctx, laptop); !errors.Is(err, auth.ErrInvalidToken) {
		t.Fatalf("other device still signed in: %v", err)
	}
	if err := a.RevokeUserSessions(ctx, ana.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := a.VerifyCSRF(ctx, phone); !errors.Is(err, auth.ErrInvalidToken) {
		t.Fatalf("still signed in after RevokeUserSessions: %v", err)
	}
}
