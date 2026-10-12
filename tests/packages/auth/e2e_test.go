//go:build integration

package auth_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"strings"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/auth"
)

// End to end: a real Hertz server on a real socket, and a real HTTP client
// with a cookie jar, as a browser holds the session in production.

// api serves a, the way the app wires it: sign in, who am I, devices, sign
// out. A failed session store is a 503, not a 401: an outage must not look
// like being signed out.
func api(t *testing.T, a auth.AUTHServices[user]) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	h := server.New(server.WithHostPorts(addr), server.WithExitWaitTime(0), server.WithDisablePrintRoute(true))
	status := func(err error) int {
		switch {
		case err == nil:
			return http.StatusOK
		case errors.Is(err, auth.ErrNoToken), errors.Is(err, auth.ErrInvalidToken):
			return http.StatusUnauthorized
		case errors.Is(err, auth.ErrSession):
			return http.StatusServiceUnavailable
		}
		return http.StatusBadRequest
	}
	h.POST("/login", func(ctx context.Context, c *app.RequestContext) {
		u := user{ID: c.Query("id"), Email: c.Query("id") + "@ecoop.test", Role: "member"}
		c.Status(status(a.SetCSRF(ctx, c, u, time.Hour)))
	})
	h.GET("/me", func(ctx context.Context, c *app.RequestContext) {
		u, err := a.GetCSRF(ctx, c)
		if err != nil {
			c.AbortWithStatus(status(err))
			return
		}
		c.JSON(http.StatusOK, u)
	})
	h.GET("/devices", func(ctx context.Context, c *app.RequestContext) {
		others, err := a.GetActiveSessionClaims(ctx, c)
		if err != nil {
			c.AbortWithStatus(status(err))
			return
		}
		c.JSON(http.StatusOK, map[string]int{"others": len(others)})
	})
	h.POST("/logout", func(ctx context.Context, c *app.RequestContext) { c.Status(status(a.ClearCSRF(ctx, c))) })
	h.POST("/logout-others", func(ctx context.Context, c *app.RequestContext) {
		c.Status(status(a.RevokeOtherDeviceSessions(ctx, c)))
	})
	go func() { _ = h.Run() }()
	t.Cleanup(func() { _ = h.Shutdown(context.Background()) })
	deadline := time.Now().Add(5 * time.Second)
	for {
		if conn, err := net.Dial("tcp", addr); err == nil {
			_ = conn.Close()
			return "http://" + addr
		}
		if time.Now().After(deadline) {
			t.Fatal("test server never started")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// browser is an HTTP client with its own cookie jar, browsing from origin.
type browser struct {
	client *http.Client
	base   string
	origin string
}

func newBrowser(t *testing.T, base, origin string, jar ...http.CookieJar) *browser {
	t.Helper()
	var j http.CookieJar
	if len(jar) > 0 {
		j = jar[0]
	} else {
		j, _ = cookiejar.New(nil)
	}
	return &browser{client: &http.Client{Jar: j, Timeout: 10 * time.Second}, base: base, origin: origin}
}

func (b *browser) do(t *testing.T, method, path string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(method, b.base+path, nil)
	req.Header.Set("Origin", b.origin)
	resp, err := b.client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func (b *browser) me(t *testing.T) (user, int) {
	t.Helper()
	resp := b.do(t, http.MethodGet, "/me")
	var u user
	if resp.StatusCode == http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		_ = json.Unmarshal(raw, &u)
	}
	return u, resp.StatusCode
}

// The whole lifecycle with nothing but the cookie the browser keeps.
func TestE2E_BrowserSessionWithCookiesOnly(t *testing.T) {
	t.Parallel()
	a, _, _ := newAuth(t)
	b := newBrowser(t, api(t, a), site)
	if _, code := b.me(t); code != http.StatusUnauthorized {
		t.Fatalf("before sign-in: %d", code)
	}
	if code := b.do(t, http.MethodPost, "/login?id=u-7").StatusCode; code != http.StatusOK {
		t.Fatalf("sign-in: %d", code)
	}
	if u, code := b.me(t); code != http.StatusOK || u.ID != "u-7" || u.Email != "u-7@ecoop.test" {
		t.Fatalf("after sign-in: %d %+v", code, u)
	}
	if code := b.do(t, http.MethodPost, "/logout").StatusCode; code != http.StatusOK {
		t.Fatalf("sign-out: %d", code)
	}
	if _, code := b.me(t); code != http.StatusUnauthorized {
		t.Fatalf("after sign-out: %d (the cookie must be gone)", code)
	}
}

// Set-Cookie as a browser receives it, in both modes.
func TestE2E_CookieFlags(t *testing.T) {
	t.Parallel()
	for _, ssl := range []bool{false, true} {
		_, ch, name := newAuth(t)
		a := auth.NewAuthService[user](name, header, ssl, ch, newSecurity())
		resp := newBrowser(t, api(t, a), site).do(t, http.MethodPost, "/login?id=u-1")
		cookie := strings.Join(resp.Header.Values("Set-Cookie"), "\n")
		want := []string{header + "-app-ecoop-test=", "HttpOnly", "max-age=3600", "path=/"}
		if ssl {
			want = append(want, "secure", "SameSite=None")
		} else {
			want = append(want, "SameSite=Lax")
		}
		for _, w := range want {
			if !strings.Contains(strings.ToLower(cookie), strings.ToLower(w)) {
				t.Errorf("ssl=%v: Set-Cookie lacks %q:\n%s", ssl, w, cookie)
			}
		}
		if !ssl && strings.Contains(strings.ToLower(cookie), "secure") {
			t.Errorf("Secure cookie over plain HTTP would never be sent back:\n%s", cookie)
		}
		if resp.Header.Get(header) == "" {
			t.Errorf("ssl=%v: no token header for API clients", ssl)
		}
	}
}

// Two front ends on one API, in the same browser: each keeps its own
// session (scoped cookies), and signing out of one leaves the other.
func TestE2E_TwoFrontEndsOneBrowser(t *testing.T) {
	t.Parallel()
	a, _, _ := newAuth(t)
	base := api(t, a)
	jar, _ := cookiejar.New(nil)
	members := newBrowser(t, base, "https://members.ecoop.test", jar)
	tellers := newBrowser(t, base, "https://tellers.ecoop.test", jar)
	members.do(t, http.MethodPost, "/login?id=member-1")
	tellers.do(t, http.MethodPost, "/login?id=teller-9")
	if u, _ := members.me(t); u.ID != "member-1" {
		t.Fatalf("members site sees %+v", u)
	}
	if u, _ := tellers.me(t); u.ID != "teller-9" {
		t.Fatalf("tellers site sees %+v", u)
	}
	members.do(t, http.MethodPost, "/logout")
	if _, code := members.me(t); code != http.StatusUnauthorized {
		t.Fatalf("members site after sign-out: %d", code)
	}
	if u, code := tellers.me(t); code != http.StatusOK || u.ID != "teller-9" {
		t.Fatalf("signing out of one site signed out the other: %d %+v", code, u)
	}
}

// A phone and a laptop: the laptop sees the phone, signs it out, stays in.
func TestE2E_SignOutOtherDevices(t *testing.T) {
	t.Parallel()
	a, _, _ := newAuth(t)
	base := api(t, a)
	phone, laptop := newBrowser(t, base, site), newBrowser(t, base, site)
	phone.do(t, http.MethodPost, "/login?id=u-3")
	laptop.do(t, http.MethodPost, "/login?id=u-3")
	var devices map[string]int
	_ = json.NewDecoder(laptop.do(t, http.MethodGet, "/devices").Body).Decode(&devices)
	if devices["others"] != 1 {
		t.Fatalf("other devices = %v", devices)
	}
	laptop.do(t, http.MethodPost, "/logout-others")
	if _, code := phone.me(t); code != http.StatusUnauthorized {
		t.Fatalf("phone still signed in: %d", code)
	}
	if _, code := laptop.me(t); code != http.StatusOK {
		t.Fatalf("laptop signed itself out: %d", code)
	}
}
