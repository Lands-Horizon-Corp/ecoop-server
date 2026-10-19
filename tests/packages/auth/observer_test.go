//go:build integration

package auth_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/auth"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/tests/internal/obstest"
)

func observedAuth(t testing.TB) (auth.AUTHServices[user], *observability.Service) {
	t.Helper()
	ch := newCache(t)
	o := obstest.New(t)
	name := fmt.Sprintf("authobs%d%d", time.Now().UnixNano(), seq.Add(1))
	t.Cleanup(func() {
		keys, _ := ch.Keys(ctxT(t), name+":*")
		for _, k := range keys {
			_, _ = ch.Delete(ctxT(t), k)
		}
	})
	return auth.NewAuthService[user](name, header, false, ch, newSecurity(), auth.WithObserver(o)), o
}

func authCount(o *observability.Service, op, outcome string) float64 {
	return obstest.Count(o, "auth", op, outcome)
}

// Signing in, reading the session and signing out are each one counted operation.
func TestObserver_SessionLifecycleIsCounted(t *testing.T) {
	a, o := observedAuth(t)
	u := user{ID: "u-1"}
	c := request("https://app.test")
	if err := a.SetCSRF(ctxT(t), c, u, time.Hour); err != nil {
		t.Fatal(err)
	}
	token := string(c.Response.Header.Peek(header))
	if _, err := a.VerifyCSRF(ctxT(t), token); err != nil {
		t.Fatal(err)
	}
	if err := a.ClearCSRF(ctxT(t), c); err != nil {
		t.Fatal(err)
	}
	for op, want := range map[string]float64{"set_session": 1, "load_session": 1, "clear_session": 1} {
		if got := authCount(o, op, "success"); got != want {
			t.Errorf("%s success = %v, want %v", op, got, want)
		}
	}
}

// A visitor who is not signed in is an answer, not a fault: a missing or invalid
// token is skipped, so it cannot raise the error rate.
func TestObserver_NoTokenAndBadTokenAreSkippedNotErrors(t *testing.T) {
	a, o := observedAuth(t)
	if _, err := a.GetCSRF(ctxT(t), request("https://app.test")); !errors.Is(err, auth.ErrNoToken) {
		t.Fatalf("GetCSRF without a token = %v, want ErrNoToken", err)
	}
	if _, err := a.VerifyCSRF(ctxT(t), "not-a-real-token"); !errors.Is(err, auth.ErrInvalidToken) {
		t.Fatalf("VerifyCSRF = %v, want ErrInvalidToken", err)
	}
	if got := authCount(o, "load_session", "skipped"); got != 2 {
		t.Fatalf("load_session skipped = %v, want 2", got)
	}
	if got := authCount(o, "load_session", "error"); got != 0 {
		t.Fatalf("unauthenticated visitors counted as %v errors", got)
	}
}

// A refused sign-in (no user id) is an error of set_session.
func TestObserver_ARefusedSignInIsAnError(t *testing.T) {
	a, o := observedAuth(t)
	if err := a.SetCSRF(ctxT(t), request("https://app.test"), user{}, time.Hour); !errors.Is(err, auth.ErrNoUserID) {
		t.Fatalf("SetCSRF = %v, want ErrNoUserID", err)
	}
	if got := authCount(o, "set_session", "error"); got != 1 {
		t.Fatalf("set_session error = %v, want 1", got)
	}
}

// No token, session id or user id is ever recorded.
func TestObserver_NoTokenOrUserIDIsRecorded(t *testing.T) {
	a, o := observedAuth(t)
	u := user{ID: "member-4242-secret-id"}
	c := request("https://app.test")
	if err := a.SetCSRF(ctxT(t), c, u, time.Hour); err != nil {
		t.Fatal(err)
	}
	token := string(c.Response.Header.Peek(header))
	_, _ = a.VerifyCSRF(ctxT(t), token)
	for _, sm := range o.Snapshot() {
		blob := sm.Name
		for k, v := range sm.Attrs {
			blob += " " + k + "=" + v
		}
		if strings.Contains(blob, token) || strings.Contains(blob, u.ID) {
			t.Fatalf("a token or user id leaked into the registry: %s", blob)
		}
	}
}

// No observer, and a bare struct, are fine.
func TestObserver_NoneIsFine(t *testing.T) {
	a, _, _ := newAuth(t)
	c := request("https://app.test")
	if err := a.SetCSRF(ctxT(t), c, user{ID: "u-2"}, time.Hour); err != nil {
		t.Fatal(err)
	}
	var zero auth.AuthService[user]
	if err := zero.SetCSRF(ctxT(t), c, user{ID: "u-3"}, time.Hour); err == nil {
		t.Fatal("a zero service must return an error, not panic")
	}
}
