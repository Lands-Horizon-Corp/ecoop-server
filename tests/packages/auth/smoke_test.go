//go:build integration

package auth_test

import (
	"errors"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/auth"
)

// Smoke: sign in, read the session back, sign out.
func TestSmoke(t *testing.T) {
	t.Parallel()
	a, _, _ := newAuth(t)
	token := login(t, a, ana, site)
	got, err := a.GetCSRF(ctxT(t), with(token, site))
	if err != nil || got != ana {
		t.Fatalf("GetCSRF = %+v, %v", got, err)
	}
	if err := a.ClearCSRF(ctxT(t), with(token, site)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.VerifyCSRF(ctxT(t), token); !errors.Is(err, auth.ErrInvalidToken) {
		t.Fatalf("after sign-out: %v", err)
	}
}
