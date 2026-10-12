//go:build integration

package auth_test

import (
	"errors"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/auth"
)

// Sad path: missing, wrong and expired sessions, and bad sign-ins.
func TestSad_Refusals(t *testing.T) {
	t.Parallel()
	a, _, _ := newAuth(t)
	ctx := ctxT(t)
	for name, c := range map[string]struct{ err, want error }{
		"no token":      {func() error { _, err := a.GetCSRF(ctx, request(site)); return err }(), auth.ErrNoToken},
		"unknown token": {func() error { _, err := a.VerifyCSRF(ctx, "made-up"); return err }(), auth.ErrInvalidToken},
		"no user id":    {a.SetCSRF(ctx, request(site), user{}, time.Hour), auth.ErrNoUserID},
		"zero expiry":   {a.SetCSRF(ctx, request(site), ana, 0), auth.ErrInvalidExpiry},
		"too long":      {a.SetCSRF(ctx, request(site), ana, auth.MaxSessionTTL+time.Hour), auth.ErrInvalidExpiry},
		"devices, signed out": {func() error {
			_, err := a.HasActiveSessionOnOtherDevice(ctx, request(site))
			return err
		}(), auth.ErrNoToken},
	} {
		if !errors.Is(c.err, c.want) {
			t.Errorf("%s: %v, want %v", name, c.err, c.want)
		}
	}
}
