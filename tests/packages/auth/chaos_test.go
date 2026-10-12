//go:build integration

package auth_test

import (
	"errors"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/auth"
)

// Chaos: sessions expire on their own, and expired ones drop out of the
// device list.
func TestChaos_Expiry(t *testing.T) {
	t.Parallel()
	a, _, _ := newAuth(t)
	short := request(site)
	if err := a.SetCSRF(ctxT(t), short, ana, time.Second); err != nil {
		t.Fatal(err)
	}
	expiring := string(short.Response.Header.Peek(header))
	staying := login(t, a, ana, site)
	time.Sleep(2100 * time.Millisecond)
	if _, err := a.VerifyCSRF(ctxT(t), expiring); !errors.Is(err, auth.ErrInvalidToken) {
		t.Fatalf("expired session: %v", err)
	}
	if other, err := a.HasActiveSessionOnOtherDevice(ctxT(t), with(staying, site)); err != nil || other {
		t.Fatalf("expired device still listed: %v, %v", other, err)
	}
}
