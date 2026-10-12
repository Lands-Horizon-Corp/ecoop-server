//go:build integration

package auth_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/auth"
)

// Sync & race: many devices sign in at once, each with its own token, and
// one revoke signs them all out. Run with -race.
func TestRace_ManyDevices(t *testing.T) {
	t.Parallel()
	a, _, _ := newAuth(t)
	tokens := make([]string, 20)
	var wg sync.WaitGroup
	for i := range tokens {
		wg.Go(func() { tokens[i] = login(t, a, ana, site) })
	}
	wg.Wait()
	seen := map[string]bool{}
	for _, tok := range tokens {
		if seen[tok] {
			t.Fatal("two devices got the same token")
		}
		seen[tok] = true
	}
	if claims, err := a.GetActiveSessionClaims(ctxT(t), with(tokens[0], site)); err != nil || len(claims) != 19 {
		t.Fatalf("other devices = %d, %v", len(claims), err)
	}
	if err := a.RevokeUserSessions(ctxT(t), ana.ID); err != nil {
		t.Fatal(err)
	}
	for _, tok := range tokens {
		if _, err := a.VerifyCSRF(ctxT(t), tok); !errors.Is(err, auth.ErrInvalidToken) {
			t.Fatalf("a device survived the revoke: %v", err)
		}
	}
}
