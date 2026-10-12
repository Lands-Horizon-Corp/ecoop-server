//go:build integration

package auth_test

import (
	"errors"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/auth"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
)

// Integration chaos: app instances behind a load balancer share sessions
// through the cache: sign in on one, use and revoke on another.
func TestIntegrationChaos_Replicas(t *testing.T) {
	t.Parallel()
	shared := newCache(t)
	a, _, name := newAuth(t, shared)
	b := auth.NewAuthService[user](name, header, false, cache.CacheServices(newCache(t)), newSecurity())
	token := login(t, a, ana, site)
	if got, err := b.GetCSRF(ctxT(t), with(token, site)); err != nil || got != ana {
		t.Fatalf("other instance = %+v, %v", got, err)
	}
	if err := b.RevokeUserSessions(ctxT(t), ana.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := a.VerifyCSRF(ctxT(t), token); !errors.Is(err, auth.ErrInvalidToken) {
		t.Fatalf("revoked on one instance, still valid on another: %v", err)
	}
}
