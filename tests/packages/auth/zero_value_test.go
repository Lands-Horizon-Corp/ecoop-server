//go:build integration

package auth_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/auth"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
)

// Zero value & absence: missing dependencies, a nil request, and a token
// kept after sign-out.
func TestZeroValue_Absence(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var typedNil *cache.CacheService
	empty := auth.NewAuthService[user]("x", header, false, typedNil, nil)
	if err := empty.SetCSRF(ctx, request(site), ana, time.Hour); !errors.Is(err, auth.ErrNoCache) || !errors.Is(err, auth.ErrNoSecurity) {
		t.Errorf("missing dependencies: %v", err)
	}
	if _, err := (&auth.AuthService[user]{}).VerifyCSRF(ctx, "t"); !errors.Is(err, auth.ErrNoCache) {
		t.Errorf("zero struct: %v", err)
	}
	a, _, _ := newAuth(t)
	if err := a.SetCSRF(ctxT(t), nil, ana, time.Hour); !errors.Is(err, auth.ErrNoRequest) {
		t.Errorf("nil request: %v", err)
	}
	if _, err := a.GetCSRF(ctxT(t), nil); !errors.Is(err, auth.ErrNoToken) {
		t.Errorf("nil request: %v", err)
	}
	token := login(t, a, ana, site) // a token kept after sign-out is dead
	_ = a.ClearCSRF(ctxT(t), with(token, site))
	if _, err := a.VerifyCSRF(ctxT(t), token); !errors.Is(err, auth.ErrInvalidToken) {
		t.Errorf("token kept after sign-out: %v", err)
	}
}
