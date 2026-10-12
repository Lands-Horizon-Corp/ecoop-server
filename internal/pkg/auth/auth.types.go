package auth

import (
	"context"
	"errors"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
)

const (
	MaxSessionTTL = 30 * 24 * time.Hour // longest session SetCSRF accepts
	TokenBytes    = 32                  // random bytes in a token
)

var (
	ErrNoCache       = errors.New("auth: needs a cache service")
	ErrNoSecurity    = errors.New("auth: needs a security service")
	ErrNoUserID      = errors.New("auth: claim has no user id")
	ErrNoRequest     = errors.New("auth: no request context")
	ErrInvalidExpiry = errors.New("auth: expiry must be between 1 second and MaxSessionTTL")
	ErrNoToken       = errors.New("auth: no session token in the request")
	ErrInvalidToken  = errors.New("auth: session token is invalid or expired")
	ErrSession       = errors.New("auth: session store failed")
)

type (
	ClaimWithID interface {
		GetID() string
	}

	// AUTHServices keeps signed-in sessions in the cache. A session is a
	// random token, sent to the browser as a header and an HttpOnly cookie
	// and read back from either; the cache holds only its hash, with the
	// claim encrypted.
	AUTHServices[T ClaimWithID] interface {
		// GetCSRF returns the claim of the request's session; VerifyCSRF the
		// claim of a token. Both return ErrNoToken or ErrInvalidToken when
		// there is no valid session.
		GetCSRF(ctx context.Context, c *app.RequestContext) (T, error)
		SetCSRF(ctx context.Context, c *app.RequestContext, claim T, expiry time.Duration) error
		ClearCSRF(ctx context.Context, c *app.RequestContext) error
		VerifyCSRF(ctx context.Context, token string) (T, error)

		// The request's user on other devices: whether they are signed in
		// elsewhere, and those sessions' claims.
		HasActiveSessionOnOtherDevice(ctx context.Context, c *app.RequestContext) (bool, error)
		GetActiveSessionClaims(ctx context.Context, c *app.RequestContext) ([]T, error)

		// RevokeUserSessions signs a user out everywhere (by id, e.g. from
		// an admin); RevokeAllUserSessions the request's user, this device
		// included; RevokeOtherDeviceSessions everywhere but this device.
		RevokeUserSessions(ctx context.Context, userID string) error
		RevokeAllUserSessions(ctx context.Context, c *app.RequestContext) error
		RevokeOtherDeviceSessions(ctx context.Context, c *app.RequestContext) error
	}
)
