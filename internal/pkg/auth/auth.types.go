package auth

import (
	"context"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
)

type (
	ClaimWithID interface {
		GetID() string
	}

	AUTHService[T ClaimWithID] interface {
		// CSRF Token Lifecycle
		GetCSRF(ctx context.Context, c *app.RequestContext) (T, error)
		SetCSRF(ctx context.Context, c *app.RequestContext, claim T, expiry time.Duration) error
		ClearCSRF(ctx context.Context, c *app.RequestContext) error
		VerifyCSRF(ctx context.Context, token string) (T, error)

		// Session & Multi-Device Management
		HasActiveSessionOnOtherDevice(ctx context.Context, c *app.RequestContext) (bool, error)
		GetActiveSessionClaims(ctx context.Context, c *app.RequestContext) ([]T, error)

		RevokeUserSessions(ctx context.Context, userID string) error
		RevokeAllUserSessions(ctx context.Context, c *app.RequestContext) error
		RevokeOtherDeviceSessions(ctx context.Context, c *app.RequestContext) error
	}
)
