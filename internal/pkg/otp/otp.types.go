package otp

import "context"

type (
	OTPServices interface {
		Generate(ctx context.Context, key string) (string, error)
		Verify(ctx context.Context, key, code string) (bool, error)
		Revoke(ctx context.Context, key string) error
	}
)
