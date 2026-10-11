package otp

import (
	"context"
	"errors"
)

var (
	ErrNoSecret         = errors.New("OTP service secret is not set")
	ErrNoSecurity       = errors.New("OTP service security service is not set")
	ErrNoCache          = errors.New("OTP service cache service is not set")
	ErrNoSMTP           = errors.New("OTP service SMTP service is not set")
	ErrNoSMS            = errors.New("OTP service SMS service is not set")
	ErrCacheUnreachable = errors.New("OTP service cache service is not reachable")
	ErrSMTPUnreachable  = errors.New("OTP service SMTP service is not reachable")
	ErrSMSUnreachable   = errors.New("OTP service SMS service is not reachable")
)

type (
	OTPServices interface {
		Start(ctx context.Context) error
		Generate(ctx context.Context, key string) (string, error)
		Verify(ctx context.Context, key, code string) (bool, error)
		Revoke(ctx context.Context, key string) error
	}
)
