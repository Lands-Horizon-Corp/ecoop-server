package otp

import (
	"context"
	"errors"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sms"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/smtp"
)

const (
	CodeLength     = 6               // digits in a generated code
	CodeTTL        = 5 * time.Minute // how long a code can be verified
	MaxAttempts    = 5               // wrong guesses before the code is revoked
	CodeVar        = "Code"          // template variable the code is sent as: {{.Code}}
	KeyPrefix      = "otp:code:"     // cache key for a code's hash
	AttemptsPrefix = "otp:attempts:" // cache key for a code's guess counter
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
	ErrNotStarted       = errors.New("OTP service not started")
	ErrEmptyKey         = errors.New("OTP key is empty")
	ErrCodeNotFound     = errors.New("OTP code not found or expired")
	ErrTooManyAttempts  = errors.New("OTP too many wrong attempts; code revoked")
	ErrSendFailed       = errors.New("OTP code could not be sent")
)

type (
	OTPServices interface {
		Start(ctx context.Context) error
		Stop(ctx context.Context) error

		Generate(ctx context.Context, key string) (string, error)
		GenerateSMTP(ctx context.Context, key string, req smtp.SMTPRequest) (string, error)
		GenerateSMS(ctx context.Context, key string, req sms.SMSRequest) (string, error)

		Verify(ctx context.Context, key, code string) (bool, error)
		Revoke(ctx context.Context, key string) error
	}
)
