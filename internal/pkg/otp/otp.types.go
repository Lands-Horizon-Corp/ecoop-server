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
	ErrNoSecret         = errors.New("otp: secret is not set")
	ErrNoSecurity       = errors.New("otp: security service is not set")
	ErrNoCache          = errors.New("otp: cache service is not set")
	ErrNoSMTP           = errors.New("otp: smtp service is not set")
	ErrNoSMS            = errors.New("otp: sms service is not set")
	ErrCacheUnreachable = errors.New("otp: cache service is not reachable")
	ErrSMTPUnreachable  = errors.New("otp: smtp service is not reachable")
	ErrSMSUnreachable   = errors.New("otp: sms service is not reachable")
	ErrNotStarted       = errors.New("otp: service not started")
	ErrEmptyKey         = errors.New("otp: key is empty")
	ErrCodeNotFound     = errors.New("otp: code not found or expired")
	ErrTooManyAttempts  = errors.New("otp: too many wrong attempts; code revoked")
	ErrSendFailed       = errors.New("otp: code could not be sent")
)

type (
	// Option configures an OTPService: WithObserver.
	Option func(*OTPService)

	IOTPService interface {
		Start(ctx context.Context) error
		Stop(ctx context.Context) error

		Generate(ctx context.Context, key string) (string, error)
		GenerateSMTP(ctx context.Context, key string, req smtp.SMTPRequest) (string, error)
		GenerateSMS(ctx context.Context, key string, req sms.SMSRequest) (string, error)

		Verify(ctx context.Context, key, code string) (bool, error)
		Revoke(ctx context.Context, key string) error
	}
)
