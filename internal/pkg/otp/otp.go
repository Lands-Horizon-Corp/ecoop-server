package otp

import (
	"context"
	"errors"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/security"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sms"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/smtp"
)

type OTPService struct {
	secret string
	sec    security.SecurityServices
	ch     cache.CacheServices

	smtp smtp.SMTPServices
	sms  sms.SMSServices
}

func NewOTPService(
	secret string,
	sec security.SecurityServices,
	ch cache.CacheServices,
	smtp smtp.SMTPServices,
	sms sms.SMSServices,
) OTPServices {
	return &OTPService{
		secret: secret,
		sec:    sec,
		ch:     ch,
		smtp:   smtp,
		sms:    sms,
	}
}

// Start implements [OTPServices].
func (o *OTPService) Start(ctx context.Context) error {
	var errs []error
	if o.secret == "" {
		errs = append(errs, ErrNoSecret)
	}
	if o.sec == nil {
		errs = append(errs, ErrNoSecurity)
	}
	if o.ch == nil {
		errs = append(errs, ErrNoCache)
	}
	if o.smtp == nil {
		errs = append(errs, ErrNoSMTP)
	}
	if o.sms == nil {
		errs = append(errs, ErrNoSMS)
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	if err := o.ch.Ping(ctx); err != nil {
		errs = append(errs, errors.Join(ErrCacheUnreachable, err))
	}
	if err := o.smtp.Ping(ctx); err != nil {
		errs = append(errs, errors.Join(ErrSMTPUnreachable, err))
	}
	if err := o.sms.Ping(ctx); err != nil {
		errs = append(errs, errors.Join(ErrSMSUnreachable, err))
	}
	return errors.Join(errs...)
}

func (o *OTPService) Generate(ctx context.Context, key string) (string, error) {
	panic("unimplemented")
}

// Revoke implements [OTPServices].
func (o *OTPService) Revoke(ctx context.Context, key string) error {
	panic("unimplemented")
}

// Verify implements [OTPServices].
func (o *OTPService) Verify(ctx context.Context, key string, code string) (bool, error) {
	panic("unimplemented")
}
