package otp

import (
	"context"
	"errors"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/security"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sms"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/smtp"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

type OTPService struct {
	secret string
	sec    security.SecurityServices
	ch     cache.CacheServices

	smtp smtp.SMTPServices
	sms  sms.SMSServices

	gate *utils.Gate
}

func NewOTPService(
	secret string,
	sec security.SecurityServices,
	ch cache.CacheServices,
	smtp smtp.SMTPServices,
	sms sms.SMSServices,
	inflight *utils.Inflight,
) OTPServices {
	return &OTPService{
		secret: secret,
		sec:    sec,
		ch:     ch,
		smtp:   smtp,
		sms:    sms,
		gate:   utils.NewGate(inflight),
	}
}

// Start implements [OTPServices].
func (o *OTPService) Start(ctx context.Context) error {
	var errs []error
	if o.secret == "" {
		errs = append(errs, ErrNoSecret)
	}
	if utils.IsNil(o.sec) {
		errs = append(errs, ErrNoSecurity)
	}
	if utils.IsNil(o.ch) {
		errs = append(errs, ErrNoCache)
	}
	if utils.IsNil(o.smtp) {
		errs = append(errs, ErrNoSMTP)
	}
	if utils.IsNil(o.sms) {
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
	if err := errors.Join(errs...); err != nil {
		return err
	}
	o.gate.Open()
	return nil
}

func (o *OTPService) Stop(ctx context.Context) error {
	return o.gate.Close(ctx)
}

func (o *OTPService) Generate(ctx context.Context, key string) (string, error) {
	done, ok := o.gate.Enter()
	if !ok {
		return "", ErrNotStarted
	}
	defer done()
	if key == "" {
		return "", ErrEmptyKey
	}
	code, err := utils.RandomDigits(CodeLength)
	if err != nil {
		return "", err
	}
	hash, err := o.sec.GeneratePassword(ctx, utils.Pepper(o.secret, code))
	if err != nil {
		return "", err
	}
	if err := o.ch.Set(ctx, KeyPrefix+key, hash, CodeTTL); err != nil {
		return "", err
	}
	if _, err := o.ch.Delete(ctx, AttemptsPrefix+key); err != nil {
		return "", err
	}
	return code, nil
}

func (o *OTPService) GenerateSMTP(ctx context.Context, key string, req smtp.SMTPRequest) (string, error) {
	code, err := o.Generate(ctx, key)
	if err != nil {
		return "", err
	}
	req.Vars = utils.CloneWith(req.Vars, CodeVar, code)
	if err := o.smtp.Send(ctx, req); err != nil {
		return "", errors.Join(ErrSendFailed, err, o.Revoke(ctx, key))
	}
	return code, nil
}

func (o *OTPService) GenerateSMS(ctx context.Context, key string, req sms.SMSRequest) (string, error) {
	code, err := o.Generate(ctx, key)
	if err != nil {
		return "", err
	}
	req.Vars = utils.CloneWith(req.Vars, CodeVar, code)
	if err := o.sms.Send(ctx, req); err != nil {
		return "", errors.Join(ErrSendFailed, err, o.Revoke(ctx, key))
	}
	return code, nil
}

func (o *OTPService) Verify(ctx context.Context, key, code string) (bool, error) {
	done, ok := o.gate.Enter()
	if !ok {
		return false, ErrNotStarted
	}
	defer done()
	if key == "" {
		return false, ErrEmptyKey
	}
	attempts, err := o.ch.Incr(ctx, AttemptsPrefix+key, CodeTTL)
	if err != nil {
		return false, err
	}
	if attempts > MaxAttempts {
		return false, errors.Join(ErrTooManyAttempts, o.revoke(ctx, key))
	}
	hash, err := o.ch.Get(ctx, KeyPrefix+key)
	if errors.Is(err, cache.ErrNotFound) {
		return false, ErrCodeNotFound
	}
	if err != nil {
		return false, err
	}
	match, err := o.sec.VerifyPassword(ctx, string(hash), utils.Pepper(o.secret, code))
	if err != nil || !match {
		return false, err
	}
	if consumed, err := o.ch.Delete(ctx, KeyPrefix+key); err != nil || !consumed {
		return false, err
	}
	_, err = o.ch.Delete(ctx, AttemptsPrefix+key)
	return true, err
}

func (o *OTPService) Revoke(ctx context.Context, key string) error {
	done, ok := o.gate.Enter()
	if !ok {
		return ErrNotStarted
	}
	defer done()
	if key == "" {
		return ErrEmptyKey
	}
	return o.revoke(ctx, key)
}

func (o *OTPService) revoke(ctx context.Context, key string) error {
	_, errCode := o.ch.Delete(ctx, KeyPrefix+key)
	_, errAttempts := o.ch.Delete(ctx, AttemptsPrefix+key)
	return errors.Join(errCode, errAttempts)
}
