package otp

import (
	"context"
	"errors"
	"fmt"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/security"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sms"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/smtp"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

type OTPService struct {
	secret string
	sec    security.ISecurityService
	ch     cache.ICacheService

	smtp smtp.ISMTPService
	sms  sms.ISMSServices

	gate *utils.Gate
	obs  observability.IObserver
}

// WithObserver gives the service an observer for its metrics, traces and logs.
// Without one it observes nothing. Only the operation name and its outcome are
// ever recorded: never a code, a key, a hash or an address.
func WithObserver(o observability.IObserver) Option {
	return func(s *OTPService) { s.obs = observability.Pick(o) }
}

// begin starts the Operation of one call. A service built as a bare struct
// literal has no observer, so it falls back to Nop without writing.
func (o *OTPService) begin(ctx context.Context, name string) (context.Context, observability.Operation) {
	if o.obs == nil {
		return observability.Nop().Begin(ctx, observability.ComponentOTP, name)
	}
	return o.obs.Begin(ctx, observability.ComponentOTP, name)
}

func NewOTPService(
	secret string,
	sec security.ISecurityService,
	ch cache.ICacheService,
	smtp smtp.ISMTPService,
	sms sms.ISMSServices,
	inflight *utils.Inflight,
	opts ...Option,
) IOTPService {
	s := &OTPService{
		secret: secret,
		sec:    sec,
		ch:     ch,
		smtp:   smtp,
		sms:    sms,
		gate:   utils.NewGate(inflight),
		obs:    observability.Nop(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	return s
}

// Start implements [IOTPService].
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

func (o *OTPService) Generate(ctx context.Context, key string) (_ string, err error) {
	done, ok := o.gate.Enter()
	if !ok {
		return "", ErrNotStarted
	}
	defer done()
	if key == "" {
		return "", ErrEmptyKey
	}
	ctx, op := o.begin(ctx, "generate")
	defer func() { op.End(err) }()
	code, err := utils.RandomDigits(CodeLength)
	if err != nil {
		return "", fmt.Errorf("otp: generating code: %w", err)
	}
	hash, err := o.sec.GeneratePassword(ctx, utils.Pepper(o.secret, code))
	if err != nil {
		return "", fmt.Errorf("otp: hashing code: %w", err)
	}
	if err := o.ch.Set(ctx, KeyPrefix+key, hash, CodeTTL); err != nil {
		return "", fmt.Errorf("otp: storing code: %w", err)
	}
	if _, err := o.ch.Delete(ctx, AttemptsPrefix+key); err != nil {
		return "", fmt.Errorf("otp: resetting attempts: %w", err)
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

func (o *OTPService) Verify(ctx context.Context, key, code string) (matched bool, err error) {
	done, ok := o.gate.Enter()
	if !ok {
		return false, ErrNotStarted
	}
	defer done()
	if key == "" {
		return false, ErrEmptyKey
	}
	ctx, op := o.begin(ctx, "verify")
	defer func() {
		// A wrong, expired or exhausted code is an answer, not a fault of the
		// service. It ends as skipped so a guessing attack shows up on its own
		// and never raises the error rate; a store failure stays an error. The
		// error the caller receives is not changed.
		seen := err
		if !matched && (err == nil || errors.Is(err, ErrCodeNotFound) || errors.Is(err, ErrTooManyAttempts)) {
			op.SetOutcome(observability.OutcomeSkipped)
			seen = nil
		}
		op.End(seen)
	}()
	attempts, err := o.ch.Incr(ctx, AttemptsPrefix+key, CodeTTL)
	if err != nil {
		return false, fmt.Errorf("otp: counting attempts: %w", err)
	}
	if attempts > MaxAttempts {
		return false, errors.Join(ErrTooManyAttempts, o.revoke(ctx, key))
	}
	hash, err := o.ch.Get(ctx, KeyPrefix+key)
	if errors.Is(err, cache.ErrNotFound) {
		return false, ErrCodeNotFound
	}
	if err != nil {
		return false, fmt.Errorf("otp: reading code: %w", err)
	}
	match, err := o.sec.VerifyPassword(ctx, string(hash), utils.Pepper(o.secret, code))
	if err != nil {
		return false, fmt.Errorf("otp: checking code: %w", err)
	}
	if !match {
		return false, nil
	}
	consumed, err := o.ch.Delete(ctx, KeyPrefix+key)
	if err != nil {
		return false, fmt.Errorf("otp: consuming code: %w", err)
	}
	if !consumed {
		return false, nil
	}
	if _, err := o.ch.Delete(ctx, AttemptsPrefix+key); err != nil {
		return true, fmt.Errorf("otp: clearing attempts: %w", err)
	}
	return true, nil
}

func (o *OTPService) Revoke(ctx context.Context, key string) (err error) {
	done, ok := o.gate.Enter()
	if !ok {
		return ErrNotStarted
	}
	defer done()
	if key == "" {
		return ErrEmptyKey
	}
	ctx, op := o.begin(ctx, "revoke")
	defer func() { op.End(err) }()
	return o.revoke(ctx, key)
}

func (o *OTPService) revoke(ctx context.Context, key string) error {
	_, errCode := o.ch.Delete(ctx, KeyPrefix+key)
	_, errAttempts := o.ch.Delete(ctx, AttemptsPrefix+key)
	return errors.Join(errCode, errAttempts)
}
