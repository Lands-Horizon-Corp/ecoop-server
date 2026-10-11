package otp_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/otp"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sms"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/smtp"
)

func TestSadPath_StartMissingEverything(t *testing.T) {
	t.Parallel()
	err := otp.NewOTPService("", nil, nil, nil, nil, nil).Start(context.Background())
	for _, want := range []error{otp.ErrNoSecret, otp.ErrNoSecurity, otp.ErrNoCache, otp.ErrNoSMTP, otp.ErrNoSMS} {
		if !errors.Is(err, want) {
			t.Errorf("Start error %v does not include %v", err, want)
		}
	}
}

func TestSadPath_StartUnreachableDependencies(t *testing.T) {
	t.Parallel()
	requireInfra(t)
	stopped := cache.NewCacheService([]string{valkeyAddr}, valkeyPassword, nil) // never Run
	unstartedSMTP := smtp.NewSMTPService("h", 25, "", "", "a@ecoop.test", "", true, nil, nil)
	unstartedSMS := sms.NewSMSService("AC", "tok", "+15005550006", 160, true, nil, nil)
	err := otp.NewOTPService(secret, newSecurity(), stopped, unstartedSMTP, unstartedSMS, nil).Start(context.Background())
	for _, want := range []error{otp.ErrCacheUnreachable, otp.ErrSMTPUnreachable, otp.ErrSMSUnreachable} {
		if !errors.Is(err, want) {
			t.Errorf("Start error %v does not include %v", err, want)
		}
	}
}

func TestSadPath_NotStarted(t *testing.T) {
	t.Parallel()
	requireInfra(t)
	o := otp.NewOTPService(secret, newSecurity(), newCache(t), mockSMTP(t), mockSMS(t), nil)
	ctx := ctxT(t)
	check := func(stage string) {
		if _, err := o.Generate(ctx, "k"); !errors.Is(err, otp.ErrNotStarted) {
			t.Errorf("%s: Generate = %v", stage, err)
		}
		if _, err := o.Verify(ctx, "k", "123456"); !errors.Is(err, otp.ErrNotStarted) {
			t.Errorf("%s: Verify = %v", stage, err)
		}
		if err := o.Revoke(ctx, "k"); !errors.Is(err, otp.ErrNotStarted) {
			t.Errorf("%s: Revoke = %v", stage, err)
		}
		if _, err := o.GenerateSMTP(ctx, "k", smtp.SMTPRequest{To: "a@ecoop.test"}); !errors.Is(err, otp.ErrNotStarted) {
			t.Errorf("%s: GenerateSMTP = %v", stage, err)
		}
	}
	check("before Start")
	if err := o.Start(ctx); err != nil {
		t.Fatal(err)
	}
	_ = o.Stop(ctx)
	check("after Stop")
}

func TestSadPath_EmptyKey(t *testing.T) {
	t.Parallel()
	o := newOTP(t)
	ctx := ctxT(t)
	if _, err := o.Generate(ctx, ""); !errors.Is(err, otp.ErrEmptyKey) {
		t.Errorf("Generate = %v", err)
	}
	if _, err := o.Verify(ctx, "", "123456"); !errors.Is(err, otp.ErrEmptyKey) {
		t.Errorf("Verify = %v", err)
	}
	if err := o.Revoke(ctx, ""); !errors.Is(err, otp.ErrEmptyKey) {
		t.Errorf("Revoke = %v, want ErrEmptyKey like Generate and Verify", err)
	}
}

func TestSadPath_WrongMissingAndExpired(t *testing.T) {
	t.Parallel()
	o := newOTP(t)
	ctx := ctxT(t)

	k := key(t)
	code, _ := o.Generate(ctx, k)
	if ok, err := o.Verify(ctx, k, otherCode(code)); ok || err != nil {
		t.Errorf("wrong code = %v, %v; want false, nil", ok, err)
	}
	if ok, err := o.Verify(ctx, key(t), "123456"); ok || !errors.Is(err, otp.ErrCodeNotFound) {
		t.Errorf("unknown key = %v, %v; want ErrCodeNotFound", ok, err)
	}

	ch := newCache(t)
	o2 := startOTP(t, secret, ch, mockSMTP(t), mockSMS(t))
	expiring := key(t)
	code2, _ := o2.Generate(ctx, expiring)
	if _, err := ch.Expire(ctx, otp.KeyPrefix+expiring, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	if ok, err := o2.Verify(ctx, expiring, code2); ok || !errors.Is(err, otp.ErrCodeNotFound) {
		t.Errorf("expired code = %v, %v; want ErrCodeNotFound", ok, err)
	}
}

func TestSadPath_TooManyAttempts(t *testing.T) {
	t.Parallel()
	o := newOTP(t)
	ctx, k := ctxT(t), key(t)
	code, _ := o.Generate(ctx, k)
	for range otp.MaxAttempts {
		if ok, _ := o.Verify(ctx, k, otherCode(code)); ok {
			t.Fatal("wrong code accepted")
		}
	}
	if ok, err := o.Verify(ctx, k, code); ok || !errors.Is(err, otp.ErrTooManyAttempts) {
		t.Fatalf("Verify after %d wrong guesses = %v, %v; want ErrTooManyAttempts", otp.MaxAttempts, ok, err)
	}
	if _, err := o.Verify(ctx, k, code); !errors.Is(err, otp.ErrCodeNotFound) {
		t.Fatalf("code still there after lockout: %v", err)
	}
}

func TestSadPath_SendFailureRevokesTheCode(t *testing.T) {
	t.Parallel()
	ch := newCache(t)
	o := startOTP(t, secret, ch, mockSMTP(t), mockSMS(t))
	ctx := ctxT(t)
	cases := map[string]func(k string) error{
		"email": func(k string) error {
			_, err := o.GenerateSMTP(ctx, k, smtp.SMTPRequest{To: "not-an-email", Body: "{{.Code}}"})
			return err
		},
		"sms": func(k string) error {
			_, err := o.GenerateSMS(ctx, k, sms.SMSRequest{To: "0917", Body: "{{.Code}}"})
			return err
		},
		"bad template": func(k string) error {
			_, err := o.GenerateSMTP(ctx, k, smtp.SMTPRequest{To: "a@ecoop.test", Body: "{{.Code"})
			return err
		},
	}
	for name, send := range cases {
		t.Run(name, func(t *testing.T) {
			k := key(t)
			if err := send(k); !errors.Is(err, otp.ErrSendFailed) {
				t.Fatalf("err = %v, want ErrSendFailed", err)
			}
			if _, err := ch.Get(ctx, otp.KeyPrefix+k); !errors.Is(err, cache.ErrNotFound) {
				t.Fatalf("code left behind after a failed send: %v", err)
			}
		})
	}
}

func TestSadPath_CacheGoesAway(t *testing.T) {
	t.Parallel()
	ch := newCache(t)
	o := startOTP(t, secret, ch, mockSMTP(t), mockSMS(t))
	ctx, k := ctxT(t), key(t)
	code, _ := o.Generate(ctx, k)
	_ = ch.Stop(ctx)
	if _, err := o.Generate(ctx, key(t)); err == nil {
		t.Error("Generate succeeded with the cache stopped")
	}
	if ok, err := o.Verify(ctx, k, code); ok || err == nil {
		t.Errorf("Verify = %v, %v with the cache stopped", ok, err)
	}
}
