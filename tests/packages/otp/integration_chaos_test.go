//go:build integration

package otp_test

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/otp"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/smtp"
)

// Integration chaos: several instances behind a load balancer sharing one
// Valkey, and real email delivery under a burst.

func TestIntegrationChaos_GenerateOnOneVerifyOnAnother(t *testing.T) {
	t.Parallel()
	a := startOTP(t, secret, newCache(t), mockSMTP(t), mockSMS(t))
	b := startOTP(t, secret, newCache(t), mockSMTP(t), mockSMS(t))
	ctx, k := ctxT(t), key(t)
	code, _ := a.Generate(ctx, k)
	if ok, err := b.Verify(ctx, k, code); !ok || err != nil {
		t.Fatalf("instance b could not verify a's code: %v, %v", ok, err)
	}
	if _, err := a.Verify(ctx, k, code); !errors.Is(err, otp.ErrCodeNotFound) {
		t.Fatalf("code reused on instance a after b consumed it: %v", err)
	}
}

func TestIntegrationChaos_OneWinnerAcrossInstances(t *testing.T) {
	t.Parallel()
	var instances []otp.OTPServices
	for range otp.MaxAttempts {
		instances = append(instances, startOTP(t, secret, newCache(t), mockSMTP(t), mockSMS(t)))
	}
	for round := range 10 {
		k := key(t)
		code, _ := instances[0].Generate(ctxT(t), k)
		var wins atomic.Int64
		var wg sync.WaitGroup
		for _, o := range instances {
			wg.Go(func() {
				if ok, _ := o.Verify(ctxT(t), k, code); ok {
					wins.Add(1)
				}
			})
		}
		wg.Wait()
		if wins.Load() != 1 {
			t.Fatalf("round %d: %d instances accepted the same code", round, wins.Load())
		}
	}
}

func TestIntegrationChaos_AttemptsSharedAcrossInstances(t *testing.T) {
	t.Parallel()
	a := startOTP(t, secret, newCache(t), mockSMTP(t), mockSMS(t))
	b := startOTP(t, secret, newCache(t), mockSMTP(t), mockSMS(t))
	ctx, k := ctxT(t), key(t)
	code, _ := a.Generate(ctx, k)
	for i := range otp.MaxAttempts {
		inst := a
		if i%2 == 1 {
			inst = b
		}
		_, _ = inst.Verify(ctx, k, otherCode(code))
	}
	if _, err := b.Verify(ctx, k, code); !errors.Is(err, otp.ErrTooManyAttempts) {
		t.Fatalf("spreading guesses over instances bypassed the limit: %v", err)
	}
}

func TestIntegrationChaos_EmailBurst(t *testing.T) {
	t.Parallel()
	ch := newCache(t)
	q := newQueue(t, ch)
	o := startOTP(t, secret, ch, realSMTP(t, q), mockSMS(t))
	type sent struct{ key, to, code string }
	const n = 15
	results := make([]sent, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			k, to := key(t), inbox(t)
			code, err := o.GenerateSMTP(ctxT(t), k, smtp.SMTPRequest{To: to, Body: fmt.Sprintf("<p>#%d code {{.Code}}</p>", i)})
			if err != nil {
				t.Error(err)
				return
			}
			results[i] = sent{k, to, code}
		})
	}
	wg.Wait()
	for i, r := range results {
		if r.code == "" {
			continue
		}
		html := emailHTML(t, r.to)
		if !strings.Contains(html, r.code) || !strings.Contains(html, fmt.Sprintf("#%d", i)) {
			t.Fatalf("email %d does not carry its own code %s:\n%s", i, r.code, html)
		}
		if ok, err := o.Verify(ctxT(t), r.key, r.code); !ok || err != nil {
			t.Fatalf("emailed code %d does not verify: %v, %v", i, ok, err)
		}
	}
}
