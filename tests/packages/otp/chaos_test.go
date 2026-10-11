//go:build integration

package otp_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/otp"
)

// Chaos: random operation sequences checked against a model, random
// deadlines, and dropped connections.

func seed(t *testing.T) uint64 {
	s := uint64(time.Now().UnixNano())
	t.Logf("chaos seed: %d", s)
	return s
}

func TestChaos_RandomOpsMatchModel(t *testing.T) {
	t.Parallel()
	o := newOTP(t)
	ctx := ctxT(t)
	r := rand.New(rand.NewPCG(seed(t), 1))
	type state struct {
		code     string
		valid    bool
		attempts int
	}
	keys := []string{key(t), key(t), key(t)}
	model := map[string]*state{}
	for _, k := range keys {
		model[k] = &state{}
	}
	for step := range 300 {
		k := keys[r.IntN(len(keys))]
		m := model[k]
		switch r.IntN(5) {
		case 0: // generate
			code, err := o.Generate(ctx, k)
			if err != nil {
				t.Fatalf("step %d Generate: %v", step, err)
			}
			*m = state{code: code, valid: true}
		case 1: // revoke
			if err := o.Revoke(ctx, k); err != nil {
				t.Fatalf("step %d Revoke: %v", step, err)
			}
			*m = state{}
		default: // verify, right or wrong
			guess := otherCode(m.code)
			if m.valid && r.IntN(3) == 0 {
				guess = m.code
			}
			ok, err := o.Verify(ctx, k, guess)
			m.attempts++
			switch {
			case m.attempts > otp.MaxAttempts:
				if ok || !errors.Is(err, otp.ErrTooManyAttempts) {
					t.Fatalf("step %d: want lockout, got %v, %v", step, ok, err)
				}
				*m = state{}
			case !m.valid:
				if ok || !errors.Is(err, otp.ErrCodeNotFound) {
					t.Fatalf("step %d: want ErrCodeNotFound, got %v, %v", step, ok, err)
				}
			case guess == m.code:
				if !ok || err != nil {
					t.Fatalf("step %d: right code rejected: %v, %v", step, ok, err)
				}
				*m = state{}
			default:
				if ok || err != nil {
					t.Fatalf("step %d: wrong code = %v, %v", step, ok, err)
				}
			}
		}
	}
}

func TestChaos_RandomDeadlines(t *testing.T) {
	t.Parallel()
	o := newOTP(t)
	r := rand.New(rand.NewPCG(seed(t), 2))
	for i := range 100 {
		k := key(t)
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(r.IntN(30))*time.Millisecond)
		var code string
		var err error
		mustNotPanic(t, "Generate", func() { code, err = o.Generate(ctx, k) })
		cancel()
		if err != nil {
			continue
		}
		// A code that was returned must work.
		if ok, verr := o.Verify(ctxT(t), k, code); !ok || verr != nil {
			t.Fatalf("op %d: returned code does not verify: %v, %v", i, ok, verr)
		}
	}
}

func TestChaos_ConnectionDrops(t *testing.T) {
	t.Parallel()
	p := newProxy(t)
	o := startOTP(t, secret, newCache(t, p.Addr()), mockSMTP(t), mockSMS(t))
	ctx := ctxT(t)
	for i := range 60 {
		k := key(t)
		if i%10 == 5 {
			p.DropAll()
		}
		var code string
		var err error
		mustNotPanic(t, "Generate", func() { code, err = o.Generate(ctx, k) })
		if err != nil {
			continue
		}
		mustNotPanic(t, "Verify", func() {
			ok, verr := o.Verify(ctx, k, code)
			if verr == nil && !ok {
				t.Errorf("op %d: right code rejected without an error", i)
			}
		})
	}
	// After the drops the service recovers.
	k := key(t)
	code, err := o.Generate(ctx, k)
	if err != nil {
		t.Fatalf("Generate after drops: %v", err)
	}
	if ok, err := o.Verify(ctx, k, code); !ok || err != nil {
		t.Fatal(fmt.Sprint("Verify after drops: ", ok, err))
	}
}
