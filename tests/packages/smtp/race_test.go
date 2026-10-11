package smtp_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/smtp"
)

// Sync & race: one service shared by many goroutines. Run with -race.

const workers = 32

func TestRace_ConcurrentSendsAllDelivered(t *testing.T) {
	ctx := ctxT(t)
	svc := newService(t)
	to := inbox(t)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			if err := svc.Send(ctx, smtp.SMTPRequest{To: to, Name: fmt.Sprint(w), Subject: fmt.Sprint(w), Body: "<p>{{.Name}}</p>"}); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	seen := map[string]bool{}
	for _, m := range received(t, to, workers) {
		if seen[m.Subject] {
			t.Errorf("duplicate delivery of %q", m.Subject)
		}
		seen[m.Subject] = true
	}
}

func TestRace_ConcurrentFormatWithOwnVars(t *testing.T) {
	svc := mockService(t)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for i := range 100 {
				want := fmt.Sprintf("%d-%d", w, i)
				out, err := svc.Format(context.Background(), smtp.SMTPRequest{
					Name: want, Body: "{{.Name}}|{{.N}}", Vars: map[string]string{"N": want},
				})
				if err != nil || out.Body != want+"|"+want {
					t.Errorf("got %q, %v; want %q", out.Body, err, want+"|"+want)
					return
				}
			}
		})
	}
	wg.Wait()
}

func TestRace_StartStopSendChurn(t *testing.T) {
	svc := newSMTP(t, serviceOpts{})
	_ = svc.Start(context.Background())
	to := inbox(t)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				mustNotPanic(t, "Send during churn", func() {
					if err := svc.Send(context.Background(), smtp.SMTPRequest{To: to, Body: "x"}); err != nil && !errors.Is(err, smtp.ErrNotStarted) {
						t.Errorf("Send: %v", err)
					}
					_ = svc.Ping(context.Background())
				})
			}
		})
	}
	for range 20 {
		_ = svc.Stop(context.Background())
		_ = svc.Start(context.Background())
	}
	close(stop)
	wg.Wait()
	_ = svc.Stop(context.Background())
}

func TestRace_ConcurrentStops(t *testing.T) {
	svc := newService(t)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			if err := svc.Stop(context.Background()); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if err := svc.Send(context.Background(), smtp.SMTPRequest{To: "a@b.co", Body: "x"}); !errors.Is(err, smtp.ErrNotStarted) {
		t.Fatalf("Send after concurrent Stops = %v, want ErrNotStarted", err)
	}
}

// Sharing one Vars map across concurrent sends is not tested here: Format
// writes "Name" into the caller's map, so it would be a concurrent map write,
// which crashes the whole test binary rather than failing one test. The
// mutation itself is caught in TestZeroValue_FormatDoesNotMutateCallerVars.
