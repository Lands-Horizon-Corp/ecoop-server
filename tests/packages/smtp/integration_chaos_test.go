package smtp_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/smtp"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Integration chaos: queue, rate limit, worker and SMTP server together,
// under outages, restarts, slow networks and several instances.

func TestIntegrationChaos_RateLimitHolds(t *testing.T) {
	t.Parallel()
	ctx := ctxT(t)
	svc := newService(t)
	to := inbox(t)
	const n = 5 * smtp.SendRateLimit
	for i := range n {
		if err := svc.Send(ctx, smtp.SMTPRequest{To: to, Subject: fmt.Sprint(i), Body: "x"}); err != nil {
			t.Fatal(err)
		}
	}
	start := time.Now()
	received(t, to, n)
	if min := minRateLimitedSpan(n); time.Since(start) < min {
		t.Fatalf("%d emails delivered in %v; the rate limit allows no faster than %v", n, time.Since(start), min)
	}
}

func TestIntegrationChaos_RateLimitIsSharedAcrossInstances(t *testing.T) {
	t.Parallel()
	// Two instances model two app replicas behind a load balancer: they share
	// the queue and the rate limit counter, so together they respect it.
	ctx := ctxT(t)
	a := newSMTP(t, serviceOpts{})
	b := newSMTP(t, serviceOpts{})
	for _, svc := range []smtp.SMTPServices{a, b} {
		if err := svc.Start(ctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = svc.Stop(context.Background()) })
	}
	to := inbox(t)
	const n = 5 * smtp.SendRateLimit
	for i := range n {
		svc := a
		if i%2 == 1 {
			svc = b
		}
		if err := svc.Send(ctx, smtp.SMTPRequest{To: to, Body: "x"}); err != nil {
			t.Fatal(err)
		}
	}
	start := time.Now()
	received(t, to, n)
	if min := minRateLimitedSpan(n); time.Since(start) < min {
		t.Fatalf("two instances delivered %d in %v; a shared limit allows no faster than %v", n, time.Since(start), min)
	}
}

func TestIntegrationChaos_RetriesAfterOutage(t *testing.T) {
	t.Parallel()
	p := newProxy(t)
	before := failedDeliveries(t)
	svc := newSMTP(t, serviceOpts{smtpAddr: p.Addr()})
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Stop(context.Background()) })
	to := inbox(t)

	p.SetDown(true)
	for range 3 {
		if err := svc.Send(ctxT(t), smtp.SMTPRequest{To: to, Body: "x"}); err != nil {
			t.Fatalf("queueing during an outage must succeed: %v", err)
		}
	}
	eventually(t, 10*time.Second, func() bool { return failedDeliveries(t) >= before+3 }, "deliveries should fail while the server is down")
	noneReceived(t, to)

	p.SetDown(false)
	received(t, to, 3)
}

func TestIntegrationChaos_NoEmailLostAcrossStop(t *testing.T) {
	t.Parallel()
	// Emails still queued, or mid-delivery, when Stop runs stay in the queue
	// and go out after the next Start.
	p := newProxy(t)
	svc := newSMTP(t, serviceOpts{smtpAddr: p.Addr()})
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	to := inbox(t)
	p.SetDelay(30 * time.Millisecond) // every server reply held: deliveries are slow
	const n = 15
	for i := range n {
		if err := svc.Send(ctxT(t), smtp.SMTPRequest{To: to, Subject: fmt.Sprint(i), Body: "x"}); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(200 * time.Millisecond)
	if err := svc.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	p.SetDelay(0)
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Stop(context.Background()) })
	got := received(t, to, n)
	subjects := map[string]bool{}
	for _, m := range got {
		subjects[m.Subject] = true
	}
	if len(subjects) != n {
		t.Fatalf("%d distinct emails arrived, want %d", len(subjects), n)
	}
}

func TestIntegrationChaos_StopRespectsItsDeadline(t *testing.T) {
	t.Parallel()
	// A delivery stuck on a stalled server must not hold Stop past its ctx.
	svc := newSMTP(t, serviceOpts{smtpAddr: stalledServer(t)})
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := svc.Send(ctxT(t), smtp.SMTPRequest{To: "a@b.co", Body: "x"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond) // let the worker pick it up and stall
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := svc.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop = %v, want DeadlineExceeded", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Stop overran its deadline: %v", d)
	}
}

func TestIntegrationChaos_RestartChurnUnderLoad(t *testing.T) {
	t.Parallel()
	svc := newSMTP(t, serviceOpts{})
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	to := inbox(t)
	var queued atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				case <-time.After(10 * time.Millisecond):
				}
				err := svc.Send(context.Background(), smtp.SMTPRequest{To: to, Body: "x"})
				switch {
				case err == nil:
					queued.Add(1)
				case !errors.Is(err, smtp.ErrNotStarted):
					t.Errorf("unexpected error during churn: %v", err)
				}
			}
		})
	}
	for range 8 {
		time.Sleep(30 * time.Millisecond)
		if err := svc.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
	t.Cleanup(func() { _ = svc.Stop(context.Background()) })
	n := int(queued.Load())
	t.Logf("%d emails queued across 8 restarts", n)
	received(t, to, n)
}

func TestIntegrationChaos_SharedTrackerDrainsWithOtherServices(t *testing.T) {
	t.Parallel()
	shared := utils.NewInflight()
	svc := newSMTP(t, serviceOpts{inflight: shared})
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	release := shared.Track() // another service's operation, still running
	stopped := make(chan error, 1)
	go func() { stopped <- svc.Stop(context.Background()) }()
	select {
	case <-stopped:
		t.Fatal("Stop returned while a shared operation was still running")
	case <-time.After(100 * time.Millisecond):
	}
	release()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Stop did not return after the shared operation finished")
	}
}

func TestIntegrationChaos_XSSUnderConcurrentLoad(t *testing.T) {
	t.Parallel()
	ctx := ctxT(t)
	svc := newService(t)
	to := inbox(t)
	var wg sync.WaitGroup
	payloads := make([]string, 0, len(xssPayloads))
	for _, p := range xssPayloads {
		payloads = append(payloads, p)
	}
	sort.Strings(payloads)
	for _, payload := range payloads {
		wg.Go(func() {
			if err := svc.Send(ctx, smtp.SMTPRequest{To: to, Name: payload, Body: fmt.Sprintf("<p>{{.Name}}</p>%s", payload)}); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	for _, m := range received(t, to, len(payloads)) {
		assertNoXSS(t, m.HTML)
	}
}
