//go:build integration

package smtp_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/queue"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/smtp"
	"github.com/Lands-Horizon-Corp/ecoop-server/tests/internal/qns"
)

func TestSadPath_StartRejectsBadConfigAllAtOnce(t *testing.T) {
	t.Parallel()
	err := smtp.NewSMTPService("", 0, "", "", "not-an-email", "", true, nil, nil).Start(context.Background())
	for _, want := range []error{smtp.ErrInvalidSender, smtp.ErrInvalidHost, smtp.ErrInvalidPort, smtp.ErrNoQueue} {
		if !errors.Is(err, want) {
			t.Errorf("Start error %v does not include %v", err, want)
		}
	}
	for _, port := range []int{-1, 65536, 100000} {
		if err := smtp.NewSMTPService("h", port, "", "", "a@b.co", "", true, nil, nil).Start(context.Background()); !errors.Is(err, smtp.ErrInvalidPort) {
			t.Errorf("port %d: err = %v, want ErrInvalidPort", port, err)
		}
	}
}

func TestSadPath_InvalidRecipients(t *testing.T) {
	t.Parallel()
	svc := mockService(t)
	for _, to := range []string{"", "plain", "@ecoop.test", "a@", "a b@ecoop.test", "a@b.co, c@d.co", "<>", "a@b.co\r\nBcc: x@evil.test"} {
		if err := svc.Send(ctxT(t), smtp.SMTPRequest{To: to, Body: "x"}); !errors.Is(err, smtp.ErrInvalidRecipient) {
			t.Errorf("To %q: err = %v, want ErrInvalidRecipient", to, err)
		}
	}
}

func TestSadPath_TemplateErrors(t *testing.T) {
	t.Parallel()
	svc := mockService(t)
	cases := map[string]struct {
		body string
		want error
	}{
		"unclosed action": {"{{ .Name", smtp.ErrParseTemplate},
		"unknown func":    {"{{ nosuchfunc }}", smtp.ErrParseTemplate},
		"field on string": {"{{ .Name.Field }}", smtp.ErrExecuteTemplate},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			err := svc.Send(ctxT(t), smtp.SMTPRequest{To: "a@b.co", Name: "x", Body: c.body})
			if !errors.Is(err, smtp.ErrFormatBody) || !errors.Is(err, c.want) {
				t.Fatalf("err = %v, want ErrFormatBody + %v", err, c.want)
			}
		})
	}
}

func TestSadPath_UnreachableServer(t *testing.T) {
	t.Parallel()
	before := failedDeliveries(t)
	svc := newSMTP(t, serviceOpts{smtpAddr: "127.0.0.1:1"})
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Stop(context.Background()) })
	ctx := ctxT(t)
	if err := svc.Ping(ctx); !errors.Is(err, smtp.ErrConnect) {
		t.Errorf("Ping = %v, want ErrConnect", err)
	}
	// Send only queues; the failed delivery shows up on the queue and is retried.
	to := inbox(t)
	if err := svc.Send(ctx, smtp.SMTPRequest{To: to, Body: "x"}); err != nil {
		t.Fatalf("Send = %v; queueing must not depend on the SMTP server", err)
	}
	eventually(t, 10*time.Second, func() bool { return failedDeliveries(t) > before }, "delivery should fail and be retried")
	noneReceived(t, to)
}

func TestSadPath_QueueUnavailable(t *testing.T) {
	t.Parallel()
	// Jobs and the rate limit live in Valkey, so Start refuses to run on a
	// queue that is not running instead of failing on the first Send.
	requireMailpit(t)
	ch := cache.NewCacheService([]string{queueAddr}, queuePassword, nil)
	err := smtp.NewSMTPService(smtpHost, smtpPort, "", "", sender, "", true, queue.NewQueueService(ch, queue.WithNamespace(qns.For(t, queueAddr, queuePassword))), nil).Start(ctxT(t))
	if !errors.Is(err, smtp.ErrQueueUnavailable) || !errors.Is(err, queue.ErrNotStarted) {
		t.Fatalf("Start = %v, want ErrQueueUnavailable + queue.ErrNotStarted", err)
	}
}

func TestSadPath_QueueStoppedAfterStart(t *testing.T) {
	t.Parallel()
	// If the queue stops while running, Send reports it rather than
	// dropping the email.
	requireMailpit(t)
	q := newQueue(t)
	svc := smtp.NewSMTPService(smtpHost, smtpPort, "", "", sender, "", true, q, nil)
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Stop(context.Background()) })
	_ = q.Stop(context.Background())
	err := svc.Send(ctxT(t), smtp.SMTPRequest{To: "a@b.co", Body: "x"})
	if !errors.Is(err, smtp.ErrEnqueue) || !errors.Is(err, queue.ErrNotStarted) {
		t.Fatalf("Send with a stopped queue = %v, want ErrEnqueue + queue.ErrNotStarted", err)
	}
}

func TestSadPath_DeadContextDeliversNothing(t *testing.T) {
	t.Parallel()
	svc := newService(t)
	to := inbox(t)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancel2 := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel2()
	for name, ctx := range map[string]context.Context{"cancelled": cancelled, "expired": expired} {
		if err := svc.Send(ctx, smtp.SMTPRequest{To: to, Body: "x"}); err == nil {
			t.Errorf("%s ctx: Send succeeded", name)
		}
	}
	noneReceived(t, to)
}

func TestSadPath_SendBeforeStartAndAfterStop(t *testing.T) {
	t.Parallel()
	svc := newSMTP(t, serviceOpts{})
	to := inbox(t)
	if err := svc.Send(ctxT(t), smtp.SMTPRequest{To: to, Body: "x"}); !errors.Is(err, smtp.ErrNotStarted) {
		t.Errorf("before Start: %v", err)
	}
	_ = svc.Start(context.Background())
	_ = svc.Stop(context.Background())
	if err := svc.Send(ctxT(t), smtp.SMTPRequest{To: to, Body: "x"}); !errors.Is(err, smtp.ErrNotStarted) {
		t.Errorf("after Stop: %v", err)
	}
	noneReceived(t, to)
}
