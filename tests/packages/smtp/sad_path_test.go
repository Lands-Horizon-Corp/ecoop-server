package smtp_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/smtp"
)

func TestSadPath_StartRejectsBadConfigAllAtOnce(t *testing.T) {
	err := smtp.NewSMTPService("", 0, "", "", "not-an-email", "", true, nil).Start(context.Background())
	for _, want := range []error{smtp.ErrInvalidSender, smtp.ErrInvalidHost, smtp.ErrInvalidPort} {
		if !errors.Is(err, want) {
			t.Errorf("Start error %v does not include %v", err, want)
		}
	}
	for _, port := range []int{-1, 65536, 100000} {
		if err := smtp.NewSMTPService("h", port, "", "", "a@b.co", "", true, nil).Start(context.Background()); !errors.Is(err, smtp.ErrInvalidPort) {
			t.Errorf("port %d: err = %v, want ErrInvalidPort", port, err)
		}
	}
}

func TestSadPath_InvalidRecipients(t *testing.T) {
	svc := mockService(t)
	for _, to := range []string{"", "plain", "@ecoop.test", "a@", "a b@ecoop.test", "a@b.co, c@d.co", "<>", "a@b.co\r\nBcc: x@evil.test"} {
		if err := svc.Send(ctxT(t), smtp.SMTPRequest{To: to, Body: "x"}); !errors.Is(err, smtp.ErrInvalidRecipient) {
			t.Errorf("To %q: err = %v, want ErrInvalidRecipient", to, err)
		}
	}
}

func TestSadPath_TemplateErrors(t *testing.T) {
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
	requireMailpit(t)
	svc := smtp.NewSMTPService("127.0.0.1", 1, "", "", sender, "", true, nil)
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx := ctxT(t)
	if err := svc.Ping(ctx); !errors.Is(err, smtp.ErrConnect) {
		t.Errorf("Ping = %v, want ErrConnect", err)
	}
	if err := svc.Send(ctx, smtp.SMTPRequest{To: "a@b.co", Body: "x"}); !errors.Is(err, smtp.ErrSendFailed) {
		t.Errorf("Send = %v, want ErrSendFailed", err)
	}
}

func TestSadPath_DeadContextDeliversNothing(t *testing.T) {
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
	requireMailpit(t)
	svc := smtp.NewSMTPService(smtpHost, smtpPort, "", "", sender, "", true, nil)
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
