//go:build integration

package smtp_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/smtp"
	"github.com/Lands-Horizon-Corp/ecoop-server/tests/internal/obstest"
)

// Mock mode (secured=false) validates and formats and sends nothing, so these
// tests need no mail server.

func observedMock(t testing.TB) (smtp.ISMTPService, *observability.Service) {
	t.Helper()
	o := obstest.New(t)
	svc := smtp.NewSMTPService("", 0, "", "", "noreply@ecoop.test", "eCoop", false, nil, nil, smtp.WithObserver(o))
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = svc.Stop(context.Background()) })
	return svc, o
}

func mailCount(o *observability.Service, op, outcome string) float64 {
	return obstest.Count(o, "smtp", op, outcome)
}

// A send is one counted operation; a rejected one (a bad template) is an error.
func TestObserver_SendIsAnOperation(t *testing.T) {
	t.Parallel()
	svc, o := observedMock(t)
	ctx := context.Background()
	for range 3 {
		if err := svc.Send(ctx, smtp.SMTPRequest{To: "a@b.co", Subject: "s", Body: "hello {{.Name}}"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := svc.Send(ctx, smtp.SMTPRequest{To: "a@b.co", Body: "{{ broken"}); err == nil {
		t.Fatal("a broken template was accepted")
	}
	if got := mailCount(o, "send", "success"); got != 3 {
		t.Fatalf("send success = %v, want 3", got)
	}
	if got := mailCount(o, "send", "error"); got != 1 {
		t.Fatalf("send error = %v, want 1", got)
	}
}

// A recipient refused up front is not counted at all, and no address or body is recorded.
func TestObserver_RefusedRecipientIsNotCountedAndNoAddressIsRecorded(t *testing.T) {
	t.Parallel()
	svc, o := observedMock(t)
	if err := svc.Send(context.Background(), smtp.SMTPRequest{To: "not-an-email", Body: "x"}); !errors.Is(err, smtp.ErrInvalidRecipient) {
		t.Fatalf("Send = %v, want ErrInvalidRecipient", err)
	}
	if err := svc.Send(context.Background(), smtp.SMTPRequest{To: "private.person@example.org", Subject: "Your balance", Body: "secret body"}); err != nil {
		t.Fatal(err)
	}
	for _, sm := range o.Snapshot() {
		blob := sm.Name
		for k, v := range sm.Attrs {
			blob += " " + k + "=" + v
		}
		for _, leak := range []string{"private.person", "example.org", "Your balance", "secret body"} {
			if strings.Contains(blob, leak) {
				t.Fatalf("%q leaked into the registry: %s", leak, blob)
			}
		}
	}
}

// No observer, and a bare struct, are fine.
func TestObserver_NoneIsFine(t *testing.T) {
	t.Parallel()
	svc := mockService(t)
	if err := svc.Send(context.Background(), smtp.SMTPRequest{To: "a@b.co", Body: "x"}); err != nil {
		t.Fatal(err)
	}
	var zero smtp.SMTPService
	if err := zero.Send(context.Background(), smtp.SMTPRequest{To: "a@b.co"}); err == nil {
		t.Fatal("a zero service must return an error, not panic")
	}
}
