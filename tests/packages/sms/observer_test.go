package sms_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sms"
	"github.com/Lands-Horizon-Corp/ecoop-server/tests/internal/obstest"
)

// Mock mode (secured=false) validates and formats and sends nothing, so these
// tests need no Twilio and no queue.

func observedMock(t testing.TB) (sms.ISMSServices, *observability.Service) {
	t.Helper()
	o := obstest.New(t)
	svc := sms.NewSMSService("", "", "+15005550006", 160, false, nil, nil, sms.WithObserver(o))
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = svc.Stop(context.Background()) })
	return svc, o
}

func smsCount(o *observability.Service, op, outcome string) float64 {
	return obstest.Count(o, "sms", op, outcome)
}

// A send is one counted operation; a message that is too long or empty is an error.
func TestObserver_SendIsAnOperation(t *testing.T) {
	t.Parallel()
	svc, o := observedMock(t)
	ctx := context.Background()
	for range 2 {
		if err := svc.Send(ctx, sms.SMSRequest{To: "+639171234567", Body: "hello"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := svc.Send(ctx, sms.SMSRequest{To: "+639171234567", Body: strings.Repeat("x", 500)}); !errors.Is(err, sms.ErrTooLong) {
		t.Fatalf("a 500 character body = %v, want ErrTooLong", err)
	}
	if got := smsCount(o, "send", "success"); got != 2 {
		t.Fatalf("send success = %v, want 2", got)
	}
	if got := smsCount(o, "send", "error"); got != 1 {
		t.Fatalf("send error = %v, want 1", got)
	}
}

// No phone number and no body is ever recorded.
func TestObserver_NoNumberOrBodyIsRecorded(t *testing.T) {
	t.Parallel()
	svc, o := observedMock(t)
	if err := svc.Send(context.Background(), sms.SMSRequest{To: "+639171234567", Body: "your code is 482913"}); err != nil {
		t.Fatal(err)
	}
	for _, sm := range o.Snapshot() {
		blob := sm.Name
		for k, v := range sm.Attrs {
			blob += " " + k + "=" + v
		}
		for _, leak := range []string{"639171234567", "482913", "your code"} {
			if strings.Contains(blob, leak) {
				t.Fatalf("%q leaked into the registry: %s", leak, blob)
			}
		}
	}
}

// No observer, and a bare struct, are fine.
func TestObserver_NoneIsFine(t *testing.T) {
	t.Parallel()
	svc := sms.NewSMSService("", "", "+15005550006", 160, false, nil, nil)
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := svc.Send(context.Background(), sms.SMSRequest{To: "+639171234567", Body: "x"}); err != nil {
		t.Fatal(err)
	}
	var zero sms.SMSService
	if err := zero.Send(context.Background(), sms.SMSRequest{To: "+639171234567"}); err == nil {
		t.Fatal("a zero service must return an error, not panic")
	}
}
