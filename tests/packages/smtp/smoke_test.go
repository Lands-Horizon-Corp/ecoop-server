//go:build integration

package smtp_test

import (
	"context"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/smtp"
)

// Smoke: start, ping, queue one email, see it arrive, stop.
func TestSmoke(t *testing.T) {
	t.Parallel()
	ctx := ctxT(t)
	svc := newSMTP(t, serviceOpts{})
	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := svc.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	to := inbox(t)
	if err := svc.Send(ctx, smtp.SMTPRequest{To: to, Subject: "smoke", Body: "<p>hello</p>"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if m := receivedOne(t, to); m.Subject != "smoke" {
		t.Fatalf("Subject = %q", m.Subject)
	}
	if err := svc.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
