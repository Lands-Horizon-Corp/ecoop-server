//go:build integration

package smtp_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/smtp"
)

func TestHappyPath_FormatInlineBody(t *testing.T) {
	t.Parallel()
	svc := mockService(t)
	out, err := svc.Format(ctxT(t), smtp.SMTPRequest{
		Name: "Ana",
		Body: "<p>Hi {{.Name}}, your code is {{.Code}}.</p>",
		Vars: map[string]string{"Code": "123456"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := "<p>Hi Ana, your code is 123456.</p>"; out.Body != want {
		t.Fatalf("Body = %q, want %q", out.Body, want)
	}
}

func TestHappyPath_FormatTemplateFile(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "welcome.html")
	if err := os.WriteFile(path, []byte("<h1>Welcome {{.Name}}</h1><p>{{.Branch}}</p>"), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := mockService(t).Format(ctxT(t), smtp.SMTPRequest{
		Name: "Ben", Body: path, Vars: map[string]string{"Branch": "Cebu"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := "<h1>Welcome Ben</h1><p>Cebu</p>"; out.Body != want {
		t.Fatalf("Body = %q, want %q", out.Body, want)
	}
}

func TestHappyPath_SendDelivers(t *testing.T) {
	t.Parallel()
	ctx := ctxT(t)
	svc := newService(t)
	to := inbox(t)
	err := svc.Send(ctx, smtp.SMTPRequest{
		Name:    "Ana",
		To:      to,
		Subject: "Loan approved",
		Body:    `<p style="color: red">Hi {{.Name}}</p><table><tr><td>Amount</td><td>{{.Amount}}</td></tr></table>`,
		Vars:    map[string]string{"Amount": "1,000.00"},
	})
	if err != nil {
		t.Fatal(err)
	}
	m := receivedOne(t, to)
	if m.Subject != "Loan approved" {
		t.Errorf("Subject = %q", m.Subject)
	}
	if m.From.Address != sender || m.From.Name != senderName {
		t.Errorf("From = %+v, want %q <%s>", m.From, senderName, sender)
	}
	if len(m.To) != 1 || m.To[0].Address != to {
		t.Errorf("To = %+v", m.To)
	}
	for _, want := range []string{"Hi Ana", "1,000.00", "<table>", "color: red"} {
		if !strings.Contains(m.HTML, want) {
			t.Errorf("HTML missing %q:\n%s", want, m.HTML)
		}
	}
	if m.MessageID == "" || m.Date == "" {
		t.Errorf("missing Message-ID (%q) or Date (%q)", m.MessageID, m.Date)
	}
}

func TestHappyPath_FromNameOverridesDefault(t *testing.T) {
	t.Parallel()
	ctx := ctxT(t)
	svc := newService(t)
	to := inbox(t)
	if err := svc.Send(ctx, smtp.SMTPRequest{To: to, FromName: "Loans Desk", Body: "x"}); err != nil {
		t.Fatal(err)
	}
	if m := receivedOne(t, to); m.From.Name != "Loans Desk" {
		t.Fatalf("From name = %q, want Loans Desk", m.From.Name)
	}
}

func TestHappyPath_UnicodeAndLongLines(t *testing.T) {
	t.Parallel()
	ctx := ctxT(t)
	svc := newService(t)
	to := inbox(t)
	subject := "Pagbati 🎉 — ñandú 日本語"
	long := strings.Repeat("a", 5000)
	if err := svc.Send(ctx, smtp.SMTPRequest{To: to, Subject: subject, Body: "<p>Ñoño 日本 🧾</p><p>" + long + "</p>"}); err != nil {
		t.Fatal(err)
	}
	m := receivedOne(t, to)
	if m.Subject != subject {
		t.Errorf("Subject = %q, want %q", m.Subject, subject)
	}
	if !strings.Contains(m.HTML, "Ñoño 日本 🧾") || !strings.Contains(m.HTML, long) {
		t.Error("unicode or a 5000-char line did not survive transport")
	}
}

func TestHappyPath_SeveralSends(t *testing.T) {
	t.Parallel()
	ctx := ctxT(t)
	svc := newService(t)
	to := inbox(t)
	for i := range 5 {
		if err := svc.Send(ctx, smtp.SMTPRequest{To: to, Subject: "n" + string(rune('0'+i)), Body: "x"}); err != nil {
			t.Fatal(err)
		}
	}
	received(t, to, 5)
}

func TestHappyPath_RestartKeepsSending(t *testing.T) {
	t.Parallel()
	ctx := ctxT(t)
	svc := newService(t)
	to := inbox(t)
	for range 3 {
		if err := svc.Stop(ctx); err != nil {
			t.Fatal(err)
		}
		if err := svc.Start(ctx); err != nil {
			t.Fatal(err)
		}
		if err := svc.Send(ctx, smtp.SMTPRequest{To: to, Body: "x"}); err != nil {
			t.Fatal(err)
		}
	}
	received(t, to, 3)
}

func TestHappyPath_PingAgainstServer(t *testing.T) {
	t.Parallel()
	ctx := ctxT(t)
	start := time.Now()
	if err := newService(t).Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 10*time.Second { // bounded, not hung; generous for a busy machine
		t.Fatalf("Ping took %v", d)
	}
}
