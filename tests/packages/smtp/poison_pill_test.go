package smtp_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/smtp"
)

// Poison pill: JS/XSS and header injection through every field a caller can
// fill, checked on the email Mailpit actually received.

func TestPoisonPill_XSSInBody(t *testing.T) {
	t.Parallel()
	ctx := ctxT(t)
	svc := newService(t)
	for name, payload := range xssPayloads {
		t.Run(name, func(t *testing.T) {
			to := inbox(t)
			if err := svc.Send(ctx, smtp.SMTPRequest{To: to, Body: "<p>before</p>" + payload + "<p>after</p>"}); err != nil {
				t.Fatalf("Send: %v", err)
			}
			m := receivedOne(t, to)
			assertNoXSS(t, m.HTML)
			// Content after an unclosed payload (e.g. a dangling <style>) may be
			// swallowed, which is safe; content before it must survive.
			if !strings.Contains(m.HTML, "before") {
				t.Errorf("sanitizer dropped the safe content before the payload:\n%s", m.HTML)
			}
		})
	}
}

func TestPoisonPill_XSSThroughTemplateVars(t *testing.T) {
	t.Parallel()
	// Vars are filled in after the template is parsed, so this is the path
	// where user-supplied values (a member's name, a remark) reach the body.
	ctx := ctxT(t)
	svc := newService(t)
	for name, payload := range xssPayloads {
		t.Run(name, func(t *testing.T) {
			to := inbox(t)
			err := svc.Send(ctx, smtp.SMTPRequest{
				To:   to,
				Name: payload,
				Body: "<p>Hi {{.Name}}</p><p>{{.Remark}}</p>",
				Vars: map[string]string{"Remark": payload},
			})
			if err != nil {
				t.Fatalf("Send: %v", err)
			}
			assertNoXSS(t, receivedOne(t, to).HTML)
		})
	}
}

func TestPoisonPill_XSSInTemplateFile(t *testing.T) {
	t.Parallel()
	ctx := ctxT(t)
	svc := newService(t)
	path := filepath.Join(t.TempDir(), "evil.html")
	if err := os.WriteFile(path, []byte(`<p>Hi {{.Name}}</p><script>steal(document.cookie)</script><img src=x onerror=alert(1)>`), 0o600); err != nil {
		t.Fatal(err)
	}
	to := inbox(t)
	if err := svc.Send(ctx, smtp.SMTPRequest{To: to, Name: "Ana", Body: path}); err != nil {
		t.Fatal(err)
	}
	m := receivedOne(t, to)
	assertNoXSS(t, m.HTML)
	if !strings.Contains(m.HTML, "Hi Ana") {
		t.Errorf("template content lost:\n%s", m.HTML)
	}
}

func TestPoisonPill_XSSThroughCSS(t *testing.T) {
	t.Parallel()
	ctx := ctxT(t)
	svc := newService(t)
	for name, payload := range cssXSSPayloads {
		t.Run(name, func(t *testing.T) {
			to := inbox(t)
			if err := svc.Send(ctx, smtp.SMTPRequest{To: to, Body: payload}); err != nil {
				t.Fatalf("Send: %v", err)
			}
			assertNoXSS(t, receivedOne(t, to).HTML)
		})
	}
}

func TestPoisonPill_XSSInSubjectAndFromNameStaysText(t *testing.T) {
	t.Parallel()
	// Headers are never HTML. The payload must arrive verbatim as text, and
	// must not break out into extra headers.
	ctx := ctxT(t)
	svc := newService(t)
	to := inbox(t)
	payload := `<script>alert(1)</script><img src=x onerror=alert(1)>`
	if err := svc.Send(ctx, smtp.SMTPRequest{To: to, Subject: payload, FromName: payload, Body: "x"}); err != nil {
		t.Fatal(err)
	}
	m := receivedOne(t, to)
	if m.Subject != payload {
		t.Errorf("Subject altered: %q", m.Subject)
	}
	if m.From.Name != payload {
		t.Errorf("From name altered: %q", m.From.Name)
	}
}

func TestPoisonPill_HeaderInjection(t *testing.T) {
	t.Parallel()
	ctx := ctxT(t)
	svc := newService(t)
	attacker := inbox(t)
	inject := "\r\nBcc: " + attacker + "\r\nX-Injected: yes"
	to := inbox(t)
	err := svc.Send(ctx, smtp.SMTPRequest{
		To:       to,
		Subject:  "Statement" + inject,
		FromName: "eCoop" + inject,
		Body:     "x",
	})
	if err != nil {
		t.Fatal(err)
	}
	m := receivedOne(t, to)
	if len(m.Bcc) != 0 || len(m.Cc) != 0 || len(m.To) != 1 {
		t.Errorf("extra recipients: To=%v Cc=%v Bcc=%v", m.To, m.Cc, m.Bcc)
	}
	if h := rawHeaders(t, m.ID); len(h["X-Injected"]) != 0 || len(h["Bcc"]) != 0 {
		t.Errorf("injected headers present: %v", h)
	}
	noneReceived(t, attacker)
}

func TestPoisonPill_RecipientInjectionRejected(t *testing.T) {
	t.Parallel()
	svc := newService(t)
	attacker := inbox(t)
	for _, to := range []string{
		"a@ecoop.test\r\nBcc: " + attacker,
		"a@ecoop.test\nRCPT TO:<" + attacker + ">",
		"a@ecoop.test, " + attacker,
	} {
		if err := svc.Send(ctxT(t), smtp.SMTPRequest{To: to, Body: "x"}); !errors.Is(err, smtp.ErrInvalidRecipient) {
			t.Errorf("To %q: err = %v, want ErrInvalidRecipient", to, err)
		}
	}
	noneReceived(t, attacker)
}

func TestPoisonPill_TemplateValuesAreNotReEvaluated(t *testing.T) {
	t.Parallel()
	out, err := mockService(t).Format(ctxT(t), smtp.SMTPRequest{
		Name: "{{.Secret}}",
		Body: "<p>{{.Name}}</p>",
		Vars: map[string]string{"Secret": "TOP-SECRET"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.Body, "TOP-SECRET") {
		t.Fatalf("a value containing template syntax was evaluated: %q", out.Body)
	}
}

// Format treats a Body that names an existing file as a template path. That
// is a feature for server-side templates, but it means a Body taken from user
// input could read any file the process can see. This pins the behaviour so
// it stays a conscious choice: never pass user input as Body.
func TestPoisonPill_BodyThatIsAFilePathIsReadAsTemplate(t *testing.T) {
	t.Parallel()
	secret := filepath.Join(t.TempDir(), "secret.txt")
	if err := os.WriteFile(secret, []byte("DB_PASSWORD=hunter2"), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := mockService(t).Format(ctxT(t), smtp.SMTPRequest{Body: secret})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Body, "hunter2") {
		t.Fatalf("behaviour changed: Body path was not read (got %q); update this test and the docs", out.Body)
	}
	t.Log("Body is read from disk when it is an existing path; callers must not pass user input as Body")
}

func TestPoisonPill_HostileBodies(t *testing.T) {
	t.Parallel()
	ctx := ctxT(t)
	svc := newService(t)
	cases := map[string]string{
		"5MiB":            "<p>" + strings.Repeat("x", 5<<20) + "</p>",
		"null bytes":      "<p>a\x00b\x00c</p>",
		"invalid utf8":    "<p>" + string([]byte{0xff, 0xfe, 0xc3, 0x28}) + "</p>",
		"rtl override":    "<p>‮gnp.exe</p>",
		"crlf lines":      "<p>a</p>\r\n.\r\n<p>b</p>", // a lone '.' line ends DATA in raw SMTP
		"deep nesting":    strings.Repeat("<div>", 2000) + "x" + strings.Repeat("</div>", 2000),
		"smtp in body":    "<p>x</p>\r\n.\r\nMAIL FROM:<evil@evil.test>\r\n",
		"unclosed tags":   "<p><table><tr><td>x",
		"comment smuggle": "<!--<script>alert(1)</script>--><p>x</p>",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			to := inbox(t)
			var err error
			mustNotPanic(t, "Send", func() { err = svc.Send(ctx, smtp.SMTPRequest{To: to, Body: body}) })
			if err != nil {
				t.Fatalf("Send: %v", err)
			}
			msgs := received(t, to, 1)
			if len(msgs) != 1 {
				t.Fatalf("%d messages arrived; body split the SMTP session", len(msgs))
			}
			assertNoXSS(t, msgs[0].HTML)
		})
	}
}
