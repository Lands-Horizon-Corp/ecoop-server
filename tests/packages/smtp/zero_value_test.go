package smtp_test

import (
	"context"
	"errors"
	"maps"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/smtp"
)

// Zero value & absence: empty fields, nil maps, zero structs, nil contexts,
// nil receivers and caller data that must not be aliased.

func TestZeroValue_ZeroStruct(t *testing.T) {
	ctx := context.Background()
	var svc smtp.SMTPServices = &smtp.SMTPService{}
	mustNotPanic(t, "zero struct", func() {
		if err := svc.Start(ctx); !errors.Is(err, smtp.ErrNoTracker) {
			t.Errorf("Start = %v, want ErrNoTracker", err)
		}
		if err := svc.Send(ctx, smtp.SMTPRequest{To: "a@b.co", Body: "x"}); !errors.Is(err, smtp.ErrNotStarted) {
			t.Errorf("Send = %v, want ErrNotStarted", err)
		}
		if err := svc.Stop(ctx); err != nil {
			t.Errorf("Stop = %v", err)
		}
		if err := svc.Ping(ctx); err != nil {
			t.Errorf("Ping on zero (mock) struct = %v", err)
		}
	})
}

func TestZeroValue_StopIsIdempotent(t *testing.T) {
	svc := smtp.NewSMTPService("h", 25, "", "", "a@b.co", "", true, nil, nil)
	for range 3 {
		if err := svc.Stop(context.Background()); err != nil {
			t.Fatalf("Stop before Start = %v", err)
		}
	}
}

func TestZeroValue_EmptyRequiredFields(t *testing.T) {
	ctx := context.Background()
	if err := smtp.NewSMTPService("h", 25, "", "", "", "", true, nil, nil).Start(ctx); !errors.Is(err, smtp.ErrInvalidSender) {
		t.Errorf("empty from: Start = %v, want ErrInvalidSender", err)
	}
	if err := mockService(t).Send(ctx, smtp.SMTPRequest{Body: "x"}); !errors.Is(err, smtp.ErrInvalidRecipient) {
		t.Errorf("empty To: Send = %v, want ErrInvalidRecipient", err)
	}
}

func TestZeroValue_EmptyOptionalFields(t *testing.T) {
	ctx := ctxT(t)
	svc := startService(t, serviceOpts{noName: true}) // no default display name
	to := inbox(t)
	if err := svc.Send(ctx, smtp.SMTPRequest{To: to}); err != nil { // no subject, body, name, vars, from name
		t.Fatalf("Send with only To = %v", err)
	}
	m := receivedOne(t, to)
	if m.Subject != "" || m.From.Name != "" || m.From.Address != sender {
		t.Errorf("Subject %q, From %+v", m.Subject, m.From)
	}
}

func TestZeroValue_NilVarsAndMissingKeys(t *testing.T) {
	out, err := mockService(t).Format(context.Background(), smtp.SMTPRequest{Body: "[{{.Name}}][{{.Missing}}]"})
	if err != nil {
		t.Fatal(err)
	}
	if out.Body != "[][]" {
		t.Fatalf("Body = %q, want [][] for absent name and key", out.Body)
	}
}

func TestZeroValue_FormatDoesNotMutateCallerVars(t *testing.T) {
	// Format must not write into the caller's map: callers reuse Vars across
	// recipients, and a shared map written concurrently crashes the process.
	vars := map[string]string{"Code": "123"}
	before := maps.Clone(vars)
	_, err := mockService(t).Format(context.Background(), smtp.SMTPRequest{Name: "Ana", Body: "{{.Name}}", Vars: vars})
	if err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(vars, before) {
		t.Fatalf("caller's Vars changed from %v to %v", before, vars)
	}
}

func TestZeroValue_FormatDoesNotAliasRequest(t *testing.T) {
	req := smtp.SMTPRequest{Body: "orig"}
	out, err := mockService(t).Format(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	out.Body = "changed"
	if req.Body != "orig" {
		t.Fatal("mutating Format's result changed the caller's request")
	}
}

func TestZeroValue_MockModeDeliversNothing(t *testing.T) {
	requireMailpit(t)
	svc := mockService(t)
	to := inbox(t)
	if err := svc.Send(ctxT(t), smtp.SMTPRequest{To: to, Body: "x"}); err != nil {
		t.Fatal(err)
	}
	noneReceived(t, to)
}

func TestZeroValue_NilContext(t *testing.T) {
	var nilCtx context.Context
	t.Run("mock mode", func(t *testing.T) {
		mustNotPanic(t, "Send(nil ctx)", func() {
			if err := mockService(t).Send(nilCtx, smtp.SMTPRequest{To: "a@b.co", Body: "x"}); err != nil {
				t.Fatal(err)
			}
		})
	})
	t.Run("queued send is a programming error", func(t *testing.T) {
		// Queueing needs a real ctx; nil must fail fast (panic or error),
		// never hang or queue silently.
		svc := newService(t)
		type result struct {
			panicked any
			err      error
		}
		done := make(chan result, 1)
		go func() {
			var err error
			r, _ := capturePanic(func() { err = svc.Send(nilCtx, smtp.SMTPRequest{To: "a@b.co", Body: "x"}) })
			done <- result{r, err}
		}()
		select {
		case r := <-done:
			if r.panicked == nil && r.err == nil {
				t.Fatal("Send(nil ctx) neither panicked nor returned an error")
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Send(nil ctx) hung")
		}
	})
}

// Dangling pointers: a nil *SMTPService or nil interface is a wiring bug and
// must fail loudly. Format is the exception: it never touches the receiver.
func TestZeroValue_NilReceiver(t *testing.T) {
	ctx := context.Background()
	var ptr *smtp.SMTPService
	var iface smtp.SMTPServices
	ops := map[string]func(smtp.SMTPServices){
		"Start": func(s smtp.SMTPServices) { _ = s.Start(ctx) },
		"Stop":  func(s smtp.SMTPServices) { _ = s.Stop(ctx) },
		"Ping":  func(s smtp.SMTPServices) { _ = s.Ping(ctx) },
		"Send":  func(s smtp.SMTPServices) { _ = s.Send(ctx, smtp.SMTPRequest{To: "a@b.co"}) },
	}
	for name, op := range ops {
		t.Run(name+"/nil pointer", func(t *testing.T) { mustPanic(t, name, func() { op(ptr) }) })
		t.Run(name+"/nil interface", func(t *testing.T) { mustPanic(t, name, func() { op(iface) }) })
	}
	t.Run("Format/nil pointer", func(t *testing.T) {
		mustNotPanic(t, "Format", func() { _, _ = ptr.Format(ctx, smtp.SMTPRequest{Body: "x"}) })
	})
}
