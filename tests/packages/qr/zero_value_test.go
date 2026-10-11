package qr_test

import (
	"context"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/qr"
)

// Zero value & absence: unstarted/stopped services, nil data, empty
// secrets, nil contexts and nil receivers.

func TestZeroValue_UnstartedServiceReturnsErrors(t *testing.T) {
	ctx := context.Background()
	cases := map[string]qr.QRServices{
		"constructed only": qr.NewQRServices(testSecret),
		"zero struct":      &qr.QRService{},
	}
	for name, svc := range cases {
		t.Run(name+"/Encode", func(t *testing.T) {
			var err error
			mustNotPanic(t, "Encode", func() { _, err = svc.Encode(ctx, &qr.QRData{Data: "x"}) })
			if err == nil {
				t.Fatal("Encode before Start returned no error")
			}
		})
		t.Run(name+"/Decode", func(t *testing.T) {
			var err error
			mustNotPanic(t, "Decode", func() { _, err = svc.Decode(ctx, "AAAA") })
			if err == nil {
				t.Fatal("Decode before Start returned no error")
			}
		})
		t.Run(name+"/Stop", func(t *testing.T) {
			mustNotPanic(t, "Stop", func() { _ = svc.Stop(ctx) })
		})
	}
}

func TestZeroValue_StoppedServiceReturnsErrors(t *testing.T) {
	ctx := context.Background()
	svc := qr.NewQRServices(testSecret)
	if err := svc.Start(ctx); err != nil {
		t.Fatal(err)
	}
	valid, _ := svc.Encode(ctx, &qr.QRData{Data: "x"})
	if err := svc.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	t.Run("Decode", func(t *testing.T) {
		var err error
		mustNotPanic(t, "Decode", func() { _, err = svc.Decode(ctx, valid) })
		if err == nil {
			t.Fatal("Decode after Stop succeeded")
		}
	})
	t.Run("Encode", func(t *testing.T) {
		var err error
		mustNotPanic(t, "Encode", func() { _, err = svc.Encode(ctx, &qr.QRData{Data: "x"}) })
		if err == nil {
			t.Fatal("Encode after Stop succeeded; codes issued by a stopped service")
		}
	})
	t.Run("double Stop", func(t *testing.T) {
		mustNotPanic(t, "Stop", func() {
			if err := svc.Stop(ctx); err != nil {
				t.Fatalf("second Stop: %v", err)
			}
		})
	})
}

func TestZeroValue_NilData(t *testing.T) {
	svc := newStarted(t)
	var enc string
	var err error
	mustNotPanic(t, "Encode(nil)", func() { enc, err = svc.Encode(context.Background(), nil) })
	if err == nil {
		t.Fatalf("Encode(nil) succeeded and produced %q (decodes to an empty QRData)", enc)
	}
}

func TestZeroValue_EmptyData(t *testing.T) {
	ctx := context.Background()
	svc := newStarted(t)
	enc := mustEncode(t, svc, &qr.QRData{})
	out, err := svc.Decode(ctx, enc)
	if err != nil || out != (qr.QRData{}) {
		t.Fatalf("empty QRData round trip = %+v, %v", out, err)
	}
}

func TestZeroValue_DecodeFailuresReturnZeroValue(t *testing.T) {
	svc := newStarted(t)
	for _, in := range []string{"", "!!", "AAAA", forge(t, testSecret, []byte(`{"data":1}`), true)} {
		out, err := svc.Decode(context.Background(), in)
		if err == nil {
			t.Fatalf("Decode(%q) succeeded", in)
		}
		if out != (qr.QRData{}) {
			t.Fatalf("Decode(%q) returned partial data %+v alongside error", in, out)
		}
	}
}

func TestZeroValue_EmptySecretIsIsolated(t *testing.T) {
	ctx := context.Background()
	empty := newStartedWithSecret(t, "")
	in := qr.QRData{Data: "x"}
	enc := mustEncode(t, empty, &in)
	if out, err := empty.Decode(ctx, enc); err != nil || out != in {
		t.Fatalf("empty secret round trip: %+v, %v", out, err)
	}
	if _, err := newStarted(t).Decode(ctx, enc); err == nil {
		t.Fatal("empty-secret QR accepted by a service with a real secret")
	}
}

func TestZeroValue_NilContext(t *testing.T) {
	svc := newStarted(t)
	var nilCtx context.Context
	mustNotPanic(t, "nil ctx", func() {
		in := qr.QRData{Data: "x"}
		enc, err := svc.Encode(nilCtx, &in)
		if err != nil {
			t.Fatal(err)
		}
		if out, err := svc.Decode(nilCtx, enc); err != nil || out != in {
			t.Fatalf("Decode with nil ctx: %+v, %v", out, err)
		}
	})
}

// Dangling pointers: a nil *QRService or nil interface is a programming
// error and must fail loudly rather than silently produce output.
func TestZeroValue_NilReceiverFailsLoudly(t *testing.T) {
	ctx := context.Background()
	var ptr *qr.QRService
	var iface qr.QRServices
	ops := map[string]func(qr.QRServices){
		"Start":  func(s qr.QRServices) { _ = s.Start(ctx) },
		"Stop":   func(s qr.QRServices) { _ = s.Stop(ctx) },
		"Encode": func(s qr.QRServices) { _, _ = s.Encode(ctx, &qr.QRData{}) },
		"Decode": func(s qr.QRServices) { _, _ = s.Decode(ctx, "AAAA") },
	}
	for name, op := range ops {
		t.Run(name+"/nil pointer", func(t *testing.T) { mustPanic(t, name, func() { op(ptr) }) })
		t.Run(name+"/nil interface", func(t *testing.T) { mustPanic(t, name, func() { op(iface) }) })
	}
}
