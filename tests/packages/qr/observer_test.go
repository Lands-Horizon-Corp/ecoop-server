package qr_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/qr"
	"github.com/Lands-Horizon-Corp/ecoop-server/tests/internal/obstest"
)

// No build tag: the QR service needs no infrastructure.

func observedQR(t testing.TB) (qr.IQRServices, *observability.Service) {
	t.Helper()
	o := obstest.New(t)
	svc := qr.NewQRServices(testSecret, nil, qr.WithObserver(o))
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = svc.Stop(context.Background()) })
	return svc, o
}

func qrCount(o *observability.Service, op, outcome string) float64 {
	return obstest.Count(o, "qr", op, outcome)
}

// Encode and Decode are each one counted, timed operation.
func TestObserver_EncodeAndDecodeAreOperations(t *testing.T) {
	t.Parallel()
	svc, o := observedQR(t)
	ctx := context.Background()
	for range 4 {
		enc, err := svc.Encode(ctx, &qr.QRData{Data: "member:42", Type: "member"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := svc.Decode(ctx, enc); err != nil {
			t.Fatal(err)
		}
	}
	if got := qrCount(o, "encode", "success"); got != 4 {
		t.Fatalf("encode success = %v, want 4", got)
	}
	if got := qrCount(o, "decode", "success"); got != 4 {
		t.Fatalf("decode success = %v, want 4", got)
	}
	if d, ok := o.Find(observability.MetricOperationDuration, map[string]string{observability.KeyOperation: "decode"}); !ok || d.Count != 4 {
		t.Fatalf("decode duration = %+v, want 4 observations", d)
	}
}

// A code that cannot be decoded is an error of the decode, however it is damaged.
func TestObserver_ADamagedCodeIsAnError(t *testing.T) {
	t.Parallel()
	svc, o := observedQR(t)
	for _, bad := range []string{"", "not base64 !!", "AAAA"} {
		if _, err := svc.Decode(context.Background(), bad); err == nil {
			t.Fatalf("Decode(%q) accepted", bad)
		}
	}
	if got := qrCount(o, "decode", "error"); got != 3 {
		t.Fatalf("decode error = %v, want 3", got)
	}
	if got := qrCount(o, "decode", "success"); got != 0 {
		t.Fatalf("a bad code counted as success %v times", got)
	}
}

// A call refused before any work (a nil argument, a service that is not started)
// is not counted, and what is counted never carries the data.
func TestObserver_RefusedCallsAreNotCounted(t *testing.T) {
	t.Parallel()
	o := obstest.New(t)
	idle := qr.NewQRServices(testSecret, nil, qr.WithObserver(o)) // never started
	if _, err := idle.Encode(context.Background(), &qr.QRData{Data: "x"}); !errors.Is(err, qr.ErrNotStarted) {
		t.Fatalf("Encode = %v, want ErrNotStarted", err)
	}
	if _, err := idle.Encode(context.Background(), nil); !errors.Is(err, qr.ErrNilData) {
		t.Fatalf("Encode(nil) = %v, want ErrNilData", err)
	}
	if got := qrCount(o, "encode", "error") + qrCount(o, "encode", "success"); got != 0 {
		t.Fatalf("refused calls were counted %v times", got)
	}
}

// No observer, and a bare struct, are fine.
func TestObserver_NoneIsFine(t *testing.T) {
	t.Parallel()
	svc := newStarted(t)
	enc, err := svc.Encode(context.Background(), &qr.QRData{Data: "a", Type: "b"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Decode(context.Background(), enc); err != nil {
		t.Fatal(err)
	}
	var zero qr.QRService
	if _, err := zero.Encode(context.Background(), &qr.QRData{}); err == nil {
		t.Fatal("a zero service must return an error, not panic")
	}
}
