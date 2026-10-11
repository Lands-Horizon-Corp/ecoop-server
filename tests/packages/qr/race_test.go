package qr_test

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/qr"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Sync & race: one started service shared across goroutines.
// Run with `go test -race` to catch data races.

const workers = 32

func TestRace_SharedEncodeDecode(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := newStarted(t)
	start := make(chan struct{})
	var wg sync.WaitGroup

	for w := range workers {
		wg.Go(func() {
			<-start
			for i := range 50 {
				in := qr.QRData{Data: fmt.Sprintf("w%d-i%d", w, i), Type: "race"}
				enc, err := svc.Encode(ctx, &in)
				if err != nil {
					t.Errorf("Encode: %v", err)
					return
				}
				if out, err := svc.Decode(ctx, enc); err != nil || out != in {
					t.Errorf("w%d i%d: got %+v, %v", w, i, out, err)
					return
				}
			}
		})
	}
	close(start)
	wg.Wait()
}

func TestRace_NoncesUniqueUnderConcurrency(t *testing.T) {
	t.Parallel()
	svc := newStarted(t)
	var mu sync.Mutex
	seen := make(map[string]struct{})
	var wg sync.WaitGroup

	for range workers {
		wg.Go(func() {
			for range 100 {
				raw, _ := base64.StdEncoding.DecodeString(mustEncode(t, svc, &qr.QRData{Data: "same"}))
				nonce := string(raw[:24])
				mu.Lock()
				if _, dup := seen[nonce]; dup {
					t.Error("XChaCha20 nonce reused under concurrency")
				}
				seen[nonce] = struct{}{}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
}

func TestRace_SharedPayloadScannedConcurrently(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := newStarted(t)
	in := qr.QRData{Data: "shared", Type: "scan"}
	enc := mustEncode(t, svc, &in)

	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			if out, err := svc.Decode(ctx, enc); err != nil || out != in {
				t.Errorf("got %+v, %v", out, err)
			}
		})
	}
	wg.Wait()
}

func TestRace_IndependentInstancesInParallel(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Go(func() {
			svc := qr.NewQRServices(fmt.Sprintf("tenant-%d", w), nil)
			if err := svc.Start(ctx); err != nil {
				t.Error(err)
				return
			}
			defer svc.Stop(ctx)
			in := qr.QRData{Data: fmt.Sprint(w)}
			enc, _ := svc.Encode(ctx, &in)
			if out, err := svc.Decode(ctx, enc); err != nil || out != in {
				t.Errorf("tenant %d: %+v, %v", w, out, err)
			}
		})
	}
	wg.Wait()
}

func TestRace_RepeatedStartDoesNotLeakGoroutines(t *testing.T) {
	ctx := context.Background()
	runtime.GC()
	before := runtime.NumGoroutine()

	svc := qr.NewQRServices(testSecret, nil)
	for range 20 {
		if err := svc.Start(ctx); err != nil {
			t.Fatal(err)
		}
		// Exercise both codecs so any lazily started workers exist.
		enc, _ := svc.Encode(ctx, &qr.QRData{Data: "x"})
		_, _ = svc.Decode(ctx, enc)
	}
	_ = svc.Stop(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before+2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before+2 {
		t.Fatalf("goroutines %d -> %d after 20 Starts and one Stop; earlier codecs were never closed", before, after)
	}
}

// Stop arriving mid-traffic (e.g. on SIGTERM) must let in-flight calls
// finish; calls that arrive after it must get ErrNotStarted, never a panic.
func TestRace_StopDuringTraffic(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := qr.NewQRServices(testSecret, nil)
	if err := svc.Start(ctx); err != nil {
		t.Fatal(err)
	}
	enc, _ := svc.Encode(ctx, &qr.QRData{Data: "in-flight"})

	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for range 200 {
				mustNotPanic(t, "Encode/Decode during Stop", func() {
					if _, err := svc.Encode(ctx, &qr.QRData{Data: "x"}); err != nil && !errors.Is(err, qr.ErrNotStarted) {
						t.Errorf("Encode: %v", err)
					}
					if out, err := svc.Decode(ctx, enc); err == nil && out.Data != "in-flight" {
						t.Errorf("Decode returned corrupted data: %+v", out)
					} else if err != nil && !errors.Is(err, qr.ErrNotStarted) {
						t.Errorf("Decode: %v", err)
					}
				})
			}
		})
	}
	time.Sleep(time.Millisecond)
	if err := svc.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}

// Stop must wait for calls in flight before releasing the codecs. Encode and
// Decode are too quick to catch mid-flight, so a shared tracker holds an
// operation open the same way a slow call would.
func TestRace_StopWaitsForInFlight(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	shared := utils.NewInflight()
	svc := qr.NewQRServices(testSecret, shared)
	if err := svc.Start(ctx); err != nil {
		t.Fatal(err)
	}
	enc, _ := svc.Encode(ctx, &qr.QRData{Data: "x"})

	release := shared.Track()
	stopped := make(chan error, 1)
	go func() { stopped <- svc.Stop(ctx) }()

	select {
	case <-stopped:
		t.Fatal("Stop returned while a call was still in flight")
	case <-time.After(50 * time.Millisecond):
	}
	if _, err := svc.Decode(ctx, enc); !errors.Is(err, qr.ErrNotStarted) {
		t.Fatalf("new call during Stop = %v, want ErrNotStarted", err)
	}
	release()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("Stop: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop did not return after the in-flight call finished")
	}
}

func TestRace_StopGivesUpWhenCtxEnds(t *testing.T) {
	t.Parallel()
	shared := utils.NewInflight()
	svc := qr.NewQRServices(testSecret, shared)
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	release := shared.Track()
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := svc.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop = %v, want DeadlineExceeded", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Stop overran its deadline: %v", d)
	}
}
