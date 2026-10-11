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
)

// Sync & race: one started service shared across goroutines.
// Run with `go test -race` to catch data races.

const workers = 32

func TestRace_SharedEncodeDecode(t *testing.T) {
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
	ctx := context.Background()
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Go(func() {
			svc := qr.NewQRServices(fmt.Sprintf("tenant-%d", w))
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

	svc := qr.NewQRServices(testSecret)
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
	ctx := context.Background()
	svc := qr.NewQRServices(testSecret)
	if err := svc.Start(ctx); err != nil {
		t.Fatal(err)
	}
	enc, _ := svc.Encode(ctx, &qr.QRData{Data: "in-flight"})

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
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
		}()
	}
	time.Sleep(time.Millisecond)
	if err := svc.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}
