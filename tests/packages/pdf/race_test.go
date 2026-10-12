//go:build integration

package pdf_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pdf"
)

// Sync & race: one service from many goroutines; Stop racing renders.

func TestRace_ConcurrentRenders(t *testing.T) {
	t.Parallel()
	s := started(t)
	var wg sync.WaitGroup
	for range 24 {
		wg.Go(func() { render(t, s, invoice()) })
	}
	wg.Wait()
}

// Stop waits for a render in flight.
func TestRace_StopWaitsForARender(t *testing.T) {
	t.Parallel()
	s, p := viaProxy(t)
	p.SetDelay(300 * time.Millisecond)
	errc := make(chan error, 1)
	go func() {
		body, err := s.Generate(context.Background(), invoice())
		if err == nil {
			_ = body.Close()
		}
		errc <- err
	}()
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	if err := s.Stop(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 300*time.Millisecond {
		t.Fatal("Stop returned before the render in flight could finish")
	}
	if err := <-errc; err != nil {
		t.Fatalf("render cut off by Stop: %v", err)
	}
}

func TestRace_StopGivesUpWhenCtxEnds(t *testing.T) {
	t.Parallel()
	s, p := viaProxy(t)
	p.SetDelay(3 * time.Second)
	go func() { _, _ = s.Generate(context.Background(), invoice()) }()
	time.Sleep(100 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := s.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop = %v, want DeadlineExceeded", err)
	}
}

func TestRace_LifecycleChurnWithTraffic(t *testing.T) {
	t.Parallel()
	s := started(t)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 6 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				mustNotPanic(t, "render during churn", func() {
					body, err := s.Generate(ctxT(t), invoice())
					if err == nil {
						_ = body.Close()
					} else if !errors.Is(err, pdf.ErrNotStarted) {
						t.Errorf("Generate: %v", err)
					}
				})
			}
		})
	}
	for i := range 10 {
		if i%2 == 0 {
			_ = s.Stop(ctxT(t))
		} else if err := s.Start(ctxT(t)); err != nil {
			t.Error(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
}
