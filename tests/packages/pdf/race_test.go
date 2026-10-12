//go:build integration

package pdf_test

import (
	"context"
	"testing"
	"time"
)

// Sync & race: Stop waits for a render in flight. Run with -race.
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
