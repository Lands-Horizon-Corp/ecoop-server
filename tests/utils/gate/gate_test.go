package gate_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

func TestStartsClosed(t *testing.T) {
	g := utils.NewGate(nil)
	if g.IsOpen() {
		t.Fatal("new gate is open")
	}
	if _, ok := g.Enter(); ok {
		t.Fatal("entered a closed gate")
	}
}

func TestOpenEnterDone(t *testing.T) {
	g := utils.NewGate(nil)
	g.Open()
	done, ok := g.Enter()
	if !ok || done == nil {
		t.Fatal("could not enter an open gate")
	}
	done()
	if err := g.Close(context.Background()); err != nil {
		t.Fatalf("Close = %v", err)
	}
	if g.IsOpen() {
		t.Fatal("gate still open after Close")
	}
}

func TestCloseWaitsForWork(t *testing.T) {
	g := utils.NewGate(nil)
	g.Open()
	done, _ := g.Enter()
	closed := make(chan error, 1)
	go func() { closed <- g.Close(context.Background()) }()

	select {
	case <-closed:
		t.Fatal("Close returned while work was still running")
	case <-time.After(50 * time.Millisecond):
	}
	if _, ok := g.Enter(); ok {
		t.Fatal("entered while closing")
	}
	done()
	if err := <-closed; err != nil {
		t.Fatalf("Close = %v", err)
	}
}

func TestCloseRespectsDeadline(t *testing.T) {
	g := utils.NewGate(nil)
	g.Open()
	done, _ := g.Enter()
	defer done()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := g.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close = %v, want DeadlineExceeded", err)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	g := utils.NewGate(nil)
	for range 3 {
		if err := g.Close(context.Background()); err != nil {
			t.Fatalf("Close on a closed gate = %v", err)
		}
	}
}

func TestReopen(t *testing.T) {
	g := utils.NewGate(nil)
	for range 3 {
		g.Open()
		done, ok := g.Enter()
		if !ok {
			t.Fatal("could not enter after reopening")
		}
		done()
		if err := g.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDoneTwiceIsHarmless(t *testing.T) {
	inflight := utils.NewInflight()
	g := utils.NewGate(inflight)
	g.Open()
	done, _ := g.Enter()
	done()
	done()
	if n := inflight.Count(); n != 0 {
		t.Fatalf("Count = %d after calling done twice", n)
	}
}

func TestSharedInflight(t *testing.T) {
	// Two services sharing one tracker: closing one waits for the other's work.
	shared := utils.NewInflight()
	a, b := utils.NewGate(shared), utils.NewGate(shared)
	a.Open()
	b.Open()
	done, _ := b.Enter()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := a.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a.Close = %v; want it to wait for b's work", err)
	}
	if !b.IsOpen() {
		t.Fatal("closing a closed b")
	}
	done()
}

func TestNilGateActsClosed(t *testing.T) {
	var g *utils.Gate
	if g.IsOpen() {
		t.Fatal("nil gate is open")
	}
	if _, ok := g.Enter(); ok {
		t.Fatal("entered a nil gate")
	}
	if err := g.Close(context.Background()); err != nil {
		t.Fatalf("Close on nil gate = %v", err)
	}
}

func TestConcurrentEnterAndClose(t *testing.T) {
	// Run with -race. No work may be running once Close returns.
	g := utils.NewGate(nil)
	var running atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				if done, ok := g.Enter(); ok {
					running.Add(1)
					time.Sleep(100 * time.Microsecond)
					running.Add(-1)
					done()
				} else {
					time.Sleep(50 * time.Microsecond)
				}
			}
		})
	}
	for range 50 {
		g.Open()
		time.Sleep(time.Millisecond)
		if err := g.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		if n := running.Load(); n != 0 {
			t.Fatalf("Close returned with %d operations still running", n)
		}
	}
	close(stop)
	wg.Wait()
}
