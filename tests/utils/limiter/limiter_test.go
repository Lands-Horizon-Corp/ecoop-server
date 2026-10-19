package limiter_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// clock is a time source a test moves by hand.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: time.Unix(1_700_000_000, 0)} }
func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func TestBurstThenRefusal(t *testing.T) {
	t.Parallel()
	c := newClock()
	l := utils.NewLimiter(1, 3, time.Minute, 10, utils.WithLimiterClock(c.now))
	for i := range 3 {
		if ok, _ := l.Allow("a"); !ok {
			t.Fatalf("request %d inside the burst was refused", i+1)
		}
	}
	ok, wait := l.Allow("a")
	if ok {
		t.Fatal("a fourth request passed a burst of 3")
	}
	if wait < 900*time.Millisecond || wait > time.Second {
		t.Errorf("retry-after = %v, want about 1s at 1 token per second", wait)
	}
}

func TestKeysAreIndependent(t *testing.T) {
	t.Parallel()
	l := utils.NewLimiter(0.001, 1, time.Hour, 10)
	if ok, _ := l.Allow("a"); !ok {
		t.Fatal("first key refused")
	}
	if ok, _ := l.Allow("a"); ok {
		t.Fatal("first key passed twice")
	}
	if ok, _ := l.Allow("b"); !ok {
		t.Error("a second key was limited by the first")
	}
}

func TestRefillsOverTimeAndNeverAboveTheBurst(t *testing.T) {
	t.Parallel()
	c := newClock()
	l := utils.NewLimiter(2, 4, time.Hour, 10, utils.WithLimiterClock(c.now))
	for range 4 {
		l.Allow("a")
	}
	c.advance(time.Second) // 2 tokens
	for i := range 2 {
		if ok, _ := l.Allow("a"); !ok {
			t.Fatalf("token %d was not refilled", i+1)
		}
	}
	if ok, _ := l.Allow("a"); ok {
		t.Error("more tokens than the elapsed time earns")
	}
	c.advance(10 * time.Minute) // far more than the burst
	var passed int
	for range 10 {
		if ok, _ := l.Allow("a"); ok {
			passed++
		}
	}
	if passed != 4 {
		t.Errorf("after a long quiet spell %d passed, want exactly the burst of 4", passed)
	}
}

func TestAClockThatGoesBackwardsTakesNothingAway(t *testing.T) {
	t.Parallel()
	c := newClock()
	l := utils.NewLimiter(1, 2, time.Hour, 10, utils.WithLimiterClock(c.now))
	l.Allow("a")
	c.advance(-time.Hour)
	if ok, _ := l.Allow("a"); !ok {
		t.Error("a clock step backwards cost the key its remaining token")
	}
}

func TestIdleKeyStartsFresh(t *testing.T) {
	t.Parallel()
	c := newClock()
	l := utils.NewLimiter(0.0001, 2, time.Minute, 10, utils.WithLimiterClock(c.now))
	l.Allow("a")
	l.Allow("a")
	if ok, _ := l.Allow("a"); ok {
		t.Fatal("setup: the bucket should be empty")
	}
	c.advance(2 * time.Minute)
	if ok, _ := l.Allow("a"); !ok {
		t.Error("a key quiet for longer than idle did not start fresh")
	}
}

func TestTableIsBoundedAndEvictsTheOldest(t *testing.T) {
	t.Parallel()
	l := utils.NewLimiter(0.0001, 1, time.Hour, 100)
	for i := range 10_000 {
		l.Allow(fmt.Sprintf("k%d", i))
	}
	if n := l.Len(); n != 100 {
		t.Errorf("table holds %d keys, want exactly the bound of 100", n)
	}
	// The newest keys are kept (and spent); the oldest were evicted and start fresh.
	if ok, _ := l.Allow("k9999"); ok {
		t.Error("the newest key was evicted")
	}
	if ok, _ := l.Allow("k0"); !ok {
		t.Error("the oldest key was kept past the bound")
	}
}

func TestRecentUseProtectsAKeyFromEviction(t *testing.T) {
	t.Parallel()
	l := utils.NewLimiter(0.0001, 1, time.Hour, 3)
	l.Allow("hot")
	l.Allow("b")
	l.Allow("c")
	l.Allow("hot") // refused, but it is now the most recently used
	l.Allow("d")   // evicts "b", the least recently used
	if ok, _ := l.Allow("hot"); ok {
		t.Error("the recently used key was evicted and got a fresh bucket")
	}
}

func TestZeroRateNeverRefills(t *testing.T) {
	t.Parallel()
	c := newClock()
	l := utils.NewLimiter(0, 1, time.Minute, 10, utils.WithLimiterClock(c.now))
	l.Allow("a")
	c.advance(30 * time.Second)
	ok, wait := l.Allow("a")
	if ok {
		t.Error("a zero rate refilled")
	}
	if wait <= 0 {
		t.Errorf("retry-after = %v, want a positive wait (and no division by zero)", wait)
	}
}

func TestNonPositiveBoundsFallBackToDefaults(t *testing.T) {
	t.Parallel()
	l := utils.NewLimiter(1, -5, 0, 0) // a negative burst is an empty bucket
	if ok, _ := l.Allow("a"); ok {
		t.Error("a negative burst let a request through")
	}
	for i := range 1000 {
		l.Allow(fmt.Sprint(i))
	}
	// "a" plus the thousand: the default bound (100,000) evicted none of them.
	if l.Len() != 1001 {
		t.Errorf("default bound lost keys: %d, want 1001", l.Len())
	}
}

func TestConcurrentCallersNeverExceedTheBurst(t *testing.T) {
	t.Parallel()
	l := utils.NewLimiter(0.0001, 25, time.Hour, 10)
	var ok atomic.Int32
	var wg sync.WaitGroup
	for range 500 {
		wg.Go(func() {
			if allowed, _ := l.Allow("same"); allowed {
				ok.Add(1)
			}
		})
	}
	wg.Wait()
	if ok.Load() != 25 {
		t.Errorf("%d of 500 concurrent calls passed a burst of 25", ok.Load())
	}
}

func TestConcurrentDistinctKeysStayBounded(t *testing.T) {
	t.Parallel()
	l := utils.NewLimiter(1, 1, time.Hour, 50)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 2000 {
				l.Allow(fmt.Sprintf("g%d-%d", g, i))
			}
		})
	}
	wg.Wait()
	if n := l.Len(); n != 50 {
		t.Errorf("table holds %d keys under concurrent load, want the bound of 50", n)
	}
}
