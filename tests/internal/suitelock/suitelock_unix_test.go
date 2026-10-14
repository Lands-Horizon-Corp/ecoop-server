//go:build unix

package suitelock

import (
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func useTempLock(t *testing.T) {
	t.Helper()
	old := lockPath
	lockPath = filepath.Join(t.TempDir(), "test.lock")
	t.Cleanup(func() { lockPath = old })
}

// A second holder waits, then gets the lock once the first lets go.
func TestAcquireExcludesAndHandsOver(t *testing.T) {
	useTempLock(t)
	first, err := Acquire("first", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Acquire("second", 300*time.Millisecond); err == nil {
		t.Fatal("the lock was granted while it was held")
	}
	first()
	second, err := Acquire("second", time.Second)
	if err != nil {
		t.Fatalf("the lock was not granted after release: %v", err)
	}
	second()
}

// Releasing twice is harmless and does not release someone else's lock.
func TestReleaseTwiceIsHarmless(t *testing.T) {
	useTempLock(t)
	a, err := Acquire("a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	a()
	b, err := Acquire("b", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	a() // must not unlock b
	if _, err := Acquire("c", 300*time.Millisecond); err == nil {
		t.Fatal("a second release of a stale handle freed the lock b holds")
	}
	b()
}

// Many contenders take turns: never two at once.
func TestManyContendersNeverOverlap(t *testing.T) {
	useTempLock(t)
	var (
		mu      sync.Mutex
		inside  int
		maxSeen int
		wg      sync.WaitGroup
	)
	for range 8 {
		wg.Go(func() {
			release, err := Acquire("n", 20*time.Second)
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			inside++
			maxSeen = max(maxSeen, inside)
			mu.Unlock()
			time.Sleep(20 * time.Millisecond)
			mu.Lock()
			inside--
			mu.Unlock()
			release()
		})
	}
	wg.Wait()
	if maxSeen != 1 {
		t.Fatalf("%d holders at once", maxSeen)
	}
}
