//go:build integration

package storage_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/storage"
)

// Sync & race: one service used from many goroutines, and Stop racing the
// uploads it must wait for. Run with -race.

func TestRace_ConcurrentUploadsGetDistinctKeys(t *testing.T) {
	t.Parallel()
	s, bucket := running(t)
	const n = 40
	keys := make(chan string, n)
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			st, err := s.UploadFromBinary(ctxT(t), pngBytes, nil)
			if err != nil {
				t.Error(err)
				return
			}
			keys <- st.StorageKey
		})
	}
	wg.Wait()
	close(keys)
	seen := map[string]bool{}
	for k := range keys {
		if seen[k] {
			t.Fatalf("key %s used twice", k)
		}
		seen[k] = true
	}
	if count(t, bucket) != n {
		t.Fatalf("objects = %d, want %d", count(t, bucket), n)
	}
}

func TestRace_GenerateUniqueNameIsUnique(t *testing.T) {
	t.Parallel()
	s, _ := running(t)
	var mu sync.Mutex
	seen := map[string]bool{}
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for range 500 {
				k, err := s.GenerateUniqueName("same.png", "image/png")
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				if seen[k] {
					t.Errorf("duplicate key %s", k)
				}
				seen[k] = true
				mu.Unlock()
			}
		})
	}
	wg.Wait()
}

// Stop waits for an upload in progress: it completes and is stored, and
// Stop returns only after it.
func TestRace_StopWaitsForAnUpload(t *testing.T) {
	t.Parallel()
	s, bucket := running(t)
	type result struct {
		st  *storage.Storage
		err error
		at  time.Time
	}
	done := make(chan result, 1)
	go func() {
		st, err := s.UploadFromStream(ctxT(t), &slowReader{r: bytes.NewReader(payload(512 << 10)), pause: 2 * time.Millisecond}, 512<<10, "", "", nil)
		done <- result{st, err, time.Now()}
	}()
	time.Sleep(100 * time.Millisecond)
	if err := s.Stop(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	stopped := time.Now()
	r := <-done
	if r.err != nil {
		t.Fatalf("upload cut off by Stop: %v", r.err)
	}
	if r.at.After(stopped) {
		t.Fatal("Stop returned before the upload finished")
	}
	if !exists(t, bucket, r.st.StorageKey) {
		t.Fatal("the upload Stop waited for is not stored")
	}
}

func TestRace_StopGivesUpWhenCtxEnds(t *testing.T) {
	t.Parallel()
	s, _ := running(t)
	go func() {
		_, _ = s.UploadFromStream(context.Background(), &slowReader{r: bytes.NewReader(payload(1 << 20)), pause: 20 * time.Millisecond}, 1<<20, "", "", nil)
	}()
	time.Sleep(100 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := s.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop = %v, want DeadlineExceeded", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Stop ignored its deadline: %v", d)
	}
}

func TestRace_ConcurrentRun(t *testing.T) {
	t.Parallel()
	s, _ := service(t, endpoint)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if err := s.Run(ctxT(t)); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	defer s.Stop(context.Background())
	if _, err := s.UploadFromBinary(ctxT(t), pngBytes, nil); err != nil {
		t.Fatal(err)
	}
}

// Run and Stop churn under traffic: calls either work or say the service
// is stopped; nothing panics or half-stores.
func TestRace_LifecycleChurnWithTraffic(t *testing.T) {
	t.Parallel()
	s, _ := running(t)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				mustNotPanic(t, "upload during churn", func() {
					if _, err := s.UploadFromBinary(ctxT(t), pngBytes, nil); err != nil && !errors.Is(err, storage.ErrNotStarted) {
						t.Errorf("upload: %v", err)
					}
				})
				time.Sleep(time.Millisecond)
			}
		})
	}
	for i := range 20 {
		if i%2 == 0 {
			_ = s.Stop(ctxT(t))
		} else if err := s.Run(ctxT(t)); err != nil {
			t.Error(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
}
