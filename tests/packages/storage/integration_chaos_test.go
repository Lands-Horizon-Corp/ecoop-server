//go:build integration

package storage_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/storage"
)

// Integration chaos: several instances of the app sharing one bucket, under
// random traffic and shutdowns.

// replicas are n services on one bucket, like instances behind a load
// balancer.
func replicas(t *testing.T, n int) ([]storage.StorageServices, string) {
	t.Helper()
	bucket := bucketName(t)
	out := make([]storage.StorageServices, n)
	for i := range out {
		out[i] = storage.NewStorageService(driver, endpoint, "", accessKey, secretKey, bucket, region, false, nil)
		if err := out[i].Run(ctxT(t)); err != nil {
			t.Fatal(err)
		}
		s := out[i]
		t.Cleanup(func() { _ = s.Stop(context.Background()) })
	}
	return out, bucket
}

// Random uploads (every kind) and deletes from every replica: the bucket
// ends up holding exactly the files that were uploaded and not deleted.
func TestIntegrationChaos_RandomTrafficFromReplicas(t *testing.T) {
	t.Parallel()
	ss, bucket := replicas(t, 3)
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed %d", seed)
	var mu sync.Mutex
	kept := map[string]bool{}
	var wg sync.WaitGroup
	for w := range 9 {
		wg.Go(func() {
			r := rand.New(rand.NewPCG(seed, uint64(w)))
			ctx := ctxT(t)
			var mine []*storage.Storage
			for range 15 {
				s := ss[r.IntN(len(ss))]
				if len(mine) > 0 && r.IntN(4) == 0 {
					victim := mine[r.IntN(len(mine))]
					if err := s.DeleteFile(ctx, victim); err != nil {
						t.Errorf("delete: %v", err)
						return
					}
					mu.Lock()
					delete(kept, victim.StorageKey)
					mu.Unlock()
					continue
				}
				data := payload(1 + r.IntN(200_000))
				var st *storage.Storage
				var err error
				switch r.IntN(3) {
				case 0:
					st, err = s.UploadFromBinary(ctx, data, nil)
				case 1:
					st, err = s.UploadFromBase64(ctx, base64.StdEncoding.EncodeToString(data), nil)
				default:
					st, err = s.UploadFromStream(ctx, bytes.NewReader(data), -1, "x.bin", "", nil)
				}
				if err != nil {
					t.Errorf("upload: %v", err)
					return
				}
				mine = append(mine, st)
				mu.Lock()
				kept[st.StorageKey] = true
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if n := count(t, bucket); n != len(kept) {
		t.Fatalf("bucket holds %d objects, want %d", n, len(kept))
	}
	for key := range kept {
		if !exists(t, bucket, key) {
			t.Fatalf("%s is missing", key)
		}
	}
}

// One replica shutting down mid-traffic: its uploads in flight finish,
// new ones are refused, and the other replica carries on.
func TestIntegrationChaos_OneReplicaStopsDuringTraffic(t *testing.T) {
	t.Parallel()
	ss, bucket := replicas(t, 2)
	ctx := ctxT(t)
	var stored, refused atomic.Int64
	var wg sync.WaitGroup
	for range 6 {
		wg.Go(func() {
			for range 10 {
				_, err := ss[0].UploadFromStream(ctx, &slowReader{r: bytes.NewReader(payload(64 << 10)), pause: time.Millisecond}, 64<<10, "", "", nil)
				switch {
				case err == nil:
					stored.Add(1)
				case errors.Is(err, storage.ErrNotStarted):
					refused.Add(1)
				default:
					t.Errorf("upload: %v", err)
				}
			}
		})
	}
	time.Sleep(150 * time.Millisecond)
	if err := ss[0].Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	wg.Wait()
	if _, err := ss[1].UploadFromBinary(ctx, pngBytes, nil); err != nil {
		t.Fatalf("the other replica: %v", err)
	}
	if got := int64(count(t, bucket)); got != stored.Load()+1 {
		t.Fatalf("bucket holds %d, want %d (stored) + 1; a half-done upload got through", got, stored.Load())
	}
	if refused.Load() == 0 {
		t.Log("Stop came after every upload had started")
	}
}

// RemoveAllFiles from one replica empties the bucket for all of them.
func TestIntegrationChaos_RemoveAllFromOneReplica(t *testing.T) {
	t.Parallel()
	ss, bucket := replicas(t, 2)
	ctx := ctxT(t)
	for i := range 20 {
		if _, err := ss[i%2].UploadFromBinary(ctx, payload(500+i), nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := ss[1].RemoveAllFiles(ctx); err != nil {
		t.Fatal(err)
	}
	if n := count(t, bucket); n != 0 {
		t.Fatalf("%d objects left", n)
	}
}
