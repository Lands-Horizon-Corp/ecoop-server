//go:build integration

package storage_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/storage"
	"github.com/Lands-Horizon-Corp/ecoop-server/tests/internal/obstest"
)

func newObserver(t testing.TB) *observability.Service {
	t.Helper()
	return obstest.New(t)
}

func storageCount(o *observability.Service, op, outcome string) float64 {
	return obstest.Count(o, "storage", op, outcome)
}

// Upload, presign and delete are each one counted operation.
func TestObserver_UploadPresignDeleteAreOperations(t *testing.T) {
	t.Parallel()
	o := newObserver(t)
	s, _ := running(t, storage.WithObserver(o))
	ctx := ctxT(t)
	st, err := s.UploadFromBinary(ctx, payload(2048), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.GeneratePresignedURL(ctx, st, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteFile(ctx, st); err != nil {
		t.Fatal(err)
	}
	for _, op := range []string{"upload", "presign", "delete_file", "run"} {
		if got := storageCount(o, op, "success"); got != 1 {
			t.Fatalf("%s success = %v, want 1", op, got)
		}
	}
}

// A stream that turns out empty or too large is the caller's answer: skipped, never an error.
func TestObserver_EmptyAndTooLargeStreamsAreSkipped(t *testing.T) {
	t.Parallel()
	o := newObserver(t)
	s, _ := running(t, storage.WithObserver(o), storage.WithMaxFileSize(1024))
	ctx := ctxT(t)
	if _, err := s.UploadFromStream(ctx, bytes.NewReader(nil), -1, "", "", nil); !errors.Is(err, storage.ErrEmptyFile) {
		t.Fatalf("empty stream = %v, want ErrEmptyFile", err)
	}
	if _, err := s.UploadFromStream(ctx, bytes.NewReader(payload(4096)), -1, "", "", nil); !errors.Is(err, storage.ErrTooLarge) {
		t.Fatalf("large stream = %v, want ErrTooLarge", err)
	}
	if got := storageCount(o, "upload", "skipped"); got != 2 {
		t.Fatalf("upload skipped = %v, want 2", got)
	}
	if got := storageCount(o, "upload", "error"); got != 0 {
		t.Fatalf("upload error = %v, want 0", got)
	}
}

// A URL on a private address is refused and ends as skipped.
func TestObserver_BlockedAddressIsSkipped(t *testing.T) {
	t.Parallel()
	o := newObserver(t)
	s, _ := running(t, storage.WithObserver(o))
	srv := fileServer(t, serveBytes(payload(64), "image/png"))
	if _, err := s.UploadFromURL(ctxT(t), srv.URL+"/a.png", nil); !errors.Is(err, storage.ErrBlockedAddress) {
		t.Fatalf("UploadFromURL = %v, want ErrBlockedAddress", err)
	}
	if got := storageCount(o, "fetch_url", "skipped"); got != 1 {
		t.Fatalf("fetch_url skipped = %v, want 1", got)
	}
	if got := storageCount(o, "fetch_url", "error"); got != 0 {
		t.Fatalf("fetch_url error = %v, want 0", got)
	}
}

// Calls refused before any work are not counted.
func TestObserver_RefusedCallsAreNotCounted(t *testing.T) {
	t.Parallel()
	o := newObserver(t)
	s, _ := running(t, storage.WithObserver(o))
	ctx := ctxT(t)
	if err := s.DeleteFile(ctx, nil); !errors.Is(err, storage.ErrNilStorage) {
		t.Fatalf("DeleteFile(nil) = %v", err)
	}
	if _, err := s.GeneratePresignedURL(ctx, &storage.Storage{StorageKey: "k"}, 0); !errors.Is(err, storage.ErrInvalidExpiry) {
		t.Fatalf("presign = %v", err)
	}
	if _, err := s.UploadFromBinary(ctx, nil, nil); !errors.Is(err, storage.ErrEmptyFile) {
		t.Fatalf("empty upload = %v", err)
	}
	for _, op := range []string{"delete_file", "presign", "upload"} {
		for _, outcome := range []string{"success", "error", "skipped"} {
			if got := storageCount(o, op, outcome); got != 0 {
				t.Fatalf("a refused %s was counted as %s: %v", op, outcome, got)
			}
		}
	}
}

// Neither a file name, a key, a bucket nor a URL reaches the registry.
func TestObserver_NoNameKeyOrBucketIsRecorded(t *testing.T) {
	t.Parallel()
	o := newObserver(t)
	s, bucket := running(t, storage.WithObserver(o))
	ctx := ctxT(t)
	st, err := s.UploadFromStream(ctx, bytes.NewReader(payload(512)), 512, "private-statement.pdf", "application/pdf", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, sm := range o.Snapshot() {
		blob := sm.Name
		for k, v := range sm.Attrs {
			blob += " " + k + "=" + v
		}
		for _, leak := range []string{"private-statement", st.StorageKey, bucket, endpoint} {
			if strings.Contains(blob, leak) {
				t.Fatalf("%q leaked into the registry: %s", leak, blob)
			}
		}
	}
}

// No observer, and a bare struct, are fine.
func TestObserver_NoneIsFine(t *testing.T) {
	t.Parallel()
	s, _ := running(t)
	if _, err := s.UploadFromBinary(ctxT(t), payload(128), nil); err != nil {
		t.Fatal(err)
	}
	var zero storage.StorageService
	if err := zero.Ping(context.Background()); err == nil {
		t.Fatal("a zero service must return an error, not panic")
	}
}
