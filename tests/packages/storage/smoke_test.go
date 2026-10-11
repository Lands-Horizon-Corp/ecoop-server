//go:build integration

package storage_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"
)

// Smoke: run, upload, fetch through a presigned link, delete, stop.
func TestSmoke(t *testing.T) {
	t.Parallel()
	s, bucket := running(t)
	ctx := ctxT(t)
	if err := s.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	st, err := s.UploadFromBinary(ctx, pngBytes, nil)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	link, err := s.GeneratePresignedURL(ctx, st, time.Minute)
	if err != nil {
		t.Fatalf("presign: %v", err)
	}
	resp, err := http.Get(link)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !bytes.Equal(got, pngBytes) {
		t.Fatalf("presigned GET = %d, %d bytes", resp.StatusCode, len(got))
	}
	if err := s.DeleteFile(ctx, st); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if exists(t, bucket, st.StorageKey) {
		t.Fatal("file still there after delete")
	}
	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
