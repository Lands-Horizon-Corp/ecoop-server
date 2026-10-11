//go:build integration

package storage_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/storage"
)

// Zero value & absence: zero structs, nil and empty arguments, handles kept
// after Stop or after the file is gone, nil contexts and nil receivers.

func TestZeroValue_ZeroStruct(t *testing.T) {
	t.Parallel()
	s := &storage.StorageService{}
	ctx := context.Background()
	st := &storage.Storage{StorageKey: "k"}
	mustNotPanic(t, "zero struct", func() {
		if err := s.Run(ctx); !errors.Is(err, storage.ErrNoTracker) {
			t.Errorf("Run = %v", err)
		}
		_, e1 := s.UploadFromBinary(ctx, pngBytes, nil)
		_, e2 := s.Upload(ctx, pngBytes, nil)
		_, e3 := s.GeneratePresignedURL(ctx, st, time.Minute)
		for name, err := range map[string]error{
			"Ping": s.Ping(ctx), "UploadFromBinary": e1, "Upload": e2, "Presign": e3,
			"DeleteFile": s.DeleteFile(ctx, st), "RemoveAllFiles": s.RemoveAllFiles(ctx),
		} {
			if !errors.Is(err, storage.ErrNotStarted) {
				t.Errorf("%s = %v, want ErrNotStarted", name, err)
			}
		}
		if _, err := s.GenerateUniqueName("a.png", ""); err != nil {
			t.Errorf("GenerateUniqueName needs no server: %v", err)
		}
		if err := s.Stop(ctx); err != nil {
			t.Errorf("Stop = %v", err)
		}
	})
}

func TestZeroValue_NilAndEmptyArguments(t *testing.T) {
	t.Parallel()
	s, _ := running(t)
	ctx := ctxT(t)
	if _, err := s.UploadFromBinary(ctx, pngBytes, nil); err != nil {
		t.Errorf("nil callback: %v", err)
	}
	if err := s.DeleteFile(ctx, nil); !errors.Is(err, storage.ErrNilStorage) {
		t.Errorf("DeleteFile(nil) = %v", err)
	}
	if _, err := s.GeneratePresignedURL(ctx, nil, time.Minute); !errors.Is(err, storage.ErrNilStorage) {
		t.Errorf("Presign(nil) = %v", err)
	}
	if _, err := s.UploadFromBinary(ctx, []byte{}, nil); !errors.Is(err, storage.ErrEmptyFile) {
		t.Errorf("empty bytes = %v", err)
	}
	if _, err := s.UploadFromStream(ctx, bytes.NewReader(nil), -1, "", "", nil); !errors.Is(err, storage.ErrEmptyFile) {
		t.Errorf("empty stream = %v", err)
	}
	if _, err := s.UploadFromPath(ctx, "", nil); err == nil {
		t.Error("empty path accepted")
	}
	if k, err := s.GenerateUniqueName("", ""); err != nil || k == "" {
		t.Errorf("GenerateUniqueName with nothing = %q, %v", k, err)
	}
}

// Dangling pointers: a *Storage kept after its file is deleted, after the
// service stopped, and a reader whose file was closed underneath it.
func TestZeroValue_DanglingHandles(t *testing.T) {
	t.Parallel()
	s, bucket := running(t)
	ctx := ctxT(t)
	st, err := s.UploadFromBinary(ctx, pngBytes, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteFile(ctx, st); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteFile(ctx, st); err != nil {
		t.Errorf("deleting an already deleted file: %v (S3 deletes are idempotent)", err)
	}
	if _, err := s.GeneratePresignedURL(ctx, st, time.Minute); err != nil {
		t.Errorf("presigning is offline, a deleted file still gets a (dead) link: %v", err)
	}
	if exists(t, bucket, st.StorageKey) {
		t.Fatal("deleted file came back")
	}

	p := filepath.Join(t.TempDir(), "a.png")
	_ = os.WriteFile(p, pngBytes, 0o644)
	f, _ := os.Open(p)
	_ = f.Close()
	mustNotPanic(t, "closed file", func() {
		if _, err := s.Upload(ctx, io.Reader(f), nil); err == nil {
			t.Error("a closed file uploaded")
		}
	})

	kept := s // a handle kept after Stop
	_ = s.Stop(context.Background())
	if _, err := kept.UploadFromBinary(ctx, pngBytes, nil); !errors.Is(err, storage.ErrNotStarted) {
		t.Errorf("upload through a handle kept after Stop = %v", err)
	}
}

func TestZeroValue_NilContextFailsFast(t *testing.T) {
	t.Parallel()
	s, _ := running(t)
	var nilCtx context.Context
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = capturePanic(func() { _, _ = s.UploadFromBinary(nilCtx, pngBytes, nil) })
		_, _ = capturePanic(func() { _ = s.Ping(nilCtx) })
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("a nil context hung")
	}
}

// A nil *StorageService or nil interface is a wiring bug and fails loudly.
func TestZeroValue_NilReceiver(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var ptr *storage.StorageService
	var iface storage.StorageServices
	ops := map[string]func(storage.StorageServices){
		"Run":    func(s storage.StorageServices) { _ = s.Run(ctx) },
		"Ping":   func(s storage.StorageServices) { _ = s.Ping(ctx) },
		"Upload": func(s storage.StorageServices) { _, _ = s.UploadFromBinary(ctx, pngBytes, nil) },
	}
	for name, op := range ops {
		t.Run(name+"/nil pointer", func(t *testing.T) { mustPanic(t, name, func() { op(ptr) }) })
		t.Run(name+"/nil interface", func(t *testing.T) { mustPanic(t, name, func() { op(iface) }) })
	}
}
