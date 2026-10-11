//go:build integration

package storage_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/storage"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Sad path: wrong configuration and wrong calls are refused with the right
// error and leave nothing behind.

func TestSad_RunReportsEveryMissingSetting(t *testing.T) {
	t.Parallel()
	s := storage.NewStorageService("", "", "", "", "", "", "", false, nil)
	err := s.Run(context.Background())
	for _, want := range []error{storage.ErrNoEndpoint, storage.ErrNoAccessKey, storage.ErrNoSecretKey, storage.ErrNoBucket} {
		if !errors.Is(err, want) {
			t.Errorf("Run = %v, missing %v", err, want)
		}
	}
}

func TestSad_WrongCredentialsOrServer(t *testing.T) {
	t.Parallel()
	requireServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wrong := storage.NewStorageService(driver, endpoint, "", accessKey, "not-the-secret", bucketName(t), region, false, nil)
	if err := wrong.Run(ctx); !errors.Is(err, storage.ErrConnect) {
		t.Fatalf("wrong secret: %v", err)
	}
	nowhere := storage.NewStorageService(driver, "127.0.0.1:1", "", accessKey, secretKey, bucketName(t), region, false, nil)
	if err := nowhere.Run(ctx); !errors.Is(err, storage.ErrConnect) {
		t.Fatalf("closed port: %v", err)
	}
}

func TestSad_CallsBeforeRunAndAfterStop(t *testing.T) {
	t.Parallel()
	s, _ := service(t, endpoint)
	ctx := ctxT(t)
	st := &storage.Storage{StorageKey: "k"}
	check := func(when string) {
		t.Helper()
		_, e1 := s.UploadFromBinary(ctx, pngBytes, nil)
		_, e2 := s.UploadFromStream(ctx, bytes.NewReader(pngBytes), -1, "", "", nil)
		_, e3 := s.GeneratePresignedURL(ctx, st, time.Minute)
		for name, err := range map[string]error{
			"Ping": s.Ping(ctx), "UploadFromBinary": e1, "UploadFromStream": e2, "Presign": e3,
			"DeleteFile": s.DeleteFile(ctx, st), "RemoveAllFiles": s.RemoveAllFiles(ctx),
		} {
			if !errors.Is(err, storage.ErrNotStarted) {
				t.Errorf("%s: %s = %v, want ErrNotStarted", when, name, err)
			}
		}
	}
	check("before Run")
	if err := s.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	check("after Stop")
}

func TestSad_BadInputsAreRefused(t *testing.T) {
	t.Parallel()
	s, bucket := running(t, storage.MaxFileSize(1024))
	ctx := ctxT(t)
	dir := t.TempDir()
	_ = os.WriteFile(filepath.Join(dir, "empty.txt"), nil, 0o644)
	cases := map[string]struct {
		err  error
		want error
	}{}
	add := func(name string, err, want error) { cases[name] = struct{ err, want error }{err, want} }
	_, err := s.UploadFromBinary(ctx, nil, nil)
	add("nil bytes", err, storage.ErrEmptyFile)
	_, err = s.UploadFromBinary(ctx, payload(2048), nil)
	add("too large", err, storage.ErrTooLarge)
	_, err = s.UploadFromStream(ctx, bytes.NewReader(payload(4096)), -1, "", "", nil)
	add("too large, unknown size", err, storage.ErrTooLarge)
	_, err = s.UploadFromBase64(ctx, "not base64 at all!!", nil)
	add("invalid base64", err, storage.ErrInvalidBase64)
	_, err = s.UploadFromBase64(ctx, "data:image/png,rawbytes", nil)
	add("data URI without base64", err, storage.ErrInvalidBase64)
	_, err = s.UploadFromBase64(ctx, "", nil)
	add("empty base64", err, storage.ErrEmptyFile)
	_, err = s.UploadFromPath(ctx, filepath.Join(dir, "missing.pdf"), nil)
	add("missing file", err, utils.ErrFileNotExist)
	_, err = s.UploadFromPath(ctx, dir, nil)
	add("directory", err, utils.ErrFileIsDir)
	_, err = s.UploadFromPath(ctx, filepath.Join(dir, "empty.txt"), nil)
	add("empty file", err, storage.ErrEmptyFile)
	_, err = s.UploadFromHeader(ctx, nil, nil)
	add("nil header", err, storage.ErrNilHeader)
	_, err = s.UploadFromStream(ctx, nil, 10, "", "", nil)
	add("nil reader", err, storage.ErrNilReader)
	for name, c := range cases {
		if !errors.Is(c.err, c.want) {
			t.Errorf("%s: %v, want %v", name, c.err, c.want)
		}
	}
	if n := count(t, bucket); n != 0 {
		t.Fatalf("refused uploads left %d objects", n)
	}
}

func TestSad_URLsThatAreNotDownloads(t *testing.T) {
	t.Parallel()
	s, _ := running(t, storage.AllowPrivateURLs())
	ctx := ctxT(t)
	for _, u := range []string{"ftp://example.com/a.png", "file:///etc/passwd", "javascript:alert(1)", "//example.com/a", "http://", "not a url"} {
		if _, err := s.UploadFromURL(ctx, u, nil); !errors.Is(err, storage.ErrInvalidURL) {
			t.Errorf("%q: %v, want ErrInvalidURL", u, err)
		}
	}
	missing := fileServer(t, func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) })
	if _, err := s.UploadFromURL(ctx, missing.URL+"/gone.png", nil); !errors.Is(err, storage.ErrDownload) {
		t.Errorf("404: %v, want ErrDownload", err)
	}
}

func TestSad_DeleteAndPresignNeedAFileOfThisBucket(t *testing.T) {
	t.Parallel()
	s, _ := running(t)
	ctx := ctxT(t)
	other := &storage.Storage{StorageKey: "k", BucketName: "someone-elses-bucket"}
	if err := s.DeleteFile(ctx, other); !errors.Is(err, storage.ErrWrongBucket) {
		t.Errorf("delete in another bucket: %v", err)
	}
	if _, err := s.GeneratePresignedURL(ctx, other, time.Minute); !errors.Is(err, storage.ErrWrongBucket) {
		t.Errorf("presign in another bucket: %v", err)
	}
	if err := s.DeleteFile(ctx, &storage.Storage{}); !errors.Is(err, storage.ErrNoKey) {
		t.Errorf("delete without key: %v", err)
	}
	st := &storage.Storage{StorageKey: "k"}
	for _, d := range []time.Duration{0, -time.Minute, time.Millisecond, 8 * 24 * time.Hour} {
		if _, err := s.GeneratePresignedURL(ctx, st, d); !errors.Is(err, storage.ErrInvalidExpiry) {
			t.Errorf("expiry %v: %v", d, err)
		}
	}
}
