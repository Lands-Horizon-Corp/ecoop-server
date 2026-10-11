//go:build integration

package storage_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/storage"
)

// Happy path: every way in stores the right bytes and describes them.

func checkStored(t *testing.T, bucket string, st *storage.Storage, want []byte, fileType string) {
	t.Helper()
	if st.Status != storage.StatusCompleted || st.BucketName != bucket || st.StorageKey == "" {
		t.Fatalf("storage = %+v", st)
	}
	if st.FileSize != int64(len(want)) || st.Progress != int64(len(want)) {
		t.Fatalf("size %d progress %d, want %d", st.FileSize, st.Progress, len(want))
	}
	if fileType != "" && st.FileType != fileType {
		t.Fatalf("type = %q, want %q", st.FileType, fileType)
	}
	if got := object(t, bucket, st.StorageKey); !bytes.Equal(got, want) {
		t.Fatalf("stored %d bytes differ from the %d sent", len(got), len(want))
	}
}

func TestHappy_RunCreatesTheBucket(t *testing.T) {
	t.Parallel()
	_, bucket := running(t)
	if ok, err := admin.BucketExists(ctxT(t), bucket); err != nil || !ok {
		t.Fatalf("bucket %s exists = %v, %v", bucket, ok, err)
	}
}

func TestHappy_UploadFromBinaryDetectsTheType(t *testing.T) {
	t.Parallel()
	s, bucket := running(t)
	st, err := s.UploadFromBinary(ctxT(t), pngBytes, nil)
	if err != nil {
		t.Fatal(err)
	}
	checkStored(t, bucket, st, pngBytes, "image/png")
	if !strings.HasSuffix(st.StorageKey, ".png") {
		t.Fatalf("key %q has no .png extension", st.StorageKey)
	}
}

func TestHappy_UploadFromPath(t *testing.T) {
	t.Parallel()
	s, bucket := running(t)
	p := filepath.Join(t.TempDir(), "statement.pdf")
	if err := os.WriteFile(p, pdfBytes, 0o644); err != nil {
		t.Fatal(err)
	}
	st, err := s.UploadFromPath(ctxT(t), p, nil)
	if err != nil {
		t.Fatal(err)
	}
	checkStored(t, bucket, st, pdfBytes, "application/pdf")
	if st.FileName != "statement.pdf" {
		t.Fatalf("name = %q", st.FileName)
	}
}

func TestHappy_UploadFromBase64(t *testing.T) {
	t.Parallel()
	s, bucket := running(t)
	ctx := ctxT(t)
	for name, in := range map[string]string{
		"data URI": "data:image/png;base64," + base64.StdEncoding.EncodeToString(pngBytes),
		"bare":     base64.StdEncoding.EncodeToString(pngBytes),
		"unpadded": base64.RawStdEncoding.EncodeToString(pngBytes),
		"url-safe": base64.URLEncoding.EncodeToString(pngBytes),
		"line-wrapped": func() string {
			e := base64.StdEncoding.EncodeToString(pngBytes)
			return e[:20] + "\n" + e[20:40] + "\r\n" + e[40:]
		}(),
	} {
		st, err := s.UploadFromBase64(ctx, in, nil)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		checkStored(t, bucket, st, pngBytes, "image/png")
	}
}

func TestHappy_UploadFromURL(t *testing.T) {
	t.Parallel()
	s, bucket := running(t, storage.AllowPrivateURLs()) // the test server is local
	srv := fileServer(t, serveBytes(pdfBytes, "application/pdf; charset=binary"))
	st, err := s.UploadFromURL(ctxT(t), srv.URL+"/docs/loan-terms.pdf", nil)
	if err != nil {
		t.Fatal(err)
	}
	checkStored(t, bucket, st, pdfBytes, "application/pdf")
	if st.FileName != "loan-terms.pdf" {
		t.Fatalf("name = %q", st.FileName)
	}
}

func TestHappy_UploadFromHeader(t *testing.T) {
	t.Parallel()
	s, bucket := running(t)
	h := fileHeader(t, "id-front.png", "image/png", pngBytes)
	st, err := s.UploadFromHeader(ctxT(t), h, nil)
	if err != nil {
		t.Fatal(err)
	}
	checkStored(t, bucket, st, pngBytes, "image/png")
	if st.FileName != "id-front.png" {
		t.Fatalf("name = %q", st.FileName)
	}
}

// An unknown-size stream bigger than one multipart part, with progress.
func TestHappy_UploadFromStreamOfUnknownSize(t *testing.T) {
	t.Parallel()
	s, bucket := running(t)
	data := payload(storage.UnknownPartSize + 3<<20)
	var log progressLog
	st, err := s.UploadFromStream(ctxT(t), bytes.NewReader(data), -1, "export.csv", "text/csv", log.cb)
	if err != nil {
		t.Fatal(err)
	}
	checkStored(t, bucket, st, data, "text/csv")
	calls := log.all()
	if len(calls) < 2 {
		t.Fatalf("progress reported %d times", len(calls))
	}
	for i := 1; i < len(calls); i++ {
		if calls[i][0] < calls[i-1][0] {
			t.Fatalf("progress went backwards: %v -> %v", calls[i-1], calls[i])
		}
	}
	if last := calls[len(calls)-1]; last[0] != int64(len(data)) {
		t.Fatalf("final progress %v, want %d", last, len(data))
	}
}

func TestHappy_ProgressOfAKnownSize(t *testing.T) {
	t.Parallel()
	s, _ := running(t)
	data := payload(1 << 20)
	var log progressLog
	if _, err := s.UploadFromBinary(ctxT(t), data, log.cb); err != nil {
		t.Fatal(err)
	}
	calls := log.all()
	if len(calls) == 0 || calls[len(calls)-1] != [2]int64{int64(len(data)), int64(len(data))} {
		t.Fatalf("progress = %v", calls)
	}
}

func TestHappy_UploadPicksTheRightWay(t *testing.T) {
	t.Parallel()
	s, bucket := running(t, storage.AllowPrivateURLs())
	ctx := ctxT(t)
	p := filepath.Join(t.TempDir(), "a.png")
	_ = os.WriteFile(p, pngBytes, 0o644)
	srv := fileServer(t, serveBytes(pngBytes, "image/png"))
	for name, in := range map[string]any{
		"path":     p,
		"url":      srv.URL + "/a.png",
		"data URI": "data:image/png;base64," + base64.StdEncoding.EncodeToString(pngBytes),
		"bytes":    pngBytes,
		"reader":   bytes.NewReader(pngBytes),
		"header":   fileHeader(t, "a.png", "image/png", pngBytes),
	} {
		st, err := s.Upload(ctx, in, nil)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		checkStored(t, bucket, st, pngBytes, "image/png")
	}
}

func TestHappy_PresignedURLDownloads(t *testing.T) {
	t.Parallel()
	s, _ := running(t)
	ctx := ctxT(t)
	st, err := s.UploadFromBinary(ctx, pdfBytes, nil)
	if err != nil {
		t.Fatal(err)
	}
	link, err := s.GeneratePresignedURL(ctx, st, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(link)
	if u.Host != endpoint || u.Query().Get("X-Amz-Expires") != "300" {
		t.Fatalf("link = %s", link)
	}
}

// With a public endpoint, links are signed for it (browsers reach the
// server there), over https.
func TestHappy_PresignedURLUsesThePublicEndpoint(t *testing.T) {
	t.Parallel()
	requireServer(t)
	bucket := bucketName(t)
	s := storage.NewStorageService(driver, endpoint, "files.ecoop.test", accessKey, secretKey, bucket, region, false, nil)
	if err := s.Run(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	defer s.Stop(context.Background())
	st, err := s.UploadFromBinary(ctxT(t), pngBytes, nil)
	if err != nil {
		t.Fatal(err)
	}
	link, err := s.GeneratePresignedURL(ctxT(t), st, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(link, "https://files.ecoop.test/"+bucket+"/") {
		t.Fatalf("link = %s", link)
	}
	if !strings.HasPrefix(st.URL, "https://files.ecoop.test/") {
		t.Fatalf("URL = %s", st.URL)
	}
}

func TestHappy_DeleteAndRemoveAll(t *testing.T) {
	t.Parallel()
	s, bucket := running(t)
	ctx := ctxT(t)
	var stored []*storage.Storage
	for range 5 {
		st, err := s.UploadFromBinary(ctx, payload(1000), nil)
		if err != nil {
			t.Fatal(err)
		}
		stored = append(stored, st)
	}
	if err := s.DeleteFile(ctx, stored[0]); err != nil {
		t.Fatal(err)
	}
	if count(t, bucket) != 4 {
		t.Fatalf("objects = %d, want 4", count(t, bucket))
	}
	if err := s.RemoveAllFiles(ctx); err != nil {
		t.Fatal(err)
	}
	if count(t, bucket) != 0 {
		t.Fatalf("objects after RemoveAllFiles = %d", count(t, bucket))
	}
}

func TestHappy_GenerateUniqueName(t *testing.T) {
	t.Parallel()
	s, _ := running(t)
	key, err := s.GenerateUniqueName("Loan Contract (final).PDF", "application/pdf")
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^\d{4}/\d{2}/\d{2}/[0-9a-f]{24}-Loan-Contract-final\.pdf$`).MatchString(key) {
		t.Fatalf("key = %q", key)
	}
	key, _ = s.GenerateUniqueName("", "image/jpeg")
	if !regexp.MustCompile(`^\d{4}/\d{2}/\d{2}/[0-9a-f]{24}\.(jpe?g|jfif|jpe)$`).MatchString(key) {
		t.Fatalf("key from the content type = %q", key)
	}
}

func TestHappy_RunIsIdempotentAndRestartable(t *testing.T) {
	t.Parallel()
	s, _ := running(t)
	ctx := ctxT(t)
	for range 3 {
		if err := s.Run(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		if err := s.Stop(ctx); err != nil {
			t.Fatal(err)
		}
		if err := s.Run(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := s.UploadFromBinary(ctx, pngBytes, nil); err != nil {
			t.Fatalf("upload after restart: %v", err)
		}
	}
}
