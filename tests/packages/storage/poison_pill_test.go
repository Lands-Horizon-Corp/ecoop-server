//go:build integration

package storage_test

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/storage"
)

// Poison pill: hostile URLs, names and payloads must be refused or
// neutralised, never crash the service, and never leave partial files.

// A URL from a user must not reach the server's own network.
func TestPoison_URLsToPrivateAddressesAreBlocked(t *testing.T) {
	t.Parallel()
	s, bucket := running(t) // private URLs not allowed
	srv := fileServer(t, serveBytes(pngBytes, "image/png"))
	_, port, _ := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
	ctx := ctxT(t)
	for _, u := range []string{
		srv.URL + "/a.png",
		"http://localhost:" + port + "/a.png",
		"http://[::1]:" + port + "/a.png",
		"http://169.254.169.254/latest/meta-data/", // cloud metadata
		"http://10.0.0.1/a.png",
		"http://0.0.0.0:" + port + "/a.png",
	} {
		if _, err := s.UploadFromURL(ctx, u, nil); !errors.Is(err, storage.ErrBlockedAddress) {
			t.Errorf("%s: %v, want ErrBlockedAddress", u, err)
		}
	}
	if count(t, bucket) != 0 {
		t.Fatal("a blocked URL stored something")
	}
}

// A redirect to a private address is blocked too (the check runs at dial
// time, for every hop).
func TestPoison_RedirectToAPrivateAddressIsBlocked(t *testing.T) {
	t.Parallel()
	s, _ := running(t)
	// No public server here, so the first hop is already private; the point
	// is that following redirects goes through the same guarded dialer.
	srv := fileServer(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://127.0.0.1:1/secret", http.StatusFound)
	})
	if _, err := s.UploadFromURL(ctxT(t), srv.URL, nil); !errors.Is(err, storage.ErrBlockedAddress) {
		t.Fatalf("redirect: %v", err)
	}
}

// A server that declares a small length and sends more, or never stops,
// is cut off at the size limit and nothing is stored.
func TestPoison_LyingAndEndlessDownloads(t *testing.T) {
	t.Parallel()
	s, bucket := running(t, storage.AllowPrivateURLs(), storage.MaxFileSize(64<<10))
	ctx := ctxT(t)
	liar := fileServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(payload(1 << 20)) // chunked: no length declared at all
	})
	if _, err := s.UploadFromURL(ctx, liar.URL+"/big.png", nil); !errors.Is(err, storage.ErrTooLarge) {
		t.Errorf("oversized body: %v", err)
	}
	declares := fileServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(10<<20))
	})
	if _, err := s.UploadFromURL(ctx, declares.URL+"/big.png", nil); !errors.Is(err, storage.ErrTooLarge) {
		t.Errorf("declared oversize: %v", err)
	}
	if _, err := s.UploadFromStream(ctx, endless{}, -1, "forever.bin", "", nil); !errors.Is(err, storage.ErrTooLarge) {
		t.Errorf("endless stream: %v", err)
	}
	if n := count(t, bucket); n != 0 {
		t.Fatalf("cut-off uploads left %d objects", n)
	}
}

// A huge base64 string is refused before it is decoded into memory.
func TestPoison_HugeBase64IsRefusedUpFront(t *testing.T) {
	t.Parallel()
	s, _ := running(t, storage.MaxFileSize(1<<20))
	huge := strings.Repeat("A", 8<<20)
	if _, err := s.UploadFromBase64(ctxT(t), huge, nil); !errors.Is(err, storage.ErrTooLarge) {
		t.Fatalf("huge base64: %v", err)
	}
}

// Hostile file names never escape the date folder or reach the key raw.
func TestPoison_HostileFileNames(t *testing.T) {
	t.Parallel()
	s, bucket := running(t)
	ctx := ctxT(t)
	for _, name := range []string{
		"../../../etc/passwd", `..\..\windows\system32\config`, "/absolute/path.png", "a/b/../../c.png",
		"<script>alert(1)</script>.png", "name\x00with-null.png", strings.Repeat("x", 5000) + ".png",
		"résumé 履歴書.pdf", ".hidden", "...", "x.PnG?.exe", "",
	} {
		st, err := s.UploadFromStream(ctx, bytes.NewReader(pngBytes), int64(len(pngBytes)), name, "", nil)
		if err != nil {
			t.Fatalf("%q: %v", name, err)
		}
		key := st.StorageKey
		if strings.Contains(key, "..") || strings.ContainsAny(key, "<>\\\x00 ") || len(key) > 120 || strings.Count(key, "/") != 3 {
			t.Errorf("%q became the unsafe key %q", name, key)
		}
		if !exists(t, bucket, key) {
			t.Errorf("%q: stored under another key than %q", name, key)
		}
	}
}

func TestPoison_UnsupportedUploadValues(t *testing.T) {
	t.Parallel()
	s, bucket := running(t)
	var nilReader *bytes.Reader
	for name, v := range map[string]any{
		"int": 42, "nil": nil, "map": map[string]string{"a": "b"}, "struct": struct{}{}, "func": func() {},
	} {
		var err error
		mustNotPanic(t, name, func() { _, err = s.Upload(ctxT(t), v, nil) })
		if !errors.Is(err, storage.ErrUnsupportedType) {
			t.Errorf("%s: %v, want ErrUnsupportedType", name, err)
		}
	}
	var err error
	mustNotPanic(t, "typed nil reader", func() { _, err = s.Upload(ctxT(t), nilReader, nil) })
	if !errors.Is(err, storage.ErrNilReader) {
		t.Errorf("typed nil reader: %v", err)
	}
	if count(t, bucket) != 0 {
		t.Fatal("refused values stored something")
	}
}

// A callback that panics is the caller's bug. minio calls it from its own
// goroutines, where an unrecovered panic would crash the whole server; it
// must instead fail that upload, store nothing, and leave the service
// working.
func TestPoison_PanickingProgressCallback(t *testing.T) {
	t.Parallel()
	s, bucket := running(t)
	ctx := ctxT(t)
	boom := func(int64, int64, *storage.Storage) { panic("boom") }
	for name, upload := range map[string]func() error{
		"known size": func() error { _, err := s.UploadFromBinary(ctx, payload(1<<20), boom); return err },
		"unknown size": func() error {
			_, err := s.UploadFromStream(ctx, bytes.NewReader(payload(1<<20)), -1, "", "", boom)
			return err
		},
	} {
		var err error
		mustNotPanic(t, name, func() { err = upload() })
		if !errors.Is(err, storage.ErrCallback) {
			t.Errorf("%s: %v, want ErrCallback", name, err)
		}
	}
	if n := count(t, bucket); n != 0 {
		t.Fatalf("a stopped upload left %d objects", n)
	}
	if _, err := s.UploadFromBinary(ctx, pngBytes, nil); err != nil {
		t.Fatalf("service unusable after a panicking callback: %v", err)
	}
}
