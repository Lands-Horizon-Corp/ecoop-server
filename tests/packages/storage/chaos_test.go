//go:build integration

package storage_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/storage"
)

// Chaos: the network, the storage server and the download source misbehave.

// running through a proxy, to cut or slow the connection to the server.
func proxied(t *testing.T, opts ...storage.Option) (storage.StorageServices, string, *proxy) {
	t.Helper()
	p := newProxy(t)
	s, bucket := service(t, p.Addr(), opts...)
	if err := s.Run(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Stop(context.Background()) })
	return s, bucket, p
}

// Cutting the connection mid-upload fails the upload, leaves no object, and
// the next upload works.
func TestChaos_ConnectionCutMidUpload(t *testing.T) {
	t.Parallel()
	s, bucket, p := proxied(t)
	p.SetDelay(20 * time.Millisecond)
	body := &countingReader{r: &slowReader{r: bytes.NewReader(payload(8 << 20)), pause: time.Millisecond}}
	errc := make(chan error, 1)
	go func() {
		_, err := s.UploadFromStream(ctxT(t), body, 8<<20, "big.bin", "", nil)
		errc <- err
	}()
	for body.n.Load() < 1<<20 { // cut it mid-upload, whatever the load on the machine
		time.Sleep(10 * time.Millisecond)
	}
	p.DropAll()
	if err := <-errc; err == nil {
		t.Fatal("upload survived its connection being cut")
	}
	p.SetDelay(0)
	if n := count(t, bucket); n != 0 {
		t.Fatalf("a cut upload left %d objects", n)
	}
	var err error
	for range 3 { // the client may reuse a dead pooled connection once
		if _, err = s.UploadFromBinary(ctxT(t), pngBytes, nil); err == nil {
			break
		}
	}
	if err != nil {
		t.Fatalf("upload after the cut: %v", err)
	}
}

// Cancelling the caller's context stops the upload promptly and leaves
// nothing behind.
func TestChaos_CancelledUploadStopsPromptly(t *testing.T) {
	t.Parallel()
	s, bucket := running(t)
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		_, err := s.UploadFromStream(ctx, &slowReader{r: bytes.NewReader(payload(4 << 20)), pause: 2 * time.Millisecond}, -1, "", "", nil)
		errc <- err
	}()
	time.Sleep(200 * time.Millisecond)
	cancel()
	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("cancelled upload succeeded")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("upload ignored its cancelled context")
	}
	time.Sleep(200 * time.Millisecond)
	if n := count(t, bucket); n != 0 {
		t.Fatalf("a cancelled upload left %d objects", n)
	}
}

// A source that stalls is abandoned after URLTimeout.
func TestChaos_StalledDownloadTimesOut(t *testing.T) {
	t.Parallel()
	s, bucket := running(t, storage.AllowPrivateURLs(), storage.URLTimeout(500*time.Millisecond))
	srv := fileServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(pngBytes[:10])
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-time.After(10 * time.Second):
		}
	})
	start := time.Now()
	_, err := s.UploadFromURL(ctxT(t), srv.URL+"/stall.png", nil)
	if err == nil {
		t.Fatal("stalled download succeeded")
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("stalled download took %v", d)
	}
	if count(t, bucket) != 0 {
		t.Fatal("a stalled download left an object")
	}
}

// A source that drops the connection halfway fails the upload cleanly.
func TestChaos_DownloadCutHalfway(t *testing.T) {
	t.Parallel()
	s, bucket := running(t, storage.AllowPrivateURLs())
	srv := fileServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "100000")
		_, _ = w.Write(payload(1000))
		hj, _ := w.(http.Hijacker)
		conn, _, _ := hj.Hijack()
		_ = conn.Close()
	})
	if _, err := s.UploadFromURL(ctxT(t), srv.URL+"/cut.bin", nil); err == nil {
		t.Fatal("truncated download succeeded")
	}
	if count(t, bucket) != 0 {
		t.Fatal("a truncated download left an object")
	}
}

// With the server gone, Ping and uploads fail (and say so) instead of
// hanging; when it is back, the service works again.
func TestChaos_ServerGoneAndBack(t *testing.T) {
	t.Parallel()
	s, _, p := proxied(t)
	p.SetDelay(30 * time.Second) // nothing gets through
	p.DropAll()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.Ping(ctx); !errors.Is(err, storage.ErrConnect) {
		t.Fatalf("Ping with the server gone = %v", err)
	}
	if _, err := s.UploadFromBinary(ctx, pngBytes, nil); err == nil {
		t.Fatal("upload succeeded with the server gone")
	}
	p.SetDelay(0)
	p.DropAll()
	var err error
	for range 3 {
		if err = s.Ping(ctxT(t)); err == nil {
			break
		}
	}
	if err != nil {
		t.Fatalf("Ping after the server came back: %v", err)
	}
}

// countingReader counts what has been read from r.
type countingReader struct {
	r io.Reader
	n atomic.Int64
}

func (c *countingReader) Read(b []byte) (int, error) {
	n, err := c.r.Read(b)
	c.n.Add(int64(n))
	return n, err
}
