//go:build integration

package storage_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/storage"
)

// Tests run against the S3-compatible storage service in docker-compose.yml
// (RustFS), configured from STORAGE_* in the environment or the repo's
// .env. Every test gets its own bucket, emptied and removed when it ends;
// the configured bucket is never touched.
var (
	endpoint   string // host:port
	driver     string
	accessKey  string
	secretKey  string
	region     string
	admin      *minio.Client // inspects and removes test buckets
	skipReason string
	runID      = time.Now().Unix()
	bucketSeq  atomic.Int64
)

var _ storage.IStorageService = (*storage.StorageService)(nil)

func TestMain(m *testing.M) {
	if err := startStorage(); err != nil {
		skipReason = "no storage server: " + err.Error()
		fmt.Fprintln(os.Stderr, "storage tests:", skipReason)
	}
	os.Exit(m.Run())
}

func startStorage() error {
	root, err := moduleRoot()
	if err != nil {
		return err
	}
	env := map[string]string{}
	if data, err := os.ReadFile(filepath.Join(root, ".env")); err == nil {
		for line := range strings.SplitSeq(string(data), "\n") {
			if k, v, ok := strings.Cut(strings.TrimSpace(line), "="); ok && !strings.HasPrefix(k, "#") {
				env[strings.TrimSpace(k)] = strings.Trim(strings.TrimSpace(v), `"'`)
			}
		}
	}
	for _, k := range []string{"STORAGE_DRIVER", "STORAGE_HOST", "STORAGE_PORT", "STORAGE_ACCESS_KEY", "STORAGE_SECRET_KEY", "STORAGE_REGION"} {
		if v, ok := os.LookupEnv(k); ok {
			env[k] = v
		}
		if env[k] == "" {
			return fmt.Errorf("set %s in .env or the environment", k)
		}
	}
	endpoint = net.JoinHostPort(env["STORAGE_HOST"], env["STORAGE_PORT"])
	driver, accessKey, secretKey, region = env["STORAGE_DRIVER"], env["STORAGE_ACCESS_KEY"], env["STORAGE_SECRET_KEY"], env["STORAGE_REGION"]
	if _, err := exec.LookPath("docker"); err != nil {
		return errors.New("docker not installed")
	}
	up := exec.Command("docker", "compose", "up", "-d", "--wait", "storage")
	up.Dir = root
	if out, err := up.CombinedOutput(); err != nil {
		return fmt.Errorf("docker compose up storage: %w\n%s", err, out)
	}
	admin, err = minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(accessKey, secretKey, ""), Region: region})
	if err != nil {
		return err
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := admin.ListBuckets(ctx)
		cancel()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("storage server never answered: %w", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func moduleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("go.mod not found")
		}
		dir = parent
	}
}

func requireServer(t testing.TB) {
	t.Helper()
	if skipReason != "" {
		t.Skip(skipReason)
	}
}

func ctxT(t testing.TB) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	return ctx
}

// bucketName is a new bucket name for t, removed with everything in it
// when t ends.
func bucketName(t testing.TB) string {
	t.Helper()
	requireServer(t)
	name := fmt.Sprintf("t-%d-%d", runID, bucketSeq.Add(1))
	t.Cleanup(func() { removeBucket(name) })
	return name
}

func removeBucket(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	objects := admin.ListObjects(ctx, name, minio.ListObjectsOptions{Recursive: true})
	for range admin.RemoveObjects(ctx, name, objects, minio.RemoveObjectsOptions{}) {
	}
	_ = admin.RemoveBucket(ctx, name)
}

// service builds (not running) a service on its own bucket, through addr
// (the server, or a proxy to it).
func service(t testing.TB, addr string, opts ...storage.Option) (storage.IStorageService, string) {
	t.Helper()
	bucket := bucketName(t)
	return storage.NewStorageService(driver, addr, "", accessKey, secretKey, bucket, region, false, nil, opts...), bucket
}

// running is a running service on its own bucket, stopped when t ends.
func running(t testing.TB, opts ...storage.Option) (storage.IStorageService, string) {
	t.Helper()
	requireServer(t)
	s, bucket := service(t, endpoint, opts...)
	if err := s.Run(ctxT(t)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Cleanup(func() { _ = s.Stop(context.Background()) })
	return s, bucket
}

// object reads a stored file straight from the server.
func object(t testing.TB, bucket, key string) []byte {
	t.Helper()
	o, err := admin.GetObject(ctxT(t), bucket, key, minio.GetObjectOptions{})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer o.Close()
	b, err := io.ReadAll(o)
	if err != nil {
		t.Fatalf("reading %s: %v", key, err)
	}
	return b
}

func exists(t testing.TB, bucket, key string) bool {
	t.Helper()
	_, err := admin.StatObject(ctxT(t), bucket, key, minio.StatObjectOptions{})
	return err == nil
}

// count is how many objects bucket holds.
func count(t testing.TB, bucket string) int {
	t.Helper()
	n := 0
	for o := range admin.ListObjects(ctxT(t), bucket, minio.ListObjectsOptions{Recursive: true}) {
		if o.Err != nil {
			t.Fatalf("listing %s: %v", bucket, o.Err)
		}
		n++
	}
	return n
}

// Sample files.
var (
	pngBytes = []byte("\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\rIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82")
	pdfBytes = []byte("%PDF-1.4\n1 0 obj << /Type /Catalog >> endobj\ntrailer << /Root 1 0 R >>\n%%EOF\n")
)

// payload is n bytes of deterministic, non-repeating-looking content.
func payload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31 + i/7)
	}
	return b
}

// fileHeader builds a *multipart.FileHeader the way an HTTP handler gets it.
func fileHeader(t testing.TB, name, contentType string, content []byte) *multipart.FileHeader {
	t.Helper()
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	h := make(map[string][]string)
	h["Content-Disposition"] = []string{fmt.Sprintf(`form-data; name="file"; filename=%q`, name)}
	h["Content-Type"] = []string{contentType}
	part, err := w.CreatePart(h)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = part.Write(content)
	_ = w.Close()
	req, _ := http.NewRequest(http.MethodPost, "/", &body)
	req.Header.Set("Content-Type", w.FormDataContentType())
	if err := req.ParseMultipartForm(32 << 20); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = req.MultipartForm.RemoveAll() })
	return req.MultipartForm.File["file"][0]
}

// fileServer serves handler for UploadFromURL tests, closed when t ends.
func fileServer(t testing.TB, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv
}

func serveBytes(content []byte, contentType string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", contentType)
		_, _ = w.Write(content)
	}
}

// progressLog records progress callbacks.
type progressLog struct {
	mu    sync.Mutex
	calls [][2]int64
}

func (p *progressLog) cb(done, total int64, _ *storage.Storage) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, [2]int64{done, total})
}

func (p *progressLog) all() [][2]int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([][2]int64(nil), p.calls...)
}

// slowReader serves content in small pieces, pausing between them.
type slowReader struct {
	r     io.Reader
	pause time.Duration
}

func (s *slowReader) Read(b []byte) (int, error) {
	time.Sleep(s.pause)
	if len(b) > 4096 {
		b = b[:4096]
	}
	return s.r.Read(b)
}

// endless is a reader that never ends.
type endless struct{}

func (endless) Read(b []byte) (int, error) {
	for i := range b {
		b[i] = 'x'
	}
	return len(b), nil
}

func capturePanic(fn func()) (recovered any, stack string) {
	defer func() {
		if r := recover(); r != nil {
			recovered, stack = r, string(debug.Stack())
		}
	}()
	fn()
	return nil, ""
}

func mustNotPanic(t *testing.T, name string, fn func()) {
	t.Helper()
	if r, stack := capturePanic(fn); r != nil {
		if testing.Verbose() {
			t.Log(stack)
		}
		t.Fatalf("%s panicked: %v", name, r)
	}
}

func mustPanic(t *testing.T, name string, fn func()) {
	t.Helper()
	if r, _ := capturePanic(fn); r == nil {
		t.Fatalf("%s: expected panic, got none", name)
	}
}

// proxy relays TCP to the storage server and can delay or cut traffic.
type proxy struct {
	ln    net.Listener
	delay atomic.Int64
	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

func newProxy(t testing.TB) *proxy {
	t.Helper()
	requireServer(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &proxy{ln: ln, conns: map[net.Conn]struct{}{}}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			s, err := net.Dial("tcp", endpoint)
			if err != nil {
				_ = c.Close()
				continue
			}
			p.mu.Lock()
			p.conns[c], p.conns[s] = struct{}{}, struct{}{}
			p.mu.Unlock()
			go p.pipe(s, c)
			go p.pipe(c, s)
		}
	}()
	t.Cleanup(func() { _ = ln.Close(); p.DropAll() })
	return p
}

func (p *proxy) pipe(dst, src net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if d := time.Duration(p.delay.Load()); d > 0 {
				time.Sleep(d)
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	_ = dst.Close()
	_ = src.Close()
}

func (p *proxy) Addr() string { return p.ln.Addr().String() }

func (p *proxy) SetDelay(d time.Duration) { p.delay.Store(int64(d)) }

func (p *proxy) DropAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for c := range p.conns {
		_ = c.Close()
	}
	clear(p.conns)
}
