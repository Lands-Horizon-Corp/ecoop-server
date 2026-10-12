//go:build integration

package pdf_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pdf"
)

// Tests run against the pdf-engine service in docker-compose.yml, configured
// from PDF_ENGINE_API_* (and STORAGE_* / PDF_BUCKET to check stored PDFs) in
// the environment or the repo's .env.
var (
	engineHost, enginePort, token, bucket string
	store                                 *minio.Client
	skipReason                            string
)

var _ pdf.PDFServices = (*pdf.PDFEngineService)(nil)

func TestMain(m *testing.M) {
	if err := startEngine(); err != nil {
		skipReason = "no pdf-engine: " + err.Error()
		fmt.Fprintln(os.Stderr, "pdf tests:", skipReason)
	}
	os.Exit(m.Run())
}

func startEngine() error {
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
	keys := []string{"PDF_ENGINE_API_HOST", "PDF_ENGINE_API_PORT", "PDF_ENGINE_API_BEARER_TOKEN", "PDF_BUCKET",
		"STORAGE_HOST", "STORAGE_PORT", "STORAGE_ACCESS_KEY", "STORAGE_SECRET_KEY", "STORAGE_REGION"}
	for _, k := range keys {
		if v, ok := os.LookupEnv(k); ok {
			env[k] = v
		}
		if env[k] == "" {
			return fmt.Errorf("set %s in .env or the environment", k)
		}
	}
	engineHost, enginePort, token, bucket = env["PDF_ENGINE_API_HOST"], env["PDF_ENGINE_API_PORT"], env["PDF_ENGINE_API_BEARER_TOKEN"], env["PDF_BUCKET"]
	if _, err := exec.LookPath("docker"); err != nil {
		return errors.New("docker not installed")
	}
	up := exec.Command("docker", "compose", "up", "-d", "--wait", "pdf-engine")
	up.Dir = root
	if out, err := up.CombinedOutput(); err != nil {
		return fmt.Errorf("docker compose up pdf-engine: %w\n%s", err, out)
	}
	store, err = minio.New(net.JoinHostPort(env["STORAGE_HOST"], env["STORAGE_PORT"]), &minio.Options{
		Creds: credentials.NewStaticV4(env["STORAGE_ACCESS_KEY"], env["STORAGE_SECRET_KEY"], ""), Region: env["STORAGE_REGION"],
	})
	return err
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

func requireEngine(t testing.TB) {
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

func engineURL(h, p string) string { return "http://" + net.JoinHostPort(h, p) }

// started is a started service, stopped when t ends.
func started(t testing.TB) pdf.PDFServices {
	t.Helper()
	requireEngine(t)
	s := pdf.NewPDFEngine(engineURL(engineHost, enginePort), token, nil)
	if err := s.Start(ctxT(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = s.Stop(context.Background()) })
	return s
}

// invoice is a realistic document.
func invoice() pdf.RenderOptions {
	return pdf.RenderOptions{
		Template: `<html><body><h1>Invoice {{ no }}</h1>
<table>{% for line in lines %}<tr><td>{{ line.item }}</td><td>{{ line.amount }}</td></tr>{% endfor %}</table>
<p>Total: {{ total }}</p></body></html>`,
		Data: map[string]any{"no": 42, "total": "1,250.00", "lines": []map[string]any{
			{"item": "Loan processing fee", "amount": "1,000.00"}, {"item": "Notarial fee", "amount": "250.00"},
		}},
		Filename: "invoice-42.pdf",
	}
}

// render generates opts and returns the PDF bytes, checking they are one.
func render(t testing.TB, s pdf.PDFServices, opts pdf.RenderOptions) []byte {
	t.Helper()
	body, err := s.Generate(ctxT(t), opts)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	defer body.Close()
	b, err := io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(b, []byte("%PDF-")) {
		t.Fatalf("not a PDF: %.40q", b)
	}
	return b
}

var mediaBox = regexp.MustCompile(`/MediaBox\s*\[\s*([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s*\]`)

// pageSize is the first page's size in points.
func pageSize(t testing.TB, doc []byte) (w, h float64) {
	t.Helper()
	m := mediaBox.FindSubmatch(doc)
	if m == nil {
		t.Fatal("no /MediaBox in the PDF")
	}
	x0, _ := strconv.ParseFloat(string(m[1]), 64)
	y0, _ := strconv.ParseFloat(string(m[2]), 64)
	x1, _ := strconv.ParseFloat(string(m[3]), 64)
	y1, _ := strconv.ParseFloat(string(m[4]), 64)
	return x1 - x0, y1 - y0
}

func stored(t testing.TB, key string) int64 {
	t.Helper()
	info, err := store.StatObject(ctxT(t), bucket, key, minio.StatObjectOptions{})
	if err != nil {
		t.Fatalf("%s is not in %s: %v", key, bucket, err)
	}
	t.Cleanup(func() { _ = store.RemoveObject(context.Background(), bucket, key, minio.RemoveObjectOptions{}) })
	return info.Size
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

// proxy relays TCP to pdf-engine and can delay or cut traffic.
type proxy struct {
	ln    net.Listener
	delay atomic.Int64
	down  atomic.Bool
	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

func newProxy(t testing.TB) *proxy {
	t.Helper()
	requireEngine(t)
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
			if p.down.Load() {
				_ = c.Close()
				continue
			}
			s, err := net.Dial("tcp", net.JoinHostPort(engineHost, enginePort))
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

func (p *proxy) URL() string {
	h, pt, _ := net.SplitHostPort(p.ln.Addr().String())
	return engineURL(h, pt)
}

func (p *proxy) SetDelay(d time.Duration) { p.delay.Store(int64(d)) }
func (p *proxy) Down()                    { p.down.Store(true); p.DropAll() }
func (p *proxy) Up()                      { p.down.Store(false) }

func (p *proxy) DropAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for c := range p.conns {
		_ = c.Close()
	}
	clear(p.conns)
}
