//go:build integration

package smtp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
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

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/queue"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/smtp"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
	"github.com/Lands-Horizon-Corp/ecoop-server/tests/internal/qns"
)

// Delivery tests run against the mailpit and valkey services in
// docker-compose.yml, using SMTP_*, MAILPIT_UI_PORT and CACHE_* from the
// environment or the repo's .env. TestMain starts them if needed. Every test
// sends to its own inbox address and uses its own queue, and deletes both
// when it finishes.
var (
	smtpHost      string
	smtpPort      int
	mailpitAPI    string
	sender        string
	senderName    string
	queueAddr     string
	queuePassword string
	skipReason    string
)

var _ smtp.ISMTPService = (*smtp.SMTPService)(nil)

func TestMain(m *testing.M) {
	os.Exit(runMain(m))
}

func runMain(m *testing.M) int {
	if err := startMailpit(); err != nil {
		skipReason = "no Mailpit: " + err.Error()
		fmt.Fprintln(os.Stderr, "smtp tests:", skipReason)
	}
	return m.Run()
}

func startMailpit() error {
	root, err := moduleRoot()
	if err != nil {
		return err
	}
	required := []string{"SMTP_HOST", "SMTP_PORT", "MAILPIT_UI_PORT", "SMTP_FROM", "CACHE_HOST", "CACHE_PORT", "CACHE_PASSWORD"}
	env, err := loadEnv(filepath.Join(root, ".env"), append(required, "SMTP_FROM_NAME")...)
	if err != nil {
		return err
	}
	var missing []string
	for _, k := range required {
		if env[k] == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("set %s in .env or the environment", strings.Join(missing, ", "))
	}
	smtpHost = env["SMTP_HOST"]
	if smtpPort, err = strconv.Atoi(env["SMTP_PORT"]); err != nil {
		return fmt.Errorf("SMTP_PORT: %w", err)
	}
	mailpitAPI = "http://" + net.JoinHostPort(smtpHost, env["MAILPIT_UI_PORT"]) + "/api/v1"
	sender, senderName = env["SMTP_FROM"], env["SMTP_FROM_NAME"]
	queueAddr, queuePassword = net.JoinHostPort(env["CACHE_HOST"], env["CACHE_PORT"]), env["CACHE_PASSWORD"]

	if _, err := exec.LookPath("docker"); err != nil {
		return errors.New("docker not installed")
	}
	up := exec.Command("docker", "compose", "up", "-d", "--wait", "mailpit", "valkey")
	up.Dir = root
	if out, err := up.CombinedOutput(); err != nil {
		return fmt.Errorf("docker compose up mailpit valkey: %w\n%s", err, out)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		resp, err := http.Get(mailpitAPI + "/info")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("mailpit API not ready at %s: %w", mailpitAPI, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// loadEnv reads KEY=VALUE lines from path; real environment variables win.
func loadEnv(path string, keys ...string) (map[string]string, error) {
	env := map[string]string{}
	data, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(strings.TrimPrefix(line, "export "), "=")
		if ok {
			env[strings.TrimSpace(k)] = strings.Trim(strings.TrimSpace(v), `"'`)
		}
	}
	for _, k := range keys {
		if v, ok := os.LookupEnv(k); ok {
			env[k] = v
		}
	}
	return env, nil
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

func requireMailpit(t testing.TB) {
	t.Helper()
	if skipReason != "" {
		t.Skip(skipReason)
	}
}

// serviceOpts tweaks a test service; zero values mean "use the default".
type serviceOpts struct {
	smtpAddr string          // deliver here instead of Mailpit (proxy, fake server)
	fromName string          // default display name
	noName   bool            // no default display name at all
	inflight *utils.Inflight // shared tracker
}

// newSMTP builds (but does not start) a service with its own cache
// connection. All services share the one email queue, as instances behind a
// load balancer do; it is cleared when the test ends.
func newSMTP(t testing.TB, opts serviceOpts) smtp.ISMTPService {
	t.Helper()
	requireMailpit(t)
	host, port := smtpHost, smtpPort
	if opts.smtpAddr != "" {
		h, p, _ := net.SplitHostPort(opts.smtpAddr)
		host = h
		port, _ = strconv.Atoi(p)
	}
	name := senderName
	if opts.fromName != "" {
		name = opts.fromName
	}
	if opts.noName {
		name = ""
	}
	q := newQueue(t)
	return smtp.NewSMTPService(host, port, "", "", sender, name, true, q, opts.inflight)
}

// newService returns a started service that delivers to Mailpit (or to
// addr if given, e.g. a proxy or a fake server).
func newService(t testing.TB, addr ...string) smtp.ISMTPService {
	t.Helper()
	var opts serviceOpts
	if len(addr) > 0 {
		opts.smtpAddr = addr[0]
	}
	return startService(t, opts)
}

func startService(t testing.TB, opts serviceOpts) smtp.ISMTPService {
	t.Helper()
	svc := newSMTP(t, opts)
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = svc.Stop(context.Background()) })
	return svc
}

// mockService runs with secured=false: validates and formats, sends nothing.
func mockService(t testing.TB) smtp.ISMTPService {
	t.Helper()
	svc := smtp.NewSMTPService("", 0, "", "", "noreply@ecoop.test", "eCoop", false, nil, nil)
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return svc
}

// newQueue returns a started queue on a running cache, both stopped (queue
// first) when the test ends.
func newQueue(t testing.TB) queue.IQueueService {
	t.Helper()
	ch := cache.NewCacheService([]string{queueAddr}, queuePassword, nil)
	if err := ch.Run(context.Background()); err != nil {
		t.Fatalf("cache Run: %v", err)
	}
	t.Cleanup(func() { _ = ch.Stop(context.Background()) })
	q := queue.NewQueueService(ch, queue.WithNamespace(qns.For(t, queueAddr, queuePassword)))
	if err := q.Start(context.Background()); err != nil {
		t.Fatalf("queue Start: %v", err)
	}
	t.Cleanup(func() { _ = q.Stop(context.Background()) })
	return q
}

// failedDeliveries is how many delivery attempts have failed today on the
// email queue. Tests compare it with a count taken before they act.
func failedDeliveries(t testing.TB) int {
	t.Helper()
	in := qns.Inspector(queueAddr, queuePassword)
	defer in.Close()
	info, err := in.GetQueueInfo(queue.QueueName(qns.For(t, queueAddr, queuePassword), smtp.QueueName))
	if err != nil {
		return 0 // queue not created yet
	}
	return info.Failed
}

// minRateLimitedSpan is the least time n emails can take at SendRateLimit
// per RateWindow. The first can land at the very end of a window and the
// last at the very start of one, so two windows are only partly used.
func minRateLimitedSpan(n int) time.Duration {
	return time.Duration(max(n/smtp.SendRateLimit-2, 0)) * smtp.RateWindow
}

func eventually(t testing.TB, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("not met within %v: %s", timeout, msg)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func ctxT(t testing.TB) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

var inboxSeq atomic.Int64

// inbox returns an address unique to this test and run, and deletes every
// message sent to it when the test ends.
func inbox(t testing.TB) string {
	t.Helper()
	addr := fmt.Sprintf("t%d-%d@ecoop.test", time.Now().UnixNano(), inboxSeq.Add(1))
	t.Cleanup(func() {
		var ids []string
		for _, m := range search(t, addr) {
			ids = append(ids, m.ID)
		}
		if len(ids) > 0 {
			body, _ := json.Marshal(map[string][]string{"IDs": ids}) // never empty: that deletes everything
			req, _ := http.NewRequest(http.MethodDelete, mailpitAPI+"/messages", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			if resp, err := http.DefaultClient.Do(req); err == nil {
				resp.Body.Close()
			}
		}
	})
	return addr
}

type address struct {
	Name    string `json:"Name"`
	Address string `json:"Address"`
}

type summary struct {
	ID      string    `json:"ID"`
	From    address   `json:"From"`
	To      []address `json:"To"`
	Bcc     []address `json:"Bcc"`
	Cc      []address `json:"Cc"`
	Subject string    `json:"Subject"`
}

type message struct {
	summary
	MessageID string `json:"MessageID"`
	Date      string `json:"Date"`
	HTML      string `json:"HTML"`
	Text      string `json:"Text"`
}

func getJSON(t testing.TB, path string, out any) {
	t.Helper()
	resp, err := http.Get(mailpitAPI + path)
	if err != nil {
		t.Fatalf("mailpit GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("mailpit GET %s: %s %s", path, resp.Status, b)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		t.Fatalf("mailpit GET %s: decoding: %v", path, err)
	}
}

func search(t testing.TB, to string) []summary {
	t.Helper()
	var res struct {
		Messages []summary `json:"messages"`
	}
	getJSON(t, "/search?limit=1000&query="+url.QueryEscape(fmt.Sprintf("to:%q", to)), &res)
	return res.Messages
}

// received waits until n messages addressed to `to` have arrived. Delivery
// is queued and rate limited, so allow for SendRateLimit per RateWindow.
func received(t testing.TB, to string, n int) []message {
	t.Helper()
	deadline := time.Now().Add(30*time.Second + time.Duration(n/smtp.SendRateLimit+1)*smtp.RateWindow)
	var got []summary
	for time.Now().Before(deadline) {
		if got = search(t, to); len(got) >= n {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(got) < n {
		t.Fatalf("%s: got %d messages, want %d", to, len(got), n)
	}
	out := make([]message, len(got))
	for i, s := range got {
		getJSON(t, "/message/"+s.ID, &out[i])
	}
	return out
}

func receivedOne(t testing.TB, to string) message {
	t.Helper()
	return received(t, to, 1)[0]
}

// noneReceived asserts nothing arrives for `to` within a short window.
func noneReceived(t testing.TB, to string) {
	t.Helper()
	time.Sleep(300 * time.Millisecond)
	if got := search(t, to); len(got) != 0 {
		t.Fatalf("%s: %d messages arrived, want none", to, len(got))
	}
}

func rawHeaders(t testing.TB, id string) map[string][]string {
	t.Helper()
	var h map[string][]string
	getJSON(t, "/message/"+id+"/headers", &h)
	return h
}

// XSS checks.

var xssPayloads = map[string]string{
	"script tag":            `<script>alert(1)</script>`,
	"script uppercase":      `<SCRIPT>alert(1)</SCRIPT>`,
	"img onerror":           `<img src=x onerror=alert(1)>`,
	"svg onload":            `<svg onload=alert(1)>`,
	"body onload":           `<body onload=alert(1)>hi</body>`,
	"inline handler":        `<p onclick="alert(1)">x</p>`,
	"javascript href":       `<a href="javascript:alert(1)">x</a>`,
	"iframe":                `<iframe src="javascript:alert(1)"></iframe>`,
	"object":                `<object data="javascript:alert(1)"></object>`,
	"embed":                 `<embed src="javascript:alert(1)">`,
	"meta refresh":          `<meta http-equiv="refresh" content="0;url=javascript:alert(1)">`,
	"form action":           `<form action="javascript:alert(1)"><button>x</button></form>`,
	"nested script":         `<scr<script>ipt>alert(1)</script>`,
	"attribute breakout":    `"><script>alert(1)</script>`,
	"mutation xss":          `<math><mtext><table><mglyph><style><img src=x onerror=alert(1)>`,
	"style block js":        `<style>body{background:url("javascript:alert(1)")}</style>`,
	"data uri":              `<a href="data:text/html;base64,PHNjcmlwdD5hbGVydCgxKTwvc2NyaXB0Pg==">x</a>`,
	"entity encoded js":     `<a href="&#106;avascript:alert(1)">x</a>`,
	"base tag":              `<base href="javascript:alert(1)//">`,
	"link stylesheet":       `<link rel="stylesheet" href="javascript:alert(1)">`,
	"srcdoc":                `<iframe srcdoc="<script>alert(1)</script>"></iframe>`,
	"template injection js": `{{"<script>alert(1)</script>"}}`,
}

// Payloads that only CSS can carry; kept separate so a gap in style
// filtering is reported on its own.
var cssXSSPayloads = map[string]string{
	"style url javascript": `<p style="background:url(javascript:alert(1))">x</p>`,
	"style expression":     `<div style="width:expression(alert(1))">x</div>`,
	"style behavior":       `<div style="behavior:url(xss.htc)">x</div>`,
}

var handlerAttr = regexp.MustCompile(`(?i)<[^>]*\son[a-z]+\s*=`)

// assertNoXSS fails if html still carries anything that can run script.
func assertNoXSS(t testing.TB, html string) {
	t.Helper()
	low := strings.ToLower(html)
	for _, bad := range []string{"<script", "javascript:", "vbscript:", "data:text/html", "expression(", "<iframe", "<object", "<embed", "<svg", "<meta", "<base", "<link", "<form", "srcdoc", "behavior:"} {
		if strings.Contains(low, bad) {
			t.Errorf("sanitized HTML still contains %q:\n%s", bad, html)
		}
	}
	if handlerAttr.MatchString(html) {
		t.Errorf("sanitized HTML still has an event-handler attribute:\n%s", html)
	}
}

// Fault injection.

// proxy relays TCP to Mailpit and can cut connections, delay replies, or
// refuse connections to simulate an outage.
type proxy struct {
	ln     net.Listener
	target string
	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	delay  atomic.Int64
	down   atomic.Bool
}

func newProxy(t testing.TB) *proxy {
	t.Helper()
	requireMailpit(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &proxy{ln: ln, target: net.JoinHostPort(smtpHost, strconv.Itoa(smtpPort)), conns: map[net.Conn]struct{}{}}
	go p.serve()
	t.Cleanup(func() { _ = ln.Close(); p.DropAll() })
	return p
}

func (p *proxy) Addr() string { return p.ln.Addr().String() }

func (p *proxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		if p.down.Load() {
			_ = c.Close()
			continue
		}
		s, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = c.Close()
			continue
		}
		p.mu.Lock()
		p.conns[c], p.conns[s] = struct{}{}, struct{}{}
		p.mu.Unlock()
		go p.pipe(c, s, true)
		go p.pipe(s, c, false)
	}
}

func (p *proxy) pipe(dst, src net.Conn, toClient bool) {
	defer func() { _ = dst.Close(); _ = src.Close() }()
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if d := time.Duration(p.delay.Load()); toClient && d > 0 {
				time.Sleep(d)
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *proxy) DropAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for c := range p.conns {
		_ = c.Close()
	}
	clear(p.conns)
}

// SetDelay holds every server reply for d, keeping sends in flight.
func (p *proxy) SetDelay(d time.Duration) { p.delay.Store(int64(d)) }

// SetDown makes the server unreachable (true) or reachable again (false).
func (p *proxy) SetDown(down bool) {
	p.down.Store(down)
	if down {
		p.DropAll()
	}
}

// fakeServer accepts connections and hands each to handle.
func fakeServer(t testing.TB, handle func(net.Conn)) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				handle(c)
			}()
		}
	}()
	return ln.Addr().String()
}

// stalledServer accepts connections and never says anything.
func stalledServer(t testing.TB) string {
	return fakeServer(t, func(c net.Conn) { _, _ = io.Copy(io.Discard, c) })
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
