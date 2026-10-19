//go:build integration

package otp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	twilioclient "github.com/twilio/twilio-go/client"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/otp"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/queue"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/security"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sms"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/smtp"
	"github.com/Lands-Horizon-Corp/ecoop-server/tests/internal/qns"
)

// Tests use the valkey and mailpit services in docker-compose.yml, with
// CACHE_*, SMTP_*, MAILPIT_UI_PORT from the environment or the repo's .env.
// SMS goes to a fake Twilio server. Every test uses its own OTP keys and
// inboxes and removes them when it ends.
var (
	valkeyAddr, valkeyPassword string
	smtpHost, mailpitAPI       string
	smtpPort                   int
	skipReason                 string
	cleaner                    cache.ICacheService // one connection for all test cleanup
)

const secret = "otp-test-pepper"

var _ otp.IOTPService = (*otp.OTPService)(nil)

func TestMain(m *testing.M) { os.Exit(runMain(m)) }

func runMain(m *testing.M) int {
	if err := setup(); err != nil {
		skipReason = "no Valkey/Mailpit: " + err.Error()
		fmt.Fprintln(os.Stderr, "otp tests:", skipReason)
		return m.Run()
	}
	cleaner = cache.NewCacheService([]string{valkeyAddr}, valkeyPassword, nil)
	if err := cleaner.Run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "otp tests: cleanup connection:", err)
		return 1
	}
	defer cleaner.Stop(context.Background())
	return m.Run()
}

func setup() error {
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
	for _, k := range []string{"CACHE_HOST", "CACHE_PORT", "CACHE_PASSWORD", "SMTP_HOST", "SMTP_PORT", "MAILPIT_UI_PORT"} {
		if v, ok := os.LookupEnv(k); ok {
			env[k] = v
		}
		if env[k] == "" {
			return fmt.Errorf("set %s in .env or the environment", k)
		}
	}
	valkeyAddr, valkeyPassword = net.JoinHostPort(env["CACHE_HOST"], env["CACHE_PORT"]), env["CACHE_PASSWORD"]
	smtpHost = env["SMTP_HOST"]
	smtpPort, _ = strconv.Atoi(env["SMTP_PORT"])
	mailpitAPI = "http://" + net.JoinHostPort(env["SMTP_HOST"], env["MAILPIT_UI_PORT"]) + "/api/v1"
	if _, err := exec.LookPath("docker"); err != nil {
		return errors.New("docker not installed")
	}
	up := exec.Command("docker", "compose", "up", "-d", "--wait", "valkey", "mailpit")
	up.Dir = root
	if out, err := up.CombinedOutput(); err != nil {
		return fmt.Errorf("docker compose up: %w\n%s", err, out)
	}
	return nil
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

func requireInfra(t testing.TB) {
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

// Building blocks.

func newCache(t testing.TB, addr ...string) cache.ICacheService {
	t.Helper()
	requireInfra(t)
	target := valkeyAddr
	if len(addr) > 0 {
		target = addr[0]
	}
	ch := cache.NewCacheService([]string{target}, valkeyPassword, nil)
	if err := ch.Run(context.Background()); err != nil {
		t.Fatalf("cache Run: %v", err)
	}
	t.Cleanup(func() { _ = ch.Stop(context.Background()) })
	return ch
}

func newSecurity() security.ISecurityService {
	return security.NewSecurityService("otp-test", 8*1024, 1, 2, 16, 32)
}

func newQueue(t testing.TB, ch cache.ICacheService) queue.IQueueService {
	t.Helper()
	q := queue.NewQueueService(ch, queue.WithNamespace(qns.For(t, valkeyAddr, valkeyPassword)))
	if err := q.Start(context.Background()); err != nil {
		t.Fatalf("queue Start: %v", err)
	}
	t.Cleanup(func() { _ = q.Stop(context.Background()) })
	return q
}

func mockSMTP(t testing.TB) smtp.ISMTPService {
	t.Helper()
	s := smtp.NewSMTPService("", 0, "", "", "noreply@ecoop.test", "eCoop", false, nil, nil)
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s
}

func mockSMS(t testing.TB) sms.ISMSServices {
	t.Helper()
	s := sms.NewSMSService("", "", "+15005550006", 160, false, nil, nil)
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s
}

// realSMTP delivers to Mailpit through the queue.
func realSMTP(t testing.TB, q queue.IQueueService) smtp.ISMTPService {
	t.Helper()
	s := smtp.NewSMTPService(smtpHost, smtpPort, "", "", "noreply@ecoop.test", "eCoop", true, q, nil)
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("smtp Start: %v", err)
	}
	t.Cleanup(func() { _ = s.Stop(context.Background()) })
	return s
}

// fakeTwilio records message bodies sent to it, by recipient.
type fakeTwilio struct {
	mu     sync.Mutex
	bodies map[string][]string
}

func (f *fakeTwilio) last(to string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b := f.bodies[to]
	if len(b) == 0 {
		return "", false
	}
	return b[len(b)-1], true
}

type redirect struct{ target *url.URL }

func (r redirect) RoundTrip(req *http.Request) (*http.Response, error) {
	req.URL.Scheme, req.URL.Host = r.target.Scheme, r.target.Host
	return http.DefaultTransport.RoundTrip(req)
}

// realSMS sends through the queue to a fake Twilio server.
func realSMS(t testing.TB, q queue.IQueueService) (sms.ISMSServices, *fakeTwilio) {
	t.Helper()
	fake := &fakeTwilio{bodies: map[string][]string{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			b, _ := io.ReadAll(r.Body)
			form, _ := url.ParseQuery(string(b))
			fake.mu.Lock()
			fake.bodies[form.Get("To")] = append(fake.bodies[form.Get("To")], form.Get("Body"))
			fake.mu.Unlock()
			w.WriteHeader(http.StatusCreated)
		}
		_, _ = w.Write([]byte(`{"sid":"X"}`))
	}))
	t.Cleanup(srv.Close)
	target, _ := url.Parse(srv.URL)
	s := sms.NewSMSService("AC00000000000000000000000000000000", "token", "+15005550006", 160, true, q, nil)
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("sms Start: %v", err)
	}
	s.Client().Client.(*twilioclient.Client).HTTPClient.Transport = redirect{target}
	t.Cleanup(func() { _ = s.Stop(context.Background()) })
	return s, fake
}

// newOTP returns a started OTP service with mock email/SMS on its own cache.
func newOTP(t testing.TB) otp.IOTPService {
	t.Helper()
	return startOTP(t, secret, newCache(t), mockSMTP(t), mockSMS(t))
}

func startOTP(t testing.TB, pepper string, ch cache.ICacheService, mail smtp.ISMTPService, text sms.ISMSServices) otp.IOTPService {
	t.Helper()
	o := otp.NewOTPService(pepper, newSecurity(), ch, mail, text, nil)
	if err := o.Start(context.Background()); err != nil {
		t.Fatalf("otp Start: %v", err)
	}
	t.Cleanup(func() { _ = o.Stop(context.Background()) })
	return o
}

var seq atomic.Int64
var runID = time.Now().UnixNano()

// testKeys collects each test's OTP keys so one cleanup per test deletes
// them all over the shared connection.
var testKeys sync.Map // testing.TB -> *keyList

type keyList struct {
	mu   sync.Mutex
	keys []string
}

// key returns an OTP key unique to this test and run; its cache entries are
// deleted when the test ends.
func key(t testing.TB) string {
	t.Helper()
	k := fmt.Sprintf("test:%d:%d", runID, seq.Add(1))
	v, loaded := testKeys.LoadOrStore(t, &keyList{})
	list := v.(*keyList)
	if !loaded {
		t.Cleanup(func() {
			testKeys.Delete(t)
			list.mu.Lock()
			defer list.mu.Unlock()
			deleteKeys(list.keys)
		})
	}
	list.mu.Lock()
	list.keys = append(list.keys, k)
	list.mu.Unlock()
	return k
}

// deleteKeys removes the code and guess counter of each key, in batches.
func deleteKeys(keys []string) {
	client := cleaner.Client()
	if client == nil || len(keys) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cache := make([]string, 0, 2*len(keys))
	for _, k := range keys {
		cache = append(cache, otp.KeyPrefix+k, otp.AttemptsPrefix+k)
	}
	for len(cache) > 0 {
		n := min(len(cache), 1000)
		_ = client.Do(ctx, client.B().Del().Key(cache[:n]...).Build()).Error()
		cache = cache[n:]
	}
}

// otherCode returns a code guaranteed not to equal code.
func otherCode(code string) string {
	if code == "000000" {
		return "111111"
	}
	return "000000"
}

// Mailpit.

func inbox(t testing.TB) string {
	t.Helper()
	addr := fmt.Sprintf("otp%d-%d@ecoop.test", runID, seq.Add(1))
	t.Cleanup(func() {
		var ids []string
		ids = append(ids, mailIDs(addr)...)
		if len(ids) > 0 {
			body, _ := json.Marshal(map[string][]string{"IDs": ids})
			req, _ := http.NewRequest(http.MethodDelete, mailpitAPI+"/messages", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			if resp, err := http.DefaultClient.Do(req); err == nil {
				resp.Body.Close()
			}
		}
	})
	return addr
}

func mailIDs(to string) []string {
	var res struct{ Messages []struct{ ID string } }
	resp, err := http.Get(mailpitAPI + "/search?limit=100&query=" + url.QueryEscape(fmt.Sprintf("to:%q", to)))
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	_ = json.NewDecoder(resp.Body).Decode(&res)
	ids := make([]string, len(res.Messages))
	for i, m := range res.Messages {
		ids[i] = m.ID
	}
	return ids
}

// emailHTML waits for the email sent to `to` and returns its HTML.
func emailHTML(t testing.TB, to string) string {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if ids := mailIDs(to); len(ids) > 0 {
			var m struct{ HTML string }
			resp, err := http.Get(mailpitAPI + "/message/" + ids[0])
			if err == nil {
				_ = json.NewDecoder(resp.Body).Decode(&m)
				resp.Body.Close()
				return m.HTML
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("no email reached %s", to)
	return ""
}

// Misc.

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

// proxy relays TCP to Valkey and can cut every connection.
type proxy struct {
	ln    net.Listener
	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

func newProxy(t testing.TB) *proxy {
	t.Helper()
	requireInfra(t)
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
			s, err := net.Dial("tcp", valkeyAddr)
			if err != nil {
				_ = c.Close()
				continue
			}
			p.mu.Lock()
			p.conns[c], p.conns[s] = struct{}{}, struct{}{}
			p.mu.Unlock()
			go func() { _, _ = io.Copy(s, c); _ = s.Close(); _ = c.Close() }()
			go func() { _, _ = io.Copy(c, s); _ = c.Close(); _ = s.Close() }()
		}
	}()
	t.Cleanup(func() { _ = ln.Close(); p.DropAll() })
	return p
}

func (p *proxy) Addr() string { return p.ln.Addr().String() }

func (p *proxy) DropAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for c := range p.conns {
		_ = c.Close()
	}
	clear(p.conns)
}
