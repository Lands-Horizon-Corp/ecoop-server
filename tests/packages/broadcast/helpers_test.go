//go:build integration

package broadcast_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
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

	"github.com/coder/websocket"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/broadcast"
)

// Tests run against the sockudo service in docker-compose.yml, configured
// from BROADCAST_* in the environment or the repo's .env. Every test uses
// its own channel names, so tests run in parallel without hearing each
// other.
var (
	host, port, appID, appKey, appSecret string
	maxPayloadKB                         int
	skipReason                           string
	runID                                = time.Now().UnixNano()
	channelSeq                           atomic.Int64
)

var _ broadcast.IBroadcasterService = (*broadcast.BroadcasterService)(nil)

func TestMain(m *testing.M) {
	if err := startSockudo(); err != nil {
		skipReason = "no Sockudo: " + err.Error()
		fmt.Fprintln(os.Stderr, "broadcast tests:", skipReason)
	}
	os.Exit(m.Run())
}

func startSockudo() error {
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
	for _, k := range []string{"BROADCAST_HOST", "BROADCAST_PORT", "BROADCAST_APP_ID", "BROADCAST_APP_KEY", "BROADCAST_APP_SECRET", "BROADCAST_MAX_PAYLOAD_KB"} {
		if v, ok := os.LookupEnv(k); ok {
			env[k] = v
		}
		if env[k] == "" {
			return fmt.Errorf("set %s in .env or the environment", k)
		}
	}
	host, port = env["BROADCAST_HOST"], env["BROADCAST_PORT"]
	appID, appKey, appSecret = env["BROADCAST_APP_ID"], env["BROADCAST_APP_KEY"], env["BROADCAST_APP_SECRET"]
	if maxPayloadKB, err = strconv.Atoi(env["BROADCAST_MAX_PAYLOAD_KB"]); err != nil {
		return fmt.Errorf("BROADCAST_MAX_PAYLOAD_KB: %w", err)
	}
	if _, err := exec.LookPath("docker"); err != nil {
		return errors.New("docker not installed")
	}
	up := exec.Command("docker", "compose", "up", "-d", "--wait", "sockudo")
	up.Dir = root
	if out, err := up.CombinedOutput(); err != nil {
		return fmt.Errorf("docker compose up sockudo: %w\n%s", err, out)
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

// channel is a channel name unique to this test and run.
func channel(prefix ...string) string {
	name := fmt.Sprintf("t-%d-%d", runID, channelSeq.Add(1))
	if len(prefix) > 0 {
		name = prefix[0] + name
	}
	return name
}

// service builds (not started) a service reaching Sockudo at h:p.
func service(h, p string, payloadKB ...int) broadcast.IBroadcasterService {
	kb := maxPayloadKB
	if len(payloadKB) > 0 {
		kb = payloadKB[0]
	}
	return broadcast.NewBroadcastService(appID, appKey, appSecret, h, p, false, kb, nil)
}

// started is a started service, stopped when t ends.
func started(t testing.TB, payloadKB ...int) broadcast.IBroadcasterService {
	t.Helper()
	requireServer(t)
	b := service(host, port, payloadKB...)
	if err := b.Start(ctxT(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop(context.Background()) })
	return b
}

// message is an event as a browser receives it.
type message struct {
	Event   string `json:"event"`
	Channel string `json:"channel"`
	Data    string `json:"data"` // the payload's JSON, as a string
}

// subscriber is a browser: a Pusher-protocol websocket subscribed to
// channels.
type subscriber struct {
	conn     *websocket.Conn
	socketID string
	msgs     chan message
}

func connect(t testing.TB) *subscriber {
	t.Helper()
	requireServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	url := fmt.Sprintf("ws://%s/app/%s?protocol=7&client=go-test&version=1.0", net.JoinHostPort(host, port), appKey)
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("websocket: %v", err)
	}
	conn.SetReadLimit(64 << 20)
	s := &subscriber{conn: conn, msgs: make(chan message, 4096)}
	var hello message
	if err := s.read(ctx, &hello); err != nil || hello.Event != "pusher:connection_established" {
		t.Fatalf("handshake: %+v, %v", hello, err)
	}
	var est struct {
		SocketID string `json:"socket_id"`
	}
	_ = json.Unmarshal([]byte(hello.Data), &est)
	s.socketID = est.SocketID
	go func() {
		defer close(s.msgs)
		for {
			var m message
			if err := s.read(context.Background(), &m); err != nil {
				return
			}
			if m.Event == "pusher:ping" {
				_ = s.send(context.Background(), map[string]any{"event": "pusher:pong", "data": map[string]any{}})
				continue
			}
			s.msgs <- m
		}
	}()
	t.Cleanup(func() { _ = conn.Close(websocket.StatusNormalClosure, "") })
	return s
}

func (s *subscriber) read(ctx context.Context, m *message) error {
	_, raw, err := s.conn.Read(ctx)
	if err != nil {
		return err
	}
	var generic struct {
		Event   string          `json:"event"`
		Channel string          `json:"channel"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &generic); err != nil {
		return err
	}
	m.Event, m.Channel = generic.Event, generic.Channel
	if err := json.Unmarshal(generic.Data, &m.Data); err != nil { // usually a JSON string
		m.Data = string(generic.Data)
	}
	return nil
}

func (s *subscriber) send(ctx context.Context, v any) error {
	raw, _ := json.Marshal(v)
	return s.conn.Write(ctx, websocket.MessageText, raw)
}

// subscribe joins channels (private-* ones with a signature from the app
// secret) and waits until the server confirms each.
func (s *subscriber) subscribe(t testing.TB, channels ...string) {
	t.Helper()
	ctx := ctxT(t)
	for _, c := range channels {
		data := map[string]any{"channel": c}
		if strings.HasPrefix(c, "private-") {
			data["auth"] = sign(s.socketID, c, appSecret)
		}
		if err := s.send(ctx, map[string]any{"event": "pusher:subscribe", "data": data}); err != nil {
			t.Fatal(err)
		}
	}
	pending := map[string]bool{}
	for _, c := range channels {
		pending[c] = true
	}
	for len(pending) > 0 {
		m, ok := s.next(10 * time.Second)
		if !ok {
			t.Fatalf("subscriptions never confirmed: %v", pending)
		}
		if m.Event == "pusher_internal:subscription_succeeded" {
			delete(pending, m.Channel)
		} else if m.Event == "pusher:subscription_error" || m.Event == "pusher:error" {
			t.Fatalf("subscribing: %+v", m)
		}
	}
}

// sign is a private-channel signature, as the app's auth endpoint makes.
func sign(socketID, channel, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(socketID + ":" + channel))
	return appKey + ":" + hex.EncodeToString(mac.Sum(nil))
}

// next is the next message, or false after timeout.
func (s *subscriber) next(timeout time.Duration) (message, bool) {
	select {
	case m, ok := <-s.msgs:
		return m, ok
	case <-time.After(timeout):
		return message{}, false
	}
}

// expect waits for n events (skipping protocol messages) and returns them.
func (s *subscriber) expect(t testing.TB, n int, timeout time.Duration) []message {
	t.Helper()
	var got []message
	deadline := time.Now().Add(timeout)
	for len(got) < n {
		m, ok := s.next(time.Until(deadline))
		if !ok {
			t.Fatalf("got %d of %d events: %+v", len(got), n, got)
		}
		if !strings.HasPrefix(m.Event, "pusher") {
			got = append(got, m)
		}
	}
	return got
}

// quiet checks no event arrives for d.
func (s *subscriber) quiet(t testing.TB, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		m, ok := s.next(time.Until(deadline))
		if !ok {
			return
		}
		if !strings.HasPrefix(m.Event, "pusher") {
			t.Fatalf("unexpected event %+v", m)
		}
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

// proxy relays TCP to Sockudo and can delay or cut traffic.
type proxy struct {
	ln    net.Listener
	delay atomic.Int64
	down  atomic.Bool
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
			if p.down.Load() {
				_ = c.Close()
				continue
			}
			s, err := net.Dial("tcp", net.JoinHostPort(host, port))
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

// HostPort is where to point a service to go through the proxy.
func (p *proxy) HostPort() (string, string) {
	h, pt, _ := net.SplitHostPort(p.ln.Addr().String())
	return h, pt
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
