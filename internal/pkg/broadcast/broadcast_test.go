package broadcast

import (
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/logger"
)

type fakeServer struct {
	*httptest.Server
	failWith atomic.Int32
	failN    atomic.Int32
	calls    atomic.Int32
	delay    time.Duration

	mu          sync.Mutex
	events      []sentEvent
	traceparent string
}

type sentEvent struct {
	Name     string   `json:"name"`
	Channels []string `json:"channels"`
	Data     string   `json:"data"`
}

func newFakeServer(t *testing.T) *fakeServer {
	f := &fakeServer{}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !validSignature(r, body, "secret") {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, "bad signature")
			return
		}
		f.mu.Lock()
		f.traceparent = r.Header.Get("traceparent")
		f.mu.Unlock()
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/channels"):
			_, _ = io.WriteString(w, `{"channels":{}}`)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/events"):
			f.calls.Add(1)
			time.Sleep(f.delay)
			if f.failN.Load() > 0 {
				f.failN.Add(-1)
				w.WriteHeader(int(f.failWith.Load()))
				_, _ = io.WriteString(w, "fail")
				return
			}
			var e sentEvent
			_ = json.Unmarshal(body, &e)
			f.mu.Lock()
			f.events = append(f.events, e)
			f.mu.Unlock()
			_, _ = io.WriteString(w, `{}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(f.Close)
	return f
}

// validSignature checks a request the way a Pusher-protocol server does.
func validSignature(r *http.Request, body []byte, secret string) bool {
	q := r.URL.Query()
	got := q.Get("auth_signature")
	q.Del("auth_signature")
	if len(body) > 0 {
		sum := md5.Sum(body)
		if q.Get("body_md5") != hex.EncodeToString(sum[:]) {
			return false
		}
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(r.Method + "\n" + r.URL.Path + "\n" + q.Encode()))
	return hmac.Equal([]byte(got), []byte(hex.EncodeToString(mac.Sum(nil))))
}

func (f *fakeServer) sent() []sentEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]sentEvent(nil), f.events...)
}

func (f *fakeServer) fail(status, times int) {
	f.failWith.Store(int32(status))
	f.failN.Store(int32(times))
}

func startedLogger(t *testing.T) logger.LoggerService {
	t.Helper()
	log := logger.NewLoggerService("broadcast-test", "error", "json")
	if err := log.Start(context.Background()); err != nil {
		t.Fatalf("start logger: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_ = log.Stop(ctx) // no collector in tests
	})
	return log
}

func newRunning(t *testing.T, f *fakeServer, opts ...Option) BroadcasterServices {
	t.Helper()
	opts = append([]Option{WithRetries(2, time.Millisecond)}, opts...)
	b := NewBroadcastService("app", "key", "secret", strings.TrimPrefix(f.URL, "http://"), "", startedLogger(t), opts...)
	if err := b.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return b
}

func TestValidation(t *testing.T) {
	ctx := context.Background()
	b := NewBroadcastService("app", "key", "secret", "localhost", "1", startedLogger(t))
	if err := b.Publish(ctx, "c", "e", nil); !errors.Is(err, ErrNotRunning) {
		t.Errorf("Publish before Run: %v", err)
	}
	if err := b.Broadcast(context.Background(), []Channel{"c"}, Events{"e"}, nil); !errors.Is(err, ErrNotRunning) {
		t.Errorf("Broadcast before Run: %v", err)
	}
	if err := b.Stop(ctx); !errors.Is(err, ErrNotRunning) {
		t.Errorf("Stop before Run: %v", err)
	}
	for name, err := range map[string]error{
		"no channels":  b.Dispatch(ctx, nil, "e", nil),
		"bad channel":  b.Publish(ctx, "has space", "e", nil),
		"empty event":  b.Publish(ctx, "c", "", nil),
		"long event":   b.Publish(ctx, "c", strings.Repeat("e", 201), nil),
		"no events":    b.Broadcast(context.Background(), []Channel{"c"}, nil, nil),
		"missing cred": NewBroadcastService("", "k", "s", "h", "", startedLogger(t)).Run(ctx),
		"nil logger":   NewBroadcastService("a", "k", "s", "h", "", nil).Run(ctx),
		"bad workers":  NewBroadcastService("a", "k", "s", "h", "", startedLogger(t), WithWorkers(0)).Run(ctx),
	} {
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%s: got %v, want ErrInvalidArgument", name, err)
		}
	}
}

func TestRunRejectsUnreachableServer(t *testing.T) {
	b := NewBroadcastService("app", "key", "secret", "127.0.0.1", "1", startedLogger(t), WithTimeout(time.Second))
	if err := b.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "connect") {
		t.Fatalf("got %v, want connect error", err)
	}
}

func TestPublishAndDispatch(t *testing.T) {
	f := newFakeServer(t)
	b := newRunning(t, f)
	defer b.Stop(context.Background())
	ctx := context.Background()

	if err := b.Publish(ctx, "users", "viewed", map[string]string{"id": "1"}); err != nil {
		t.Fatal(err)
	}
	channels := make([]string, 250)
	for i := range channels {
		channels[i] = fmt.Sprintf("user-%d", i)
	}
	if err := b.Dispatch(ctx, channels, "notice", "hi"); err != nil {
		t.Fatal(err)
	}

	got := f.sent()
	if len(got) != 4 {
		t.Fatalf("got %d API calls, want 4 (1 publish + 3 batches)", len(got))
	}
	if got[0].Name != "viewed" || got[0].Channels[0] != "users" || got[0].Data != `{"id":"1"}` {
		t.Errorf("publish sent %+v", got[0])
	}
	if n := len(got[1].Channels) + len(got[2].Channels) + len(got[3].Channels); n != 250 || len(got[1].Channels) != 100 {
		t.Errorf("dispatch batches wrong: %d,%d,%d", len(got[1].Channels), len(got[2].Channels), len(got[3].Channels))
	}
}

func TestRetries(t *testing.T) {
	f := newFakeServer(t)
	b := newRunning(t, f)
	defer b.Stop(context.Background())
	ctx := context.Background()

	f.fail(http.StatusServiceUnavailable, 2) // 2 failures, then success: within 2 retries
	if err := b.Publish(ctx, "c", "e", "x"); err != nil {
		t.Errorf("5xx should be retried: %v", err)
	}

	f.fail(http.StatusServiceUnavailable, 5)
	if err := b.Publish(ctx, "c", "e", "x"); err == nil {
		t.Error("want error after retries are exhausted")
	}

	f.fail(http.StatusBadRequest, 1)
	before := f.calls.Load()
	if err := b.Publish(ctx, "c", "e", "x"); err == nil {
		t.Error("want error for 400")
	}
	if n := f.calls.Load() - before; n != 1 {
		t.Errorf("400 was attempted %d times, want 1 (no retry)", n)
	}
	f.fail(0, 0)
}

func TestBroadcastIsDeliveredAndStopDrains(t *testing.T) {
	f := newFakeServer(t)
	f.delay = 20 * time.Millisecond
	b := newRunning(t, f, WithWorkers(1))

	if err := b.Broadcast(context.Background(), []Channel{"a", "b"}, Events{"created", "updated", "deleted"}, "p"); err != nil {
		t.Fatal(err)
	}
	if err := b.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if got := f.sent(); len(got) != 3 {
		t.Fatalf("delivered %d events before Stop returned, want 3", len(got))
	}
	if err := b.Broadcast(context.Background(), []Channel{"a"}, Events{"e"}, nil); !errors.Is(err, ErrNotRunning) {
		t.Errorf("Broadcast after Stop: %v", err)
	}
}

func TestBroadcastQueueFull(t *testing.T) {
	f := newFakeServer(t)
	f.delay = 200 * time.Millisecond
	b := newRunning(t, f, WithWorkers(1), WithQueueSize(1))
	defer b.Stop(context.Background())

	var err error
	for i := 0; i < 5 && err == nil; i++ {
		err = b.Broadcast(context.Background(), []Channel{"c"}, Events{"e"}, i)
	}
	if !errors.Is(err, ErrQueueFull) {
		t.Errorf("got %v, want ErrQueueFull", err)
	}
}

func TestStopTimeoutReportsDropped(t *testing.T) {
	f := newFakeServer(t)
	f.delay = 100 * time.Millisecond
	b := newRunning(t, f, WithWorkers(1))

	if err := b.Broadcast(context.Background(), []Channel{"c"}, Events{"1", "2", "3", "4", "5"}, nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := b.Stop(ctx); err == nil || !strings.Contains(err.Error(), "dropped") {
		t.Errorf("got %v, want dropped error", err)
	}
}

func TestRunRejectsWrongSecret(t *testing.T) {
	f := newFakeServer(t)
	b := NewBroadcastService("app", "key", "wrong", strings.TrimPrefix(f.URL, "http://"), "", startedLogger(t))
	err := b.Run(context.Background())
	var se *StatusError
	if !errors.As(err, &se) || se.Code != http.StatusUnauthorized {
		t.Fatalf("got %v, want 401 StatusError", err)
	}
}

func TestContextIsPropagatedAndHonoured(t *testing.T) {
	f := newFakeServer(t)
	b := newRunning(t, f)
	defer b.Stop(context.Background())

	if err := b.Publish(context.Background(), "c", "e", "x"); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	tp := f.traceparent
	f.mu.Unlock()
	if tp == "" {
		t.Error("traceparent header not sent; the server call is not part of the trace")
	}

	f.fail(http.StatusServiceUnavailable, 100)
	defer f.fail(0, 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	before := f.calls.Load()
	if err := b.Publish(ctx, "c", "e", "x"); !errors.Is(err, context.Canceled) {
		t.Errorf("got %v, want context.Canceled", err)
	}
	if n := f.calls.Load() - before; n != 0 {
		t.Errorf("cancelled publish made %d calls, want 0", n)
	}
}

func TestPayloadLimit(t *testing.T) {
	f := newFakeServer(t)
	b := newRunning(t, f, WithMaxPayloadKB(1))
	defer b.Stop(context.Background())
	if err := b.Publish(context.Background(), "c", "e", strings.Repeat("x", 2048)); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("got %v, want ErrInvalidArgument", err)
	}
}
