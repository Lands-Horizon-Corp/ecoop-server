package api_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/api"
)

// These tests use a real listener and real connections: a body that is too big,
// a client that never finishes its request, a stream, and shutdown. None of that
// exists in the in-process harness.

type live struct {
	svc  *api.APIService
	addr string
}

// serve starts the service on a free loopback port. routes are registered first.
func serve(t testing.TB, cfg api.Config, routes func(*api.APIService), opts ...api.Option) *live {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	svc := api.NewAPIService(0, nil, nil, append([]api.Option{api.WithConfig(cfg), api.WithListener(l)}, opts...)...).(*api.APIService)
	if routes != nil {
		routes(svc)
	}
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Cleanup(func() { _ = svc.Stop(context.Background()) })
	return &live{svc: svc, addr: l.Addr().String()}
}

func (l *live) url(p string) string { return "http://" + l.addr + p }

func basicRoutes(s *api.APIService) {
	_ = s.RegisterRoute(api.Route{Route: "/echo", Method: "POST"}, func(_ context.Context, c *app.RequestContext) error {
		c.String(200, fmt.Sprint(len(c.Request.Body())))
		return nil
	})
	_ = s.RegisterRoute(api.Route{Route: "/hello", Method: "GET"}, func(_ context.Context, c *app.RequestContext) error {
		c.String(200, "hello")
		return nil
	})
}

// ---- lifecycle -------------------------------------------------------------

func TestSocket_StartRunServeStop(t *testing.T) {
	t.Parallel()
	lv := serve(t, api.Config{}, basicRoutes)
	// One client keeps its connection open across Stop, to see what a keep-alive
	// client is told; a second connects fresh afterwards.
	kept := &http.Client{Timeout: 2 * time.Second}
	resp, err := kept.Get(lv.url("/hello"))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(b) != "hello" {
		t.Fatalf("GET /hello = %d %q", resp.StatusCode, b)
	}
	if resp.Header.Get("X-Content-Type-Options") != "nosniff" || resp.Header.Get("X-Request-Id") == "" {
		t.Errorf("real response headers: %v", resp.Header)
	}
	if err := lv.svc.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	fresh := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	if r, err := fresh.Get(lv.url("/hello")); err == nil {
		r.Body.Close()
		t.Error("a new connection was accepted after Stop")
	}
	// An already-open connection is refused, not served, and told to go away.
	if r, err := kept.Get(lv.url("/hello")); err == nil {
		r.Body.Close()
		if r.StatusCode != http.StatusServiceUnavailable {
			t.Errorf("a kept-alive client after Stop got %d, want 503 or a closed connection", r.StatusCode)
		}
	}
	if err := lv.svc.Stop(context.Background()); err != nil {
		t.Errorf("second Stop = %v, want nil", err)
	}
}

// Probes answer over a real connection, with no auth, and say nothing about why.
func TestSocket_ProbesOverRealHTTP(t *testing.T) {
	t.Parallel()
	failing := api.Check{Name: "database", Fn: func(context.Context) error { return fmt.Errorf("dial tcp 10.1.2.3:5432: refused") }}
	lv := serve(t, api.Config{}, basicRoutes, api.WithReadiness(failing))
	for path, want := range map[string]int{api.LivenessPath: 200, api.ReadinessPath: 503} {
		resp, err := http.Get(lv.url(path))
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("%s = %d, want %d", path, resp.StatusCode, want)
		}
		if strings.Contains(string(b), "10.1.2.3") || strings.Contains(string(b), "5432") {
			t.Errorf("%s leaked a detail: %s", path, b)
		}
	}
}

// pprof is served by Hertz's net/http adaptor, which writes to the connection
// itself. Over a real connection the profile arrives whole, and the headers the
// middleware set before the handler are still on it.
func TestSocket_PprofBodyAndHeadersOverARealConnection(t *testing.T) {
	t.Parallel()
	lv := serve(t, api.Config{EnablePprof: true}, basicRoutes)
	for _, p := range []string{"/debug/pprof/cmdline", "/debug/pprof/", "/debug/pprof/goroutine?debug=1"} {
		resp, err := http.Get(lv.url(p))
		if err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || len(b) == 0 {
			t.Errorf("%s = %d with %d bytes", p, resp.StatusCode, len(b))
		}
		if resp.Header.Get("X-Content-Type-Options") != "nosniff" || resp.Header.Get("X-Request-Id") == "" ||
			resp.Header.Get("Content-Security-Policy") == "" {
			t.Errorf("%s lost the middleware's headers: %v", p, resp.Header)
		}
	}
	// Not enabled: nothing is registered, over the wire as well.
	off := serve(t, api.Config{}, basicRoutes)
	if resp, err := http.Get(off.url("/debug/pprof/cmdline")); err != nil || resp.StatusCode != http.StatusNotFound {
		t.Errorf("pprof while disabled: %v", err)
	} else {
		resp.Body.Close()
	}
}

// ---- body limits on a real connection --------------------------------------

func TestSocket_OversizedBodyIsRefusedWhateverTheFraming(t *testing.T) {
	t.Parallel()
	lv := serve(t, api.Config{MaxBodyBytes: 4096}, basicRoutes)
	post := func(body io.Reader, length int64) int {
		req, _ := http.NewRequest(http.MethodPost, lv.url("/echo"), body)
		req.ContentLength = length // -1 means chunked
		resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
		if err != nil {
			// A server may close the connection after refusing a body it did not read.
			return -1
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.StatusCode
	}
	if got := post(strings.NewReader(strings.Repeat("a", 4096)), 4096); got != 200 {
		t.Errorf("exactly at the limit = %d", got)
	}
	if got := post(strings.NewReader(strings.Repeat("a", 100)), 100); got != 200 {
		t.Errorf("below = %d", got)
	}
	if got := post(strings.NewReader(strings.Repeat("a", 4097)), 4097); got != 413 {
		t.Errorf("declared one byte over = %d, want 413", got)
	}
	if got := post(strings.NewReader(strings.Repeat("a", 1<<20)), 1<<20); got != 413 && got != -1 {
		t.Errorf("declared 1 MiB = %d, want 413 or a closed connection", got)
	}
	// Chunked: no declared length to check, so the server must count the bytes.
	if got := post(io.NopCloser(strings.NewReader(strings.Repeat("a", 1<<20))), -1); got == 200 {
		t.Errorf("a chunked 1 MiB body was accepted by a 4 KiB limit")
	}
	// A lying Content-Length (short) is the HTTP layer's to reject; the server must
	// not hang on it and must stay up.
	conn, err := net.Dial("tcp", lv.addr)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = fmt.Fprintf(conn, "POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 10\r\n\r\nabc")
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, _ = bufio.NewReader(conn).ReadString('\n')
	conn.Close()
	if got := post(strings.NewReader("ok"), 2); got != 200 {
		t.Errorf("the server stopped answering after a truncated body: %d", got)
	}
}

// ---- slow clients ----------------------------------------------------------

// A client that opens a connection and never finishes its request must not hold
// a server slot forever: ReadTimeout closes it. This is what stops slowloris.
func TestSocket_SlowClientIsCutOffByTheReadTimeout(t *testing.T) {
	t.Parallel()
	lv := serve(t, api.Config{ReadTimeout: 500 * time.Millisecond, WriteTimeout: time.Second, IdleTimeout: time.Second}, basicRoutes)
	conn, err := net.Dial("tcp", lv.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, _ = fmt.Fprintf(conn, "GET /hello HTTP/1.1\r\nHost: x\r\nX-Slow: ") // never ends the header
	start := time.Now()
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, rerr := io.ReadAll(conn)
	took := time.Since(start)
	if took > 8*time.Second {
		t.Fatalf("a slow client held its connection for %v (read error %v)", took, rerr)
	}
	// And the server is still serving others.
	resp, err := http.Get(lv.url("/hello"))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Errorf("after the slow client: %v", err)
	}
	if resp != nil {
		resp.Body.Close()
	}
}

// An idle keep-alive connection is closed after IdleTimeout.
func TestSocket_IdleConnectionIsClosed(t *testing.T) {
	t.Parallel()
	lv := serve(t, api.Config{IdleTimeout: 400 * time.Millisecond, ReadTimeout: time.Second, WriteTimeout: 2 * time.Second}, basicRoutes)
	conn, err := net.Dial("tcp", lv.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, _ = fmt.Fprintf(conn, "GET /hello HTTP/1.1\r\nHost: x\r\n\r\n")
	br := bufio.NewReader(conn)
	first, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, first.Body) // the body must be consumed or ReadByte returns it
	first.Body.Close()
	_ = conn.SetReadDeadline(time.Now().Add(8 * time.Second))
	start := time.Now()
	if _, err := br.ReadByte(); err == nil {
		t.Fatal("the idle connection was not closed")
	}
	if took := time.Since(start); took > 6*time.Second {
		t.Errorf("the idle connection lasted %v with IdleTimeout of 400ms", took)
	}
}

// Headers over MaxHeaderBytes are refused, and the server survives.
func TestSocket_OversizedHeadersAreRefused(t *testing.T) {
	t.Parallel()
	lv := serve(t, api.Config{MaxHeaderBytes: 2048}, basicRoutes)
	req, _ := http.NewRequest(http.MethodGet, lv.url("/hello"), nil)
	req.Header.Set("X-Big", strings.Repeat("a", 64<<10))
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err == nil {
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Errorf("a 64 KiB header was accepted by a 2 KiB limit")
		}
	}
	if r, err := http.Get(lv.url("/hello")); err != nil || r.StatusCode != http.StatusOK {
		t.Errorf("after the oversized header: %v", err)
	} else {
		r.Body.Close()
	}
}

// ---- streaming ---------------------------------------------------------------

type ev struct {
	Status    string   `json:"status"`
	Data      any      `json:"data"`
	Message   []string `json:"message"`
	RequestID string   `json:"request_id"`
}

func readEvents(t testing.TB, body io.Reader) []ev {
	t.Helper()
	var out []ev
	sc := bufio.NewScanner(body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var e ev
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			t.Fatalf("a stream line is not JSON: %q (%v)", sc.Text(), err)
		}
		out = append(out, e)
	}
	return out
}

func streamRoute(path string, cb func(ctx context.Context, c *app.RequestContext, send func(string, any, ...string)) error) func(*api.APIService) {
	return func(s *api.APIService) {
		if err := s.RegisterSSR(api.Route{Route: path, Method: "GET"}, cb); err != nil {
			panic(err)
		}
	}
}

// Events arrive one per line, in order, as they are sent, and the stream ends with "done".
func TestStream_EventsArriveInOrderAndEndWithDone(t *testing.T) {
	t.Parallel()
	lv := serve(t, api.Config{}, streamRoute("/progress", func(_ context.Context, _ *app.RequestContext, send func(string, any, ...string)) error {
		for i := range 5 {
			send("progress", map[string]int{"step": i}, "working")
		}
		return nil
	}))
	resp, err := http.Get(lv.url("/progress"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != api.StreamContentType {
		t.Fatalf("stream = %d %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	for _, k := range []string{"X-Content-Type-Options", "Content-Security-Policy", "X-Request-Id"} {
		if resp.Header.Get(k) == "" {
			t.Errorf("a stream response lacks %s: headers must be set before the first byte", k)
		}
	}
	if resp.Header.Get("Cache-Control") != "no-store" {
		t.Errorf("Cache-Control = %q", resp.Header.Get("Cache-Control"))
	}
	events := readEvents(t, resp.Body)
	if len(events) != 6 {
		t.Fatalf("%d events, want 5 progress and one done: %+v", len(events), events)
	}
	for i := range 5 {
		if events[i].Status != "progress" {
			t.Errorf("event %d = %q", i, events[i].Status)
		}
	}
	if last := events[5]; last.Status != api.StatusStreamDone || last.RequestID == "" {
		t.Errorf("last event = %+v, want done with a request id", last)
	}
}

// Events are delivered while the handler is still running, not buffered to the end.
func TestStream_EventsAreFlushedAsTheyAreSent(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	lv := serve(t, api.Config{}, streamRoute("/live", func(ctx context.Context, _ *app.RequestContext, send func(string, any, ...string)) error {
		send("first", nil)
		select {
		case <-release:
		case <-ctx.Done():
		}
		send("second", nil)
		return nil
	}))
	defer close(release)
	resp, err := http.Get(lv.url("/live"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	line := make(chan string, 1)
	go func() {
		s, _ := bufio.NewReader(resp.Body).ReadString('\n')
		line <- s
	}()
	select {
	case s := <-line:
		if !strings.Contains(s, `"first"`) {
			t.Errorf("first line = %q", s)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the first event was not delivered while the handler was still running")
	}
}

// docs/api.md Priority 0 #5: a failure after the stream began is one generic
// event with the request id. The error's text never reaches the client.
func TestStream_FailureNeverLeaksTheError(t *testing.T) {
	t.Parallel()
	for name, cb := range map[string]func(context.Context, *app.RequestContext, func(string, any, ...string)) error{
		"error": func(context.Context, *app.RequestContext, func(string, any, ...string)) error {
			return fmt.Errorf(`pq: password authentication failed for user "ecoop_admin" at 10.1.2.3`)
		},
		"panic": func(context.Context, *app.RequestContext, func(string, any, ...string)) error {
			panic("card 4111111111111111 of ana@example.com")
		},
	} {
		lv := serve(t, api.Config{}, streamRoute("/boom", cb))
		resp, err := http.Get(lv.url("/boom"))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		for _, leak := range []string{"password", "ecoop_admin", "10.1.2.3", "4111", "ana@example.com", "panic", "goroutine", ".go:"} {
			if strings.Contains(string(raw), leak) {
				t.Errorf("%s: %q leaked into the stream: %s", name, leak, raw)
			}
		}
		events := readEvents(t, strings.NewReader(string(raw)))
		if len(events) == 0 || events[len(events)-1].Status != api.StatusStreamFailed || events[len(events)-1].RequestID == "" {
			t.Errorf("%s: events = %+v, want a final failed event with a request id", name, events)
		}
	}
}

// A handler may report a failure it chose to show, through Public, and only that code.
func TestStream_PublicErrorShowsOnlyItsCode(t *testing.T) {
	t.Parallel()
	lv := serve(t, api.Config{}, streamRoute("/p", func(context.Context, *app.RequestContext, func(string, any, ...string)) error {
		return api.Public(409, "already_running")
	}))
	resp, err := http.Get(lv.url("/p"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	events := readEvents(t, resp.Body)
	last := events[len(events)-1]
	if last.Status != api.StatusStreamFailed || len(last.Message) != 1 || last.Message[0] != "already_running" {
		t.Errorf("last = %+v", last)
	}
}

// Event messages are bounded and cleaned, so a handler cannot forge a line or
// send a log's worth of text.
func TestStream_MessagesAreBoundedAndCleaned(t *testing.T) {
	t.Parallel()
	lv := serve(t, api.Config{}, streamRoute("/m", func(_ context.Context, _ *app.RequestContext, send func(string, any, ...string)) error {
		send("note", nil, "line one\n{\"status\":\"done\"}", strings.Repeat("x", 5000), "a", "b", "c", "dropped")
		return nil
	}))
	resp, err := http.Get(lv.url("/m"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	events := readEvents(t, resp.Body)
	if len(events) != 2 {
		t.Fatalf("%d events, want 2 (a newline in a message must not create a third): %+v", len(events), events)
	}
	if got := len(events[0].Message); got != 4 {
		t.Errorf("%d messages, want at most 4", got)
	}
	for _, m := range events[0].Message {
		if len(m) > 210 || strings.ContainsAny(m, "\n\r") {
			t.Errorf("message not bounded or cleaned: %q", m)
		}
	}
}

// An event that cannot be encoded is replaced by a generic failure, not dropped
// silently and not turned into a stack trace.
func TestStream_UnencodableEventBecomesAGenericFailure(t *testing.T) {
	t.Parallel()
	lv := serve(t, api.Config{}, streamRoute("/u", func(_ context.Context, _ *app.RequestContext, send func(string, any, ...string)) error {
		send("bad", make(chan int))
		return nil
	}))
	resp, err := http.Get(lv.url("/u"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(raw), "chan") || strings.Contains(string(raw), "json:") {
		t.Errorf("an encoding error leaked: %s", raw)
	}
	events := readEvents(t, strings.NewReader(string(raw)))
	if len(events) < 1 || events[0].Status != api.StatusStreamFailed {
		t.Errorf("events = %+v", events)
	}
}

// A stream is bounded in time: past StreamTimeout it is ended with a "limit" event.
func TestStream_EndsAtTheStreamTimeout(t *testing.T) {
	t.Parallel()
	lv := serve(t, api.Config{StreamTimeout: 300 * time.Millisecond}, streamRoute("/forever", func(ctx context.Context, _ *app.RequestContext, send func(string, any, ...string)) error {
		send("started", nil)
		<-ctx.Done()
		return ctx.Err()
	}))
	start := time.Now()
	resp, err := http.Get(lv.url("/forever"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	events := readEvents(t, resp.Body)
	if took := time.Since(start); took > 8*time.Second {
		t.Fatalf("a stream with a 300ms limit ran for %v", took)
	}
	if last := events[len(events)-1]; last.Status != api.StatusStreamLimit {
		t.Errorf("last event = %+v, want limit", last)
	}
}

// A client that leaves ends the handler's work: its context is cancelled once a
// write fails, so a handler that loops on the context does not run forever.
func TestStream_ClientDisconnectStopsTheHandler(t *testing.T) {
	t.Parallel()
	stopped := make(chan struct{})
	lv := serve(t, api.Config{}, streamRoute("/leave", func(ctx context.Context, _ *app.RequestContext, send func(string, any, ...string)) error {
		defer close(stopped)
		for ctx.Err() == nil {
			send("tick", strings.Repeat("x", 4096))
			time.Sleep(5 * time.Millisecond)
		}
		return ctx.Err()
	}))
	conn, err := net.Dial("tcp", lv.addr)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = fmt.Fprintf(conn, "GET /leave HTTP/1.1\r\nHost: x\r\n\r\n")
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, _ = bufio.NewReader(conn).ReadString('\n')
	conn.Close() // the client leaves mid-stream
	select {
	case <-stopped:
	case <-time.After(StreamLeaveGrace):
		t.Fatal("the handler kept running after its client left: a stream would hold a slot forever")
	}
}

// StreamLeaveGrace is how long a handler may take to notice its client left.
const StreamLeaveGrace = 20 * time.Second

// More streams than MaxStreams are refused with 503 and Retry-After, and a slot
// is freed when a stream ends.
func TestStream_ConcurrencyIsCapped(t *testing.T) {
	t.Parallel()
	hold := make(chan struct{})
	var open atomic.Int32
	lv := serve(t, api.Config{MaxStreams: 3}, streamRoute("/hold", func(ctx context.Context, _ *app.RequestContext, send func(string, any, ...string)) error {
		open.Add(1)
		send("open", nil)
		select {
		case <-hold:
		case <-ctx.Done():
		}
		return nil
	}))
	var conns []*http.Response
	for range 3 {
		resp, err := http.Get(lv.url("/hold"))
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, resp)
	}
	deadline := time.Now().Add(5 * time.Second)
	for open.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	resp, err := http.Get(lv.url("/hold"))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
		t.Errorf("the fourth stream = %d, Retry-After %q; want 503 with Retry-After", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	resp.Body.Close()
	close(hold)
	for _, r := range conns {
		_, _ = io.Copy(io.Discard, r.Body)
		r.Body.Close()
	}
	// A slot is free again.
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r, err := http.Get(lv.url("/hold"))
		if err == nil && r.StatusCode == http.StatusOK {
			_, _ = io.Copy(io.Discard, r.Body)
			r.Body.Close()
			return
		}
		if r != nil {
			r.Body.Close()
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Error("a stream slot was never freed after the streams ended")
}

// ---- graceful shutdown -----------------------------------------------------

// Stop lets a request that is running finish, and refuses new ones.
func TestSocket_StopWaitsForARunningRequest(t *testing.T) {
	t.Parallel()
	started := make(chan struct{})
	release := make(chan struct{})
	var finished atomic.Bool
	lv := serve(t, api.Config{ShutdownTimeout: 10 * time.Second}, func(s *api.APIService) {
		_ = s.RegisterRoute(api.Route{Route: "/slow", Method: "GET"}, func(_ context.Context, c *app.RequestContext) error {
			close(started)
			<-release
			finished.Store(true)
			c.String(200, "finished")
			return nil
		})
	})
	var wg sync.WaitGroup
	var body string
	var status int
	wg.Go(func() {
		resp, err := http.Get(lv.url("/slow"))
		if err != nil {
			return
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		body, status = string(b), resp.StatusCode
	})
	<-started
	stopped := make(chan error, 1)
	go func() { stopped <- lv.svc.Stop(context.Background()) }()
	select {
	case <-stopped:
		t.Fatal("Stop returned while a request was still running")
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-stopped:
		if err != nil {
			t.Errorf("Stop = %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Stop did not return after the request finished")
	}
	wg.Wait()
	if !finished.Load() || status != 200 || body != "finished" {
		t.Errorf("the in-flight request was cut: finished=%v status=%d body=%q", finished.Load(), status, body)
	}
}

// Stop is bounded: a request that never ends cannot hold shutdown past ShutdownTimeout.
func TestSocket_StopIsBoundedByTheShutdownTimeout(t *testing.T) {
	t.Parallel()
	started := make(chan struct{})
	never := make(chan struct{})
	t.Cleanup(func() { close(never) })
	lv := serve(t, api.Config{ShutdownTimeout: 500 * time.Millisecond, WriteTimeout: 30 * time.Second, ReadTimeout: 10 * time.Second}, func(s *api.APIService) {
		_ = s.RegisterRoute(api.Route{Route: "/stuck", Method: "GET"}, func(_ context.Context, c *app.RequestContext) error {
			close(started)
			<-never
			return nil
		})
	})
	go func() { _, _ = (&http.Client{Timeout: 20 * time.Second}).Get(lv.url("/stuck")) }()
	<-started
	begin := time.Now()
	_ = lv.svc.Stop(context.Background())
	if took := time.Since(begin); took > 8*time.Second {
		t.Fatalf("Stop took %v with a 500ms ShutdownTimeout", took)
	}
}
