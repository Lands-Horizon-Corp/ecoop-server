package regressions

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/broadcast"
)

// The broadcaster talks to Pusher over HTTP, so these tests point it at a local server and check what was
// sent and what was logged: failures must name the channels and the event, and never the payload.

type pusherCall struct {
	path     string
	name     string
	channels []string
	data     string
}

type fakePusher struct {
	srv    *httptest.Server
	status atomic.Int32
	mu     sync.Mutex
	calls  []pusherCall
}

func newFakePusher(t *testing.T) *fakePusher {
	t.Helper()
	f := &fakePusher{}
	f.status.Store(http.StatusOK)
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var evt struct {
			Name     string   `json:"name"`
			Channels []string `json:"channels"`
			Data     string   `json:"data"`
		}
		_ = json.Unmarshal(body, &evt)
		f.mu.Lock()
		f.calls = append(f.calls, pusherCall{path: r.URL.Path, name: evt.Name, channels: evt.Channels, data: evt.Data})
		f.mu.Unlock()
		w.WriteHeader(int(f.status.Load()))
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakePusher) snapshot() []pusherCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]pusherCall(nil), f.calls...)
}

func (f *fakePusher) service(t *testing.T, log *recordingLog) broadcast.BroadcasterServices {
	t.Helper()
	u, err := url.Parse(f.srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	if log == nil {
		return broadcast.NewBroadcastService("app1", "key", "secret", u.Hostname(), u.Port(), nil)
	}
	return broadcast.NewBroadcastService("app1", "key", "secret", u.Hostname(), u.Port(), log)
}

func TestBroadcaster_SendsEachNonBlankEventToEveryChannel(t *testing.T) {
	f := newFakePusher(t)
	log := &recordingLog{Context: bg}
	svc := f.service(t, log)

	err := svc.Broadcast([]broadcast.Channel{"accounts", "audit"}, broadcast.Events{"account.created", "  ", " account.updated "}, map[string]string{"id": "a1"})
	if err != nil {
		t.Fatalf("Broadcast: %v", err)
	}

	calls := f.snapshot()
	if len(calls) != 2 {
		t.Fatalf("%d requests; want one per non-blank event: %+v", len(calls), calls)
	}
	for i, want := range []string{"account.created", "account.updated"} {
		c := calls[i]
		if c.path != "/apps/app1/events" || c.name != want || strings.Join(c.channels, ",") != "accounts,audit" {
			t.Errorf("request %d = %+v; want %s on accounts and audit", i, c, want)
		}
		if !strings.Contains(c.data, `"success":true`) || !strings.Contains(c.data, `"data":{"id":"a1"}`) {
			t.Errorf("request %d data = %s; want the payload wrapped as success/data", i, c.data)
		}
	}
	for _, e := range log.snapshot() {
		if e.level == "error" || e.level == "warn" {
			t.Errorf("a successful broadcast logged %+v", e)
		}
	}
}

func TestBroadcaster_FailuresAreLoggedWithChannelsAndEventNotThePayload(t *testing.T) {
	f := newFakePusher(t)
	f.status.Store(http.StatusInternalServerError)
	log := &recordingLog{Context: bg}
	svc := f.service(t, log)

	err := svc.Broadcast([]broadcast.Channel{"accounts"}, broadcast.Events{"account.created", "account.deleted"}, map[string]string{"email": "secret@example.com"})
	if err == nil {
		t.Fatal("Broadcast succeeded against a failing server")
	}
	for _, event := range []string{"account.created", "account.deleted"} {
		if !strings.Contains(err.Error(), event) {
			t.Errorf("the joined error %q does not mention %s", err, event)
		}
		e := log.await(t, event, func(e logEvent) bool { return e.msg == "broadcast.dispatch failed" && e.fields["event"] == event })
		if e.level != "error" || e.err == nil || e.fields["component"] != "broadcast" || strings.Join(e.fields["channels"].([]string), ",") != "accounts" {
			t.Errorf("%s line = %+v", event, e)
		}
	}
	for _, e := range log.snapshot() {
		if strings.Contains(fmt.Sprintf("%v %v %v", e.msg, e.err, e.fields), "secret@example.com") {
			t.Fatalf("the payload reached the log: %+v", e)
		}
	}
}

func TestBroadcaster_PublishAndHealthCheck(t *testing.T) {
	f := newFakePusher(t)
	log := &recordingLog{Context: bg}
	svc := f.service(t, log)

	if err := svc.Publish(bg, "inbox", "message.new", map[string]int{"n": 1}); err != nil {
		t.Fatal(err)
	}
	if err := svc.Run(bg); err != nil {
		t.Fatalf("health check: %v", err)
	}

	calls := f.snapshot()
	if len(calls) != 2 || calls[0].name != "message.new" || strings.Join(calls[0].channels, ",") != "inbox" ||
		calls[1].name != "ping" || strings.Join(calls[1].channels, ",") != "system-health" {
		t.Fatalf("requests = %+v", calls)
	}
	log.await(t, "readiness", withMsg("broadcaster ready"))

	f.status.Store(http.StatusServiceUnavailable)
	err := svc.Run(bg)
	if err == nil || !strings.Contains(err.Error(), "broadcaster health check failed") {
		t.Fatalf("Run against a failing server = %v; want the health check error", err)
	}
	log.await(t, "the failed health check", withMsg("broadcast.publish failed"))
}

func TestBroadcaster_NoChannelsOrNoLoggerChangesNothing(t *testing.T) {
	f := newFakePusher(t)
	svc := f.service(t, nil)

	if err := svc.Broadcast(nil, broadcast.Events{"x"}, nil); err != nil || len(f.snapshot()) != 0 {
		t.Fatalf("Broadcast with no channels = %v, %d requests; want a quiet no-op", err, len(f.snapshot()))
	}
	f.status.Store(http.StatusInternalServerError)
	if err := svc.Publish(bg, "c", "e", nil); err == nil {
		t.Fatal("a failure must still be returned without a logger")
	}
}

func TestBroadcaster_RunnerPublishesUntilItsContextEnds(t *testing.T) {
	f := newFakePusher(t)
	svc := f.service(t, nil)
	ctx, cancel := context.WithCancel(bg)
	done := make(chan struct{})
	go func() { svc.Runner(ctx); close(done) }()

	deadline := time.After(3 * time.Second)
	for len(f.snapshot()) == 0 {
		select {
		case <-deadline:
			t.Fatal("the runner never published")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("the runner did not stop when its context ended")
	}
	if c := f.snapshot()[0]; c.name != "client-test" || strings.Join(c.channels, ",") != "test" {
		t.Errorf("tick = %+v; want client-test on the test channel", c)
	}
}
