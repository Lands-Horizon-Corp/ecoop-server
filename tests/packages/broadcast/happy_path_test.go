//go:build integration

package broadcast_test

import (
	"context"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/broadcast"
)

// Happy path: every way of sending reaches the right subscribers, intact.

func TestHappy_DispatchReachesEveryChannelOnce(t *testing.T) {
	t.Parallel()
	b := started(t)
	chs := []string{channel(), channel(), channel()}
	sub := connect(t)
	sub.subscribe(t, chs...)
	if err := b.Dispatch(ctxT(t), chs, "branch.closed", map[string]string{"branch": "MNL"}); err != nil {
		t.Fatal(err)
	}
	got := map[string]int{}
	for _, m := range sub.expect(t, 3, 10*time.Second) {
		got[m.Channel]++
	}
	for _, c := range chs {
		if got[c] != 1 {
			t.Fatalf("channel %s received %d times", c, got[c])
		}
	}
	sub.quiet(t, 300*time.Millisecond)
}

// More channels than one protocol call allows are sent in batches.
func TestHappy_DispatchBeyondTheProtocolLimit(t *testing.T) {
	t.Parallel()
	b := started(t)
	n := broadcast.MaxChannelsPerTrigger*2 + 17
	chs := make([]string, n)
	for i := range chs {
		chs[i] = channel()
	}
	sub := connect(t)
	sub.subscribe(t, chs...)
	if err := b.Dispatch(ctxT(t), chs, "rates.updated", map[string]float64{"savings": 2.5}); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, m := range sub.expect(t, n, 20*time.Second) {
		seen[m.Channel] = true
	}
	if len(seen) != n {
		t.Fatalf("%d of %d channels received the event", len(seen), n)
	}
}

func TestHappy_BroadcastEveryEventToEveryChannel(t *testing.T) {
	t.Parallel()
	b := started(t)
	chs := []string{channel(), channel()}
	sub := connect(t)
	sub.subscribe(t, chs...)
	events := []string{"ledger.posted", "balance.changed", "statement.ready"}
	if err := b.Broadcast(ctxT(t), chs, events, map[string]int{"account": 7}); err != nil {
		t.Fatal(err)
	}
	pairs := map[string]bool{}
	for _, m := range sub.expect(t, 6, 10*time.Second) {
		pairs[m.Channel+"/"+m.Event] = true
	}
	if len(pairs) != 6 {
		t.Fatalf("received %v", pairs)
	}
}

// Strings and bytes are sent as they are; other values as JSON.
func TestHappy_PayloadEncodings(t *testing.T) {
	t.Parallel()
	b := started(t)
	ch := channel()
	sub := connect(t)
	sub.subscribe(t, ch)
	ctx := ctxT(t)
	type transfer struct {
		From   int64 `json:"from"`
		To     int64 `json:"to"`
		Amount int64 `json:"amount_cents"`
	}
	cases := []struct {
		payload any
		want    string
	}{
		{`{"already":"json"}`, `{"already":"json"}`},
		{[]byte(`{"raw":true}`), `{"raw":true}`},
		{transfer{1, 2, 12_550}, `{"from":1,"to":2,"amount_cents":12550}`},
		{[]int{1, 2, 3}, `[1,2,3]`},
		{nil, `null`},
	}
	for i, c := range cases {
		if err := b.Publish(ctx, ch, "e"+string(rune('a'+i)), c.payload); err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		if got := sub.expect(t, 1, 10*time.Second)[0].Data; got != c.want {
			t.Fatalf("case %d: received %q, want %q", i, got, c.want)
		}
	}
}

// Private channels: only a subscriber signed by the app hears them.
func TestHappy_PrivateChannel(t *testing.T) {
	t.Parallel()
	b := started(t)
	ch := channel("private-")
	sub := connect(t)
	sub.subscribe(t, ch)
	if err := b.Publish(ctxT(t), ch, "account.locked", map[string]bool{"locked": true}); err != nil {
		t.Fatal(err)
	}
	if got := sub.expect(t, 1, 10*time.Second)[0]; got.Channel != ch || got.Data != `{"locked":true}` {
		t.Fatalf("received %+v", got)
	}
}

func TestHappy_RunAnnouncesTheService(t *testing.T) {
	t.Parallel()
	requireServer(t)
	sub := connect(t)
	sub.subscribe(t, broadcast.HealthChannel)
	b := service(host, port)
	if err := b.Run(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(context.Background())
	for {
		m := sub.expect(t, 1, 10*time.Second)[0]
		if m.Event == broadcast.HealthEvent { // other tests may announce too
			return
		}
	}
}

func TestHappy_RunnerSendsHeartbeatsUntilCancelled(t *testing.T) {
	t.Parallel()
	b := started(t)
	sub := connect(t)
	sub.subscribe(t, broadcast.HeartbeatChannel)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Runner(ctx) }()
	sub.expect(t, 2, 5*time.Second)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Runner = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Runner ignored its cancelled context")
	}
}

func TestHappy_PingAndRestart(t *testing.T) {
	t.Parallel()
	b := started(t)
	ctx := ctxT(t)
	for range 2 {
		if err := b.Start(ctx); err != nil { // already started: nothing to do
			t.Fatal(err)
		}
		if err := b.Ping(ctx); err != nil {
			t.Fatal(err)
		}
		if err := b.Stop(ctx); err != nil {
			t.Fatal(err)
		}
		if err := b.Start(ctx); err != nil {
			t.Fatal(err)
		}
	}
	ch := channel()
	sub := connect(t)
	sub.subscribe(t, ch)
	if err := b.Publish(ctx, ch, "after.restart", 1); err != nil {
		t.Fatal(err)
	}
	sub.expect(t, 1, 10*time.Second)
}
