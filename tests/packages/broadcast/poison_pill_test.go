//go:build integration

package broadcast_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/broadcast"
)

// Poison pill: hostile payloads, names and subscribers.

// Content that looks like markup, protocol messages or control characters
// is delivered as data, byte for byte.
func TestPoison_HostilePayloadsArriveAsData(t *testing.T) {
	t.Parallel()
	b := started(t)
	ch := channel()
	sub := connect(t)
	sub.subscribe(t, ch)
	ctx := ctxT(t)
	for i, p := range []map[string]string{
		{"memo": `<script>alert(document.cookie)</script>`},
		{"memo": `{"event":"pusher:subscribe","data":{"channel":"private-admin"}}`},
		{"memo": "nul\x00byte, tab\t, newline\n, emoji 💸, 中文"},
		{"memo": `'; DROP TABLE ledger; --`},
	} {
		if err := b.Publish(ctx, ch, "memo", p); err != nil {
			t.Fatalf("payload %d: %v", i, err)
		}
		var got map[string]string
		if err := json.Unmarshal([]byte(sub.expect(t, 1, 10*time.Second)[0].Data), &got); err != nil || got["memo"] != p["memo"] {
			t.Fatalf("payload %d arrived as %v (%v)", i, got, err)
		}
	}
}

// Names that could escape the channel or impersonate the protocol are
// refused before anything is sent.
func TestPoison_HostileNames(t *testing.T) {
	t.Parallel()
	b := started(t)
	ctx := ctxT(t)
	for _, c := range []string{"a b", "a/b", `a"b`, "a\nb", "a\x00b", "ünicode", strings.Repeat("x", broadcast.MaxChannelName+1), "#all", "*"} {
		if err := b.Publish(ctx, c, "e", 1); !errors.Is(err, broadcast.ErrInvalidChannel) {
			t.Errorf("channel %q: %v", c, err)
		}
	}
	for _, e := range []string{"pusher:subscribe", "pusher_internal:subscription_succeeded", " padded ", strings.Repeat("e", broadcast.MaxEventName+1)} {
		if err := b.Publish(ctx, channel(), e, 1); !errors.Is(err, broadcast.ErrInvalidEvent) {
			t.Errorf("event %.30q: %v", e, err)
		}
	}
}

// The size limit is exact: at the limit is sent, one byte over is not.
func TestPoison_PayloadAtTheLimit(t *testing.T) {
	t.Parallel()
	b := started(t, 2)
	ch := channel()
	sub := connect(t)
	sub.subscribe(t, ch)
	ctx := ctxT(t)
	if err := b.Publish(ctx, ch, "e", strings.Repeat("x", 2<<10)); err != nil {
		t.Fatalf("at the limit: %v", err)
	}
	if len(sub.expect(t, 1, 10*time.Second)[0].Data) != 2<<10 {
		t.Fatal("payload at the limit was cut")
	}
	if err := b.Publish(ctx, ch, "e", strings.Repeat("x", 2<<10+1)); !errors.Is(err, broadcast.ErrPayloadTooLarge) {
		t.Fatalf("over the limit: %v", err)
	}
}

// Repeated channels or events go out once.
func TestPoison_DuplicatesAreSentOnce(t *testing.T) {
	t.Parallel()
	b := started(t)
	ch := channel()
	sub := connect(t)
	sub.subscribe(t, ch)
	if err := b.Broadcast(ctxT(t), []string{ch, ch, ch}, []string{"e", "e"}, 1); err != nil {
		t.Fatal(err)
	}
	sub.expect(t, 1, 10*time.Second)
	sub.quiet(t, 500*time.Millisecond)
}

// A subscriber without a valid signature cannot join a private channel.
func TestPoison_ForgedPrivateSubscription(t *testing.T) {
	t.Parallel()
	b := started(t)
	ch := channel("private-")
	sub := connect(t)
	_ = sub.send(ctxT(t), map[string]any{"event": "pusher:subscribe", "data": map[string]any{
		"channel": ch, "auth": sign(sub.socketID, ch, "guessed-secret"),
	}})
	if err := b.Publish(ctxT(t), ch, "secret.event", map[string]string{"pin": "1234"}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		m, ok := sub.next(time.Until(deadline))
		if !ok {
			return
		}
		if m.Event == "secret.event" || m.Event == "pusher_internal:subscription_succeeded" {
			t.Fatalf("a forged signature joined a private channel: %+v", m)
		}
	}
}
