//go:build integration

package broadcast_test

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/broadcast"
)

// Sad path: wrong settings and wrong calls are refused with the right
// error, and nothing reaches subscribers.

func TestSad_StartReportsEveryMissingSetting(t *testing.T) {
	t.Parallel()
	err := broadcast.NewBroadcastService("", "", "", "", "", false, 0, nil).Start(context.Background())
	for _, want := range []error{broadcast.ErrNoAppID, broadcast.ErrNoKey, broadcast.ErrNoSecret, broadcast.ErrNoHost, broadcast.ErrNoPort} {
		if !errors.Is(err, want) {
			t.Errorf("Start = %v, missing %v", err, want)
		}
	}
}

func TestSad_WrongCredentialsOrServer(t *testing.T) {
	t.Parallel()
	requireServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	wrong := broadcast.NewBroadcastService(appID, appKey, "not-the-secret", host, port, false, 0, nil)
	if err := wrong.Start(ctx); !errors.Is(err, broadcast.ErrConnect) {
		t.Errorf("wrong secret: %v", err)
	}
	unknownApp := broadcast.NewBroadcastService("no-such-app", appKey, appSecret, host, port, false, 0, nil)
	if err := unknownApp.Start(ctx); !errors.Is(err, broadcast.ErrConnect) {
		t.Errorf("unknown app: %v", err)
	}
	nowhere := broadcast.NewBroadcastService(appID, appKey, appSecret, "127.0.0.1", "1", false, 0, nil)
	if err := nowhere.Start(ctx); !errors.Is(err, broadcast.ErrConnect) {
		t.Errorf("closed port: %v", err)
	}
}

func TestSad_CallsBeforeStartAndAfterStop(t *testing.T) {
	t.Parallel()
	requireServer(t)
	b := service(host, port)
	ctx := ctxT(t)
	ch := channel()
	check := func(when string) {
		t.Helper()
		for name, err := range map[string]error{
			"Ping": b.Ping(ctx), "Publish": b.Publish(ctx, ch, "e", 1), "Dispatch": b.Dispatch(ctx, []string{ch}, "e", 1),
			"Broadcast": b.Broadcast(ctx, []string{ch}, []string{"e"}, 1), "Runner": b.Runner(ctx),
		} {
			if !errors.Is(err, broadcast.ErrNotStarted) {
				t.Errorf("%s: %s = %v, want ErrNotStarted", when, name, err)
			}
		}
	}
	check("before Start")
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	check("after Stop")
}

func TestSad_BadArgumentsSendNothing(t *testing.T) {
	t.Parallel()
	b := started(t, 1) // 1 KB limit
	ch := channel()
	sub := connect(t)
	sub.subscribe(t, ch)
	ctx := ctxT(t)
	for name, c := range map[string]struct {
		err  error
		want error
	}{
		"no channels":      {b.Dispatch(ctx, nil, "e", 1), broadcast.ErrNoChannels},
		"no events":        {b.Broadcast(ctx, []string{ch}, nil, 1), broadcast.ErrNoEvents},
		"empty event":      {b.Publish(ctx, ch, "", 1), broadcast.ErrInvalidEvent},
		"empty channel":    {b.Publish(ctx, "", "e", 1), broadcast.ErrInvalidChannel},
		"channel function": {b.Publish(ctx, ch, "e", func() {}), broadcast.ErrEncode},
		"channel of chan":  {b.Publish(ctx, ch, "e", make(chan int)), broadcast.ErrEncode},
		"NaN":              {b.Publish(ctx, ch, "e", math.NaN()), broadcast.ErrEncode},
		"too large":        {b.Publish(ctx, ch, "e", strings.Repeat("x", 1025)), broadcast.ErrPayloadTooLarge},
		"one bad event":    {b.Broadcast(ctx, []string{ch}, []string{"ok", "pusher:spoof"}, 1), broadcast.ErrInvalidEvent},
		"one bad channel":  {b.Dispatch(ctx, []string{ch, "bad channel"}, "e", 1), broadcast.ErrInvalidChannel},
	} {
		if !errors.Is(c.err, c.want) {
			t.Errorf("%s: %v, want %v", name, c.err, c.want)
		}
	}
	sub.quiet(t, 500*time.Millisecond)
}
