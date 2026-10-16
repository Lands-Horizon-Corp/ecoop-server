package broadcast

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pusher/pusher-http-go/v5"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

type BroadcasterService struct {
	appID        string
	key          string
	secret       string
	host         string
	port         string
	secured      bool
	maxPayloadKB int

	client    atomic.Pointer[pusher.Client]
	lifecycle sync.Mutex // serialises Start and Stop
	gate      *utils.Gate
}

func NewBroadcastService(
	appID, key, secret, host, port string,
	secured bool,
	maxPayloadKB int,
	inflight *utils.Inflight,
) IBroadcastService {
	if maxPayloadKB <= 0 {
		maxPayloadKB = DefaultMaxPayloadKB
	}
	return &BroadcasterService{
		appID:        appID,
		key:          key,
		secret:       secret,
		host:         host,
		port:         port,
		secured:      secured,
		maxPayloadKB: maxPayloadKB,
		gate:         utils.NewGate(inflight),
	}
}

func (b *BroadcasterService) Start(ctx context.Context) error {
	if b.gate == nil {
		return ErrNoTracker
	}
	var errs []error
	for _, missing := range []struct {
		v   string
		err error
	}{{b.appID, ErrNoAppID}, {b.key, ErrNoKey}, {b.secret, ErrNoSecret}, {b.host, ErrNoHost}, {b.port, ErrNoPort}} {
		if strings.TrimSpace(missing.v) == "" {
			errs = append(errs, missing.err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	b.lifecycle.Lock()
	defer b.lifecycle.Unlock()
	if b.gate.IsOpen() {
		return nil
	}
	client := &pusher.Client{
		AppID:                       b.appID,
		Key:                         b.key,
		Secret:                      b.secret,
		Host:                        net.JoinHostPort(b.host, b.port),
		Secure:                      b.secured,
		HTTPClient:                  &http.Client{Timeout: RequestTimeout},
		OverrideMaxMessagePayloadKB: b.maxPayloadKB,
	}
	if err := await(ctx, func() error { _, err := client.Channels(pusher.ChannelsParams{}); return err }); err != nil {
		return errors.Join(ErrConnect, err)
	}
	b.client.Store(client)
	b.gate.Open()
	return nil
}

// Run starts the service and announces it on HealthChannel.
func (b *BroadcasterService) Run(ctx context.Context) error {
	if err := b.Start(ctx); err != nil {
		return err
	}
	return b.Publish(ctx, HealthChannel, HealthEvent, map[string]any{"status": "ok", "time": time.Now().Unix()})
}

func (b *BroadcasterService) Stop(ctx context.Context) error {
	b.lifecycle.Lock()
	defer b.lifecycle.Unlock()
	return b.gate.Close(ctx)
}

func (b *BroadcasterService) Ping(ctx context.Context) error {
	err := b.call(ctx, func(c *pusher.Client) error {
		_, err := c.Channels(pusher.ChannelsParams{})
		return err
	})
	if err != nil && !errors.Is(err, ErrNotStarted) {
		return errors.Join(ErrConnect, err)
	}
	return err
}

func (b *BroadcasterService) Runner(ctx context.Context) error {
	if !b.gate.IsOpen() {
		return ErrNotStarted
	}
	tick := time.NewTicker(HeartbeatInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case t := <-tick.C:
			err := b.Publish(ctx, HeartbeatChannel, HeartbeatEvent, map[string]any{
				"timestamp":    t.Format("2006-01-02T15:04:05.000Z07:00"),
				"date":         t.Format(time.DateOnly),
				"time":         t.Format(time.TimeOnly),
				"second":       t.Second(),
				"milliseconds": t.Nanosecond() / int(time.Millisecond),
			})
			if errors.Is(err, ErrNotStarted) {
				return nil
			}
		}
	}
}

// Events

func (b *BroadcasterService) Publish(ctx context.Context, channel, event string, payload any) error {
	return b.Dispatch(ctx, []string{channel}, event, payload)
}

// Dispatch sends event to every channel, in batches of
// MaxChannelsPerTrigger. Duplicate channels receive it once.
func (b *BroadcasterService) Dispatch(ctx context.Context, channels []string, event string, payload any) error {
	if len(channels) == 0 {
		return ErrNoChannels
	}
	channels = unique(channels)
	for _, c := range channels {
		if !validChannel.MatchString(c) {
			return fmt.Errorf("%w: %q", ErrInvalidChannel, c)
		}
	}
	if !validEvent(event) {
		return fmt.Errorf("%w: %q", ErrInvalidEvent, event)
	}
	data, err := b.encode(payload)
	if err != nil {
		return fmt.Errorf("broadcast: %w", err)
	}
	var errs []error
	for batch := range slices.Chunk(channels, MaxChannelsPerTrigger) {
		err := b.call(ctx, func(c *pusher.Client) error { return c.TriggerMulti(batch, event, data) })
		if errors.Is(err, ErrNotStarted) || ctx.Err() != nil {
			return err
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("%w: event %q to %d channels: %w", ErrPublish, event, len(batch), err))
		}
	}
	return errors.Join(errs...)
}

func (b *BroadcasterService) Broadcast(ctx context.Context, channels []string, events []string, payload any) error {
	events = unique(events)
	if len(events) == 0 {
		return ErrNoEvents
	}
	for _, e := range events {
		if !validEvent(e) {
			return fmt.Errorf("%w: %q", ErrInvalidEvent, e)
		}
	}
	var errs []error
	for _, e := range events {
		err := b.Dispatch(ctx, channels, e, payload)
		if errors.Is(err, ErrNotStarted) || errors.Is(err, ErrNoChannels) || errors.Is(err, ErrInvalidChannel) ||
			errors.Is(err, ErrEncode) || errors.Is(err, ErrPayloadTooLarge) || ctx.Err() != nil {
			return err
		}
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// Helpers

var validChannel = regexp.MustCompile(`^[A-Za-z0-9_\-=@,.;]{1,164}$`)

func validEvent(e string) bool {
	return e != "" && len(e) <= MaxEventName && strings.TrimSpace(e) == e &&
		!strings.HasPrefix(e, "pusher:") && !strings.HasPrefix(e, "pusher_internal:")
}

func (b *BroadcasterService) encode(payload any) (string, error) {
	var data string
	switch p := payload.(type) {
	case string:
		data = p
	case []byte:
		data = string(p)
	default:
		raw, err := json.Marshal(payload)
		if err != nil {
			return "", errors.Join(ErrEncode, err)
		}
		data = string(raw)
	}
	limit := b.maxPayloadKB
	if limit <= 0 {
		limit = DefaultMaxPayloadKB
	}
	if len(data) > limit<<10 {
		return "", fmt.Errorf("%w: %d bytes, limit %d KB", ErrPayloadTooLarge, len(data), limit)
	}
	return data, nil
}

func unique(items []string) []string {
	seen := make(map[string]bool, len(items))
	out := make([]string, 0, len(items))
	for _, it := range items {
		if !seen[it] {
			seen[it] = true
			out = append(out, it)
		}
	}
	return out
}

func (b *BroadcasterService) call(ctx context.Context, fn func(*pusher.Client) error) error {
	done, ok := b.gate.Enter()
	if !ok {
		return ErrNotStarted
	}
	client := b.client.Load()
	if client == nil {
		done()
		return ErrNotStarted
	}
	errc := make(chan error, 1)
	go func() {
		err := fn(client)
		done()
		errc <- err
	}()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func await(ctx context.Context, fn func() error) error {
	errc := make(chan error, 1)
	go func() { errc <- fn() }()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
