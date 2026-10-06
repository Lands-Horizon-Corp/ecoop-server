package broadcast

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/logger"
	"github.com/pusher/pusher-http-go/v5"
	"go.opentelemetry.io/otel/attribute"
)

type BroadcasterService struct {
	client pusher.Client
	log    logger.LogContextService
}

// NewBroadcastService builds a Pusher-backed broadcaster. log is optional (it must already be started);
// when set, every publish is traced and failures are logged with their channels and event.
func NewBroadcastService(
	appID, key, secret, host, port string, log logger.LogContextService) BroadcasterServices {
	return &BroadcasterService{
		client: pusher.Client{
			AppID:  appID,
			Key:    key,
			Secret: secret,
			Host:   fmt.Sprintf("%s:%s", host, port),
		},
		log: log,
	}
}

// observe runs fn inside a span named name. Failures are logged as errors with the channels and event;
// successes are debug lines, because a broadcaster publishes constantly. The payload is never logged.
func (b *BroadcasterService) observe(name string, channels []string, event string, fn func() error) error {
	if b.log == nil {
		return fn()
	}
	_, lvl := b.log.Trace(name,
		attribute.StringSlice("broadcast.channels", channels), attribute.String("broadcast.event", event))
	defer lvl.Span().End()
	kv := []any{"component", "broadcast", "channels", channels, "event", event}
	if err := fn(); err != nil {
		lvl.Error(err, name+" failed", kv...)
		return err
	}
	lvl.Debug(name+" done", kv...)
	return nil
}

func envelope(payload any) map[string]any {
	return map[string]any{"success": true, "data": payload}
}

func (b *BroadcasterService) Run(_ context.Context) error {
	err := b.Publish(context.Background(), healthChannel, "ping", map[string]any{
		"status": "ok",
		"time":   time.Now().Unix(),
	})
	if err != nil {
		return fmt.Errorf("broadcaster health check failed: %w", err)
	}
	if b.log != nil {
		b.log.Emit("broadcast.ready", func(l logger.LoggerLevel) {
			l.Info("broadcaster ready", "component", "broadcast")
		})
	}
	return nil
}

func (b *BroadcasterService) Publish(_ context.Context, channel, event string, payload any) error {
	return b.observe("broadcast.publish", []string{channel}, event, func() error {
		if err := b.client.Trigger(channel, event, envelope(payload)); err != nil {
			return fmt.Errorf("failed to trigger event %s on channel %s: %w", event, channel, err)
		}
		return nil
	})
}

func (b *BroadcasterService) Dispatch(_ context.Context, channels []string, event string, payload any) error {
	return b.observe("broadcast.dispatch", channels, event, func() error {
		if err := b.client.TriggerMulti(channels, event, envelope(payload)); err != nil {
			return fmt.Errorf("failed to dispatch event %s to %d channels: %w", event, len(channels), err)
		}
		return nil
	})
}

// Broadcast sends each non-blank event to every channel and returns the failures joined together. The
// caller decides whether to wait: the CQRS service already calls it from its own goroutine.
func (b *BroadcasterService) Broadcast(channels []Channel, events Events, payload any) error {
	if len(channels) == 0 {
		return nil
	}
	names := make([]string, len(channels))
	for i, c := range channels {
		names[i] = string(c)
	}
	var errs []error
	for _, event := range events {
		event = strings.TrimSpace(event)
		if event == "" {
			continue
		}
		if err := b.Dispatch(context.Background(), names, event, payload); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Runner publishes a timestamp on a test channel until ctx ends, for checking the connection end to end.
// Only failures are logged: a line per successful tick would drown everything else.
func (b *BroadcasterService) Runner(ctx context.Context) {
	ticker := time.NewTicker(runnerTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case t := <-ticker.C:
			err := b.client.Trigger(runnerChannel, runnerEvent, envelope(map[string]any{
				"timestamp":    t.Format("2006-01-02T15:04:05.000Z07:00"),
				"date":         t.Format(time.DateOnly),
				"time":         t.Format(time.TimeOnly),
				"second":       t.Second(),
				"milliseconds": t.Nanosecond() / 1000000,
			}))
			if err != nil && b.log != nil {
				b.log.Emit("broadcast.runner", func(l logger.LoggerLevel) {
					l.Warn("broadcaster runner tick failed", "component", "broadcast", "error", err.Error())
				})
			}
		}
	}
}
