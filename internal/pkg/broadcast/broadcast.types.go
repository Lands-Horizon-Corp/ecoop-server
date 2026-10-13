package broadcast

import (
	"context"
	"errors"
	"time"
)

const (
	MaxChannelsPerTrigger = 100 // the Pusher protocol's limit per call; Dispatch batches beyond it
	MaxChannelName        = 164
	MaxEventName          = 200
	DefaultMaxPayloadKB   = 10 // Pusher's default; Sockudo allows more when configured
	RequestTimeout        = 10 * time.Second

	HealthChannel     = "system-health" // Run announces the service here
	HealthEvent       = "ping"
	HeartbeatChannel  = "system-heartbeat" // Runner ticks here
	HeartbeatEvent    = "heartbeat"
	HeartbeatInterval = time.Second
)

var (
	ErrNotStarted      = errors.New("broadcast: service not started")
	ErrNoTracker       = errors.New("broadcast: service has no in-flight tracker; create it with NewBroadcastService")
	ErrNoAppID         = errors.New("broadcast: missing app id")
	ErrNoKey           = errors.New("broadcast: missing app key")
	ErrNoSecret        = errors.New("broadcast: missing app secret")
	ErrNoHost          = errors.New("broadcast: missing host")
	ErrNoPort          = errors.New("broadcast: missing port")
	ErrConnect         = errors.New("broadcast: cannot reach the broadcast server")
	ErrNoChannels      = errors.New("broadcast: no channels")
	ErrNoEvents        = errors.New("broadcast: no events")
	ErrInvalidChannel  = errors.New("broadcast: invalid channel name (1-164 of A-Z a-z 0-9 _ - = @ , . ;)")
	ErrInvalidEvent    = errors.New("broadcast: invalid event name (1-200 characters, not pusher: or pusher_internal:)")
	ErrEncode          = errors.New("broadcast: payload cannot be encoded as JSON")
	ErrPayloadTooLarge = errors.New("broadcast: payload is larger than the limit")
	ErrPublish         = errors.New("broadcast: publishing failed")
)

type (
	IBroadcasterService interface {
		Start(ctx context.Context) error
		Run(ctx context.Context) error
		Stop(ctx context.Context) error
		Ping(ctx context.Context) error
		Runner(ctx context.Context) error
		Publish(ctx context.Context, channel, event string, payload any) error
		Dispatch(ctx context.Context, channels []string, event string, payload any) error
		Broadcast(ctx context.Context, channels []string, events []string, payload any) error
	}
)
