package broadcast

import (
	"context"
	"errors"
)

var (
	ErrNotRunning      = errors.New("broadcast: not running")
	ErrAlreadyRunning  = errors.New("broadcast: already running")
	ErrInvalidArgument = errors.New("broadcast: invalid argument")
	ErrQueueFull       = errors.New("broadcast: queue full")
)

type (
	Channel string
	Events  []string

	BroadcasterServices interface {
		Run(ctx context.Context) error
		Stop(ctx context.Context) error
		Publish(ctx context.Context, channel, event string, payload any) error
		Dispatch(ctx context.Context, channels []string, event string, payload any) error
		Broadcast(ctx context.Context, channels []Channel, events Events, payload any) error
		Runner(ctx context.Context)
	}
)
