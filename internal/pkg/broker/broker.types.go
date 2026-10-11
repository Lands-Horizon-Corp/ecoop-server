package broker

import (
	"context"
	"errors"
)

var (
	ErrNotRunning            = errors.New("broker: not running")
	ErrAlreadyRunning        = errors.New("broker: already running")
	ErrDuplicateSubscription = errors.New("broker: topic already subscribed")
	ErrInvalidArgument       = errors.New("broker: invalid argument")
)

type (
	MessageBrokerServices interface {
		Run(ctx context.Context) error
		Stop(ctx context.Context) error
		Publish(ctx context.Context, topic string, key, value []byte) error
		Subscribe(ctx context.Context, topic string, handler func(ctx context.Context, key, value []byte) error) error
	}
)
