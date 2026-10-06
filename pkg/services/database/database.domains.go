package database

import (
	"context"
)

type (
	MessageBrokerService interface {
		Publish(ctx context.Context, topic string, key, value []byte) error
		Subscribe(ctx context.Context, topic string, handler func(key, value []byte) error) error
	}

	DatabaseService interface {
		Run(ctx context.Context) error
		Register(ctx context.Context) error
	}
)
