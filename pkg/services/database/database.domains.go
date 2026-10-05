package database

import (
	"context"
	"time"

	"github.com/uptrace/bun"
)

// Shared infrastructure contracts. This package must not import pagination or cqrs.
type (
	Channel string
	Events  []string

	LogService interface {
		Log(ctx context.Context, message string)
		Error(ctx context.Context, message string)
		Warn(ctx context.Context, message string)
		Fatal(ctx context.Context, message string)
		Success(ctx context.Context, message string)
	}

	BroadcastService interface {
		Broadcast(channels []Channel, events Events, payload any) error
	}

	MessageBrokerService interface {
		Publish(ctx context.Context, topic string, key, value []byte) error
		Subscribe(ctx context.Context, topic string, handler func(key, value []byte) error) error
	}

	CacheService interface {
		Get(ctx context.Context, key string) ([]byte, error)
		Set(ctx context.Context, key string, value any, ttl time.Duration) error
	}

	SQLServices interface {
		Ping(ctx context.Context) error
		Client() *bun.DB
		Find(ctx context.Context, model any, dest any) error
	}
)
