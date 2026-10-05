package cache

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	// ErrNotFound is returned by Get when the key does not exist.
	ErrNotFound = errors.New("cache: key not found")
	// ErrNotRunning is returned when a method is called before Run or after Stop.
	ErrNotRunning = errors.New("cache: service not running; call Run first")
)

type CacheServices interface {
	Run(ctx context.Context) error
	Stop(ctx context.Context) error
	Ping(ctx context.Context) error
	Flush(ctx context.Context) error
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value any, ttl time.Duration) error
	Exists(ctx context.Context, key string) (bool, error)
	Delete(ctx context.Context, key string) error
	Keys(ctx context.Context, pattern string) ([]string, error)
	ZAdd(ctx context.Context, key string, score float64, member any) error
	ZRange(ctx context.Context, key string, start, stop int64) ([]string, error)
	ZRangeWithScores(ctx context.Context, key string, start, stop int64) ([]redis.Z, error)
	ZCard(ctx context.Context, key string) (int64, error)
	ZRem(ctx context.Context, key string, members ...any) (int64, error)
	ZRemRangeByScore(ctx context.Context, key string, min, max string) (int64, error)
	SetNX(ctx context.Context, key string, value any, ttl time.Duration) (bool, error)
	Incr(ctx context.Context, key string, ttl time.Duration) (int64, error)
	Expire(ctx context.Context, key string, ttl time.Duration) (bool, error)
}
