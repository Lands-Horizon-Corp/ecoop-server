package cache

import (
	"context"
	"errors"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

var (
	incrScript = valkey.NewLuaScript(`
	local n = redis.call('INCR', KEYS[1])
	local ttl = tonumber(ARGV[1])
	if ttl > 0 then
		redis.call('PEXPIRE', KEYS[1], ttl, 'NX')
	end
	return n
`)
)
var (
	ErrNotFound    = errors.New("cache: key not found")
	ErrNotRunning  = errors.New("cache: service not running; call Run first")
	ErrNoAddresses = errors.New("cache: no addresses configured")
	ErrNilValue    = utils.ErrNilValue
)

const (
	ScanCount = 1000
)

// Config is how a CacheService connects to Valkey.
type Config struct {
	Addresses []string
	Password  string
}

type CacheServices interface {
	Client() valkey.Client
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
	ZCard(ctx context.Context, key string) (int64, error)
	ZRem(ctx context.Context, key string, members ...any) (int64, error)
	ZRemRangeByScore(ctx context.Context, key string, min, max string) (int64, error)
	SetNX(ctx context.Context, key string, value any, ttl time.Duration) (bool, error)
	Incr(ctx context.Context, key string, ttl time.Duration) (int64, error)
	Expire(ctx context.Context, key string, ttl time.Duration) (bool, error)

	// Config returns the settings the cache was created with.
	Config() Config
}
