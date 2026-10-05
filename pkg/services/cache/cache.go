package cache

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

var incrScript = redis.NewScript(`
local current = redis.call('INCR', KEYS[1])
if current == 1 and tonumber(ARGV[1]) > 0 then
	redis.call('PEXPIRE', KEYS[1], ARGV[1])
end
return current
`)

type CacheService struct {
	url              []string
	password         string
	sentinelPassword string
	sentinelAddrs    []string
	masterName       string
	client           *redis.Client
	prefix           string
}

func NewCacheService(
	url []string, password, sentinelPassword, prefix string,
) CacheServices {
	return &CacheService{
		url:              url,
		password:         password,
		sentinelPassword: sentinelPassword,
		prefix:           prefix,
	}
}

func NewSentinelCacheService(
	sentinelAddrs []string, masterName, password, sentinelPassword, prefix string,
) CacheServices {
	return &CacheService{
		sentinelAddrs:    sentinelAddrs,
		masterName:       masterName,
		password:         password,
		sentinelPassword: sentinelPassword,
		prefix:           prefix,
	}
}

func (c *CacheService) newClient() (*redis.Client, error) {
	if len(c.sentinelAddrs) > 0 {
		if c.masterName == "" {
			return nil, errors.New("cache: sentinel master name is required")
		}
		return redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:       c.masterName,
			SentinelAddrs:    c.sentinelAddrs,
			Password:         c.password,
			SentinelPassword: c.sentinelPassword,
			DialTimeout:      dialTimeout,
			ReadTimeout:      ioTimeout,
			WriteTimeout:     ioTimeout,
			PoolSize:         poolSize,
		}), nil
	}
	if len(c.url) != 1 {
		return nil, fmt.Errorf("cache: expected exactly one redis url, got %d", len(c.url))
	}
	opt, err := redis.ParseURL(c.url[0])
	if err != nil {
		return nil, fmt.Errorf("cache: parse redis url: %w", err)
	}
	if c.password != "" {
		opt.Password = c.password
	}
	opt.DialTimeout = dialTimeout
	opt.ReadTimeout = ioTimeout
	opt.WriteTimeout = ioTimeout
	opt.PoolSize = poolSize
	return redis.NewClient(opt), nil
}

func (c *CacheService) Run(ctx context.Context) error {
	if c.client != nil {
		return errors.New("cache: service already running")
	}
	client, err := c.newClient()
	if err != nil {
		return err
	}
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return fmt.Errorf("cache: ping redis: %w", err)
	}
	c.client = client
	return nil
}

func (c *CacheService) Stop(ctx context.Context) error {
	if c.client == nil {
		return nil
	}
	err := c.client.Close()
	c.client = nil
	if err != nil {
		return fmt.Errorf("cache: close redis client: %w", err)
	}
	return nil
}

func (c *CacheService) Ping(ctx context.Context) error {
	client, err := c.connected()
	if err != nil {
		return err
	}
	if err := client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("cache: ping: %w", err)
	}
	return nil
}
func (c *CacheService) Flush(ctx context.Context) error {
	client, err := c.connected()
	if err != nil {
		return err
	}
	keys, err := c.scan(ctx, client, globEscape(c.prefix)+"*")
	if err != nil {
		return err
	}
	return deleteKeys(ctx, client, keys)
}

func (c *CacheService) Get(ctx context.Context, key string) ([]byte, error) {
	client, err := c.connected()
	if err != nil {
		return nil, err
	}
	val, err := client.Get(ctx, c.key(key)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("cache: get: %w", err)
	}
	return val, nil
}

func (c *CacheService) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	client, err := c.connected()
	if err != nil {
		return err
	}
	data, err := encode(value)
	if err != nil {
		return err
	}
	if err := client.Set(ctx, c.key(key), data, ttl).Err(); err != nil {
		return fmt.Errorf("cache: set: %w", err)
	}
	return nil
}

// SetNX stores value only if the key does not exist. It reports whether the
// value was stored.
func (c *CacheService) SetNX(ctx context.Context, key string, value any, ttl time.Duration) (bool, error) {
	client, err := c.connected()
	if err != nil {
		return false, err
	}
	data, err := encode(value)
	if err != nil {
		return false, err
	}
	stored, err := client.SetNX(ctx, c.key(key), data, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("cache: setnx: %w", err)
	}
	return stored, nil
}

func (c *CacheService) Exists(ctx context.Context, key string) (bool, error) {
	client, err := c.connected()
	if err != nil {
		return false, err
	}
	n, err := client.Exists(ctx, c.key(key)).Result()
	if err != nil {
		return false, fmt.Errorf("cache: exists: %w", err)
	}
	return n > 0, nil
}

func (c *CacheService) Delete(ctx context.Context, key string) error {
	client, err := c.connected()
	if err != nil {
		return err
	}
	if err := client.Del(ctx, c.key(key)).Err(); err != nil {
		return fmt.Errorf("cache: delete: %w", err)
	}
	return nil
}

// Keys returns the keys matching pattern, without the prefix. The pattern uses
// Redis glob syntax (*, ?, [...]). It uses SCAN, so it does not block the server.
func (c *CacheService) Keys(ctx context.Context, pattern string) ([]string, error) {
	client, err := c.connected()
	if err != nil {
		return nil, err
	}
	raw, err := c.scan(ctx, client, globEscape(c.prefix)+pattern)
	if err != nil {
		return nil, err
	}
	keys := make([]string, len(raw))
	for i, k := range raw {
		keys[i] = strings.TrimPrefix(k, c.prefix)
	}
	return keys, nil
}

// Expire sets a TTL on an existing key. It reports false if the key does not
// exist. ttl must be positive; a zero TTL would delete the key immediately.
func (c *CacheService) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	client, err := c.connected()
	if err != nil {
		return false, err
	}
	if ttl <= 0 {
		return false, errors.New("cache: expire ttl must be positive")
	}
	ok, err := client.PExpire(ctx, c.key(key), ttl).Result()
	if err != nil {
		return false, fmt.Errorf("cache: expire: %w", err)
	}
	return ok, nil
}

// Incr atomically increments the counter and returns the new value. The TTL is
// set when the counter is created, so it is a fixed window. A zero TTL means
// no expiry.
func (c *CacheService) Incr(ctx context.Context, key string, ttl time.Duration) (int64, error) {
	client, err := c.connected()
	if err != nil {
		return 0, err
	}
	n, err := incrScript.Run(ctx, client, []string{c.key(key)}, ttl.Milliseconds()).Int64()
	if err != nil {
		return 0, fmt.Errorf("cache: incr: %w", err)
	}
	return n, nil
}

func (c *CacheService) ZAdd(ctx context.Context, key string, score float64, member any) error {
	client, err := c.connected()
	if err != nil {
		return err
	}
	m, err := encode(member)
	if err != nil {
		return err
	}
	if err := client.ZAdd(ctx, c.key(key), redis.Z{Score: score, Member: m}).Err(); err != nil {
		return fmt.Errorf("cache: zadd: %w", err)
	}
	return nil
}

func (c *CacheService) ZRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	client, err := c.connected()
	if err != nil {
		return nil, err
	}
	members, err := client.ZRange(ctx, c.key(key), start, stop).Result()
	if err != nil {
		return nil, fmt.Errorf("cache: zrange: %w", err)
	}
	return members, nil
}

func (c *CacheService) ZRangeWithScores(ctx context.Context, key string, start, stop int64) ([]redis.Z, error) {
	client, err := c.connected()
	if err != nil {
		return nil, err
	}
	members, err := client.ZRangeWithScores(ctx, c.key(key), start, stop).Result()
	if err != nil {
		return nil, fmt.Errorf("cache: zrange with scores: %w", err)
	}
	return members, nil
}

func (c *CacheService) ZCard(ctx context.Context, key string) (int64, error) {
	client, err := c.connected()
	if err != nil {
		return 0, err
	}
	n, err := client.ZCard(ctx, c.key(key)).Result()
	if err != nil {
		return 0, fmt.Errorf("cache: zcard: %w", err)
	}
	return n, nil
}

// ZRem removes members from the sorted set and returns how many were removed.
func (c *CacheService) ZRem(ctx context.Context, key string, members ...any) (int64, error) {
	client, err := c.connected()
	if err != nil {
		return 0, err
	}
	encoded := make([]any, len(members))
	for i, m := range members {
		if encoded[i], err = encode(m); err != nil {
			return 0, err
		}
	}
	n, err := client.ZRem(ctx, c.key(key), encoded...).Result()
	if err != nil {
		return 0, fmt.Errorf("cache: zrem: %w", err)
	}
	return n, nil
}

// ZRemRangeByScore removes members with scores in [min, max]. Bounds use Redis
// syntax, such as "-inf", "+inf", "(1" for exclusive.
func (c *CacheService) ZRemRangeByScore(ctx context.Context, key string, min, max string) (int64, error) {
	client, err := c.connected()
	if err != nil {
		return 0, err
	}
	n, err := client.ZRemRangeByScore(ctx, c.key(key), min, max).Result()
	if err != nil {
		return 0, fmt.Errorf("cache: zremrangebyscore: %w", err)
	}
	return n, nil
}

func (c *CacheService) connected() (*redis.Client, error) {
	if c.client == nil {
		return nil, ErrNotRunning
	}
	return c.client, nil
}

func (c *CacheService) key(key string) string {
	return c.prefix + key
}

// scan returns every raw key matching glob, which must already include the escaped prefix.
func (c *CacheService) scan(ctx context.Context, client *redis.Client, glob string) ([]string, error) {
	var (
		cursor uint64
		keys   []string
	)
	for {
		page, next, err := client.Scan(ctx, cursor, glob, scanCount).Result()
		if err != nil {
			return nil, fmt.Errorf("cache: scan: %w", err)
		}
		keys = append(keys, page...)
		if next == 0 {
			return keys, nil
		}
		cursor = next
	}
}
