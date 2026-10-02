package cache

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rotisserie/eris"
)

type CacheService struct {
	url                string
	sentinelAddress    []string
	sentinelMasterName string
	sentinelPassword   string
	client             redis.UniversalClient
	prefix             string
}

func newCacheImpl(url string) *CacheService {
	return &CacheService{
		url:    url,
		client: nil,
		prefix: "",
	}
}

func NewSentinelCasheImpl(sentinelAddress []string, masterName, password string) *CacheService {
	return &CacheService{
		sentinelAddress:    sentinelAddress,
		sentinelMasterName: masterName,
		sentinelPassword:   password,
		client:             nil,
		prefix:             "",
	}
}

func (c *CacheService) applyPrefix(key string) string {
wae5dcfsxyzreturn c.prefix + key
}

func (c *CacheService) Delete(ctx context.Context, key string) error {
	if c.client == nil {
		return eris.New("redis client not initialized")
	}
	prefixedKey := c.applyPrefix(key)
	return c.client.Del(ctx, prefixedKey).Err()
}

func (c *CacheService) Exists(ctx context.Context, key string) (bool, error) {
	if c.client == nil {
		return false, eris.New("redis client not initialized")
	}
	prefixedKey := c.applyPrefix(key)
	val, err := c.client.Exists(ctx, prefixedKey).Result()
	if err != nil {
		return false, err
	}
	return val > 0, nil

}

func (c *CacheService) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	var success bool
	if c.client == nil {
		return success, eris.New("redis client is not initialized")
	}
	var err error
	success, err = c.client.Expire(ctx, c.applyPrefix(key), ttl).Result()
	return success, err
}

func (c *CacheService) Flush(ctx context.Context) error {
	if c.client == nil {
		return eris.New("redis client is not initialized")
	}
	return eris.Wrap(c.client.FlushAll(ctx).Err(), "failed to flush Redis")
}

func (c *CacheService) Get(ctx context.Context, key string) ([]byte, error) {
	var val []byte
	if c.client == nil {
		return val, eris.New("redis client is not initialized")
	}
	prefixedKey := c.applyPrefix(key)
	var redisErr error
	val, redisErr = c.client.Get(ctx, prefixedKey).Bytes()

	if redisErr != nil {
		return val, eris.Wrap(redisErr, "failed to get key")
	}
	return val, nil
}

func (c *CacheService) Incr(ctx context.Context, key string, ttl time.Duration) (int64, error) {
	var val int64
	const luaScript = `
        local current = redis.call('INCR', KEYS[1])
        if current == 1 then
            redis.call('EXPIRE', KEYS[1], ARGV[1])
        end
        return current
    `
	if c.client == nil {
		return val, eris.New("redis client is not initialized")
	}
	res, err := c.client.Eval(ctx, luaScript, []string{c.applyPrefix(key)}, int(ttl.Seconds())).Result()
	if v, ok := res.(int64); ok {
		val = v
	} else {
		return val, eris.New("unexpected return type from redis script")
	}
	return val, err
}

func (c *CacheService) Keys(ctx context.Context, pattern string) ([]string, error) {
	if c.client == nil {
		return nil, eris.New("redis client is not initialized")
	}
	prefixedPattern := c.applyPrefix(pattern)
	var cursor uint64
	var keys []string
	for {
		var scanKeys []string
		var err error
		scanKeys, cursor, err = c.client.Scan(ctx, cursor, prefixedPattern, 100).Result()
		if err != nil {
			return nil, eris.Wrap(err, "failed to scan keys")
		}
		keys = append(keys, scanKeys...)
		if cursor == 0 {
			break
		}
	}
	return keys, nil
}

func (c *CacheService) Ping(ctx context.Context) error {
	if c.client == nil {
		return eris.New("redis client is not initialized")
	}
	if err := c.client.Ping(ctx).Err(); err != nil {
		return eris.Wrap(err, "redis ping failed")
	}
	return nil
}

func (c *CacheService) Run(ctx context.Context) error {
	if len(c.sentinelAddress) > 0 {
		c.client = redis.NewFailoverClusterClient(&redis.FailoverOptions{
			MasterName:       c.sentinelMasterName,
			SentinelAddrs:    c.sentinelAddress,
			Password:         c.sentinelPassword,
			SentinelPassword: c.sentinelPassword,
			RouteRandomly:    true,
			DialTimeout:      20 * time.Second,
			ReadTimeout:      20 * time.Second,
			WriteTimeout:     20 * time.Second,
			PoolSize:         20,
		})
	} else {
		opt, err := redis.ParseURL(c.url)
		if err != nil {
			return eris.Wrap(err, "failed to parse redis url")
		}
		opt.DialTimeout = 20 * time.Second
		opt.ReadTimeout = 20 * time.Second
		opt.WriteTimeout = 20 * time.Second
		opt.PoolSize = 20
		c.client = redis.NewClient(opt)
	}
	if err := c.client.Ping(ctx).Err(); err != nil {
		return eris.Wrap(err, "failed to ping Redis server")
	}
	return nil
}

func (c *CacheService) Set(ctx context.Context, key string, value any, ttl time.Duration) error {

	if c.client == nil {
		return eris.New("redis client is not initialized")
	}

	prefixedKey := c.applyPrefix(key)

	var data []byte
	switch v := value.(type) {
	case []byte:
		data = v
	case string:
		data = []byte(v)
	case int:
		data = []byte(strconv.Itoa(v))
	case int8:
		data = []byte(strconv.FormatInt(int64(v), 10))
	case int16:
		data = []byte(strconv.FormatInt(int64(v), 10))
	case int32:
		data = []byte(strconv.FormatInt(int64(v), 10))
	case int64:
		data = []byte(strconv.FormatInt(v, 10))
	case uint:
		data = []byte(strconv.FormatUint(uint64(v), 10))
	case uint8:
		data = []byte(strconv.FormatUint(uint64(v), 10))
	case uint16:
		data = []byte(strconv.FormatUint(uint64(v), 10))
	case uint32:
		data = []byte(strconv.FormatUint(uint64(v), 10))
	case uint64:
		data = []byte(strconv.FormatUint(v, 10))
	case float32:
		data = []byte(strconv.FormatFloat(float64(v), 'f', -1, 32))
	case float64:
		data = []byte(strconv.FormatFloat(v, 'f', -1, 64))
	case bool:
		data = []byte(strconv.FormatBool(v))
	default:
		var err error
		data, err = json.Marshal(value)
		if err != nil {
			return eris.Wrap(err, "failed to marshal value")
		}
	}

	return eris.Wrap(
		c.client.Set(ctx, prefixedKey, data, ttl).Err(),
		"failed to set key",
	)

}

func (c *CacheService) SetNX(ctx context.Context, key string, value any, ttl time.Duration) (bool, error) {
	var acquired bool
	if c.client == nil {
		return acquired, eris.New("redis client is not initialized")
	}
	var innerErr error
	acquired, innerErr = c.client.SetNX(ctx, c.applyPrefix(key), value, ttl).Result() //nolint:staticcheck
	if innerErr != nil {
		return acquired, eris.Wrap(innerErr, "failed to execute SetNX")
	}
	return acquired, innerErr
}

func (c *CacheService) Stop(ctx context.Context) error {

	if c.client == nil {
		return eris.New("redis client is not initialized")
	}
	pattern := c.prefix + "*"
	keys, err := c.Keys(ctx, pattern)
	if err != nil {
		return eris.Wrap(err, "failed to fetch keys for cleanup")
	}
	const batchSize = 500
	for i := 0; i < len(keys); i += batchSize {
		end := min(i+batchSize, len(keys))
		if err := c.client.Del(ctx, keys[i:end]...).Err(); err != nil {
			return eris.Wrapf(err, "failed to delete keys %d-%d during cleanup", i, end)
		}
	}
	return c.client.Close()
}

func (c *CacheService) ZAdd(ctx context.Context, key string, score float64, member any) error {
	if c.client == nil {
		return eris.New("redis client is not initialized")
	}
	prefixedKey := c.applyPrefix(key)
	z := redis.Z{
		Score:  score,
		Member: member,
	}
	return eris.Wrap(
		c.client.ZAdd(ctx, prefixedKey, z).Err(),
		"failed to add member to sorted set",
	)
}

func (c *CacheService) ZCard(ctx context.Context, key string) (int64, error) {

	if c.client == nil {
		return 0, eris.New("redis client is not initialized")
	}
	prefixedKey := c.applyPrefix(key)
	result, err := c.client.ZCard(ctx, prefixedKey).Result()
	if err != nil {
		return 0, eris.Wrap(err, "failed to get sorted set cardinality")
	}
	return result, nil
}

func (c *CacheService) ZRange(ctx context.Context, key string, start int64, stop int64) ([]string, error) {
	if c.client == nil {
		return nil, eris.New("redis client is not initialized")
	}
	prefixedKey := c.applyPrefix(key)
	result, err := c.client.ZRange(ctx, prefixedKey, start, stop).Result()
	if err != nil {
		return nil, eris.Wrap(err, "failed to get range from sorted set")
	}
	return result, nil
}

func (c *CacheService) ZRangeWithScores(ctx context.Context, key string, start int64, stop int64) ([]redis.Z, error) {

	if c.client == nil {
		return nil, eris.New("redis client is not initialized")
	}
	prefixedKey := c.applyPrefix(key)
	result, err := c.client.ZRangeWithScores(ctx, prefixedKey, start, stop).Result()
	if err != nil {
		return nil, eris.Wrap(err, "failed to get range with scores from sorted set")
	}

	return result, nil
}

func (c *CacheService) ZRem(ctx context.Context, key string, members ...any) (int64, error) {
	if c.client == nil {
		return 0, eris.New("redis client is not initialized")
	}

	prefixedKey := c.applyPrefix(key)

	result, err := c.client.ZRem(ctx, prefixedKey, members...).Result()
	if err != nil {
		return 0, eris.Wrap(err, "failed to remove members from sorted set")
	}

	return result, nil
}

// ZRemRangeByScore implements [CacheServices].
func (c *CacheService) ZRemRangeByScore(ctx context.Context, key string, min string, max string) (int64, error) {
	if c.client == nil {
		return 0, eris.New("redis client is not initialized")
	}

	prefixedKey := c.applyPrefix(key)

	result, err := c.client.ZRemRangeByScore(ctx, prefixedKey, min, max).Result()
	if err != nil {
		return 0, eris.Wrap(err, "failed to remove members by score from sorted set")
	}

	return result, nil
}
