package cache

import (
	"context"
	"errors"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
)

// Every public operation is the same two lines: begin an Operation, end it with
// the error. The work itself is in the do* methods of cache.go. A missing key
// and a lost SetNX are answers, not failures, so they end as skipped instead of
// an error: the error rate then says the cache is unwell, not that a key was
// absent.

func (c *CacheService) Ping(ctx context.Context) (err error) {
	ctx, op := c.begin(ctx, "ping")
	defer func() { op.End(err) }()
	return c.doPing(ctx)
}

func (c *CacheService) Flush(ctx context.Context) (err error) {
	ctx, op := c.begin(ctx, "flush")
	defer func() { op.End(err) }()
	return c.doFlush(ctx)
}

func (c *CacheService) Get(ctx context.Context, key string) (val []byte, err error) {
	ctx, op := c.begin(ctx, "get")
	defer func() {
		if errors.Is(err, ErrNotFound) {
			op.SetOutcome(observability.OutcomeSkipped) // a miss
			op.End(nil)
			return
		}
		op.End(err)
	}()
	return c.doGet(ctx, key)
}

func (c *CacheService) Set(ctx context.Context, key string, value any, ttl time.Duration) (err error) {
	ctx, op := c.begin(ctx, "set")
	defer func() { op.End(err) }()
	return c.doSet(ctx, key, value, ttl)
}

func (c *CacheService) SetNX(ctx context.Context, key string, value any, ttl time.Duration) (set bool, err error) {
	ctx, op := c.begin(ctx, "setnx")
	defer func() {
		if err == nil && !set {
			op.SetOutcome(observability.OutcomeSkipped) // the key was already there
		}
		op.End(err)
	}()
	return c.doSetNX(ctx, key, value, ttl)
}

func (c *CacheService) Exists(ctx context.Context, key string) (found bool, err error) {
	ctx, op := c.begin(ctx, "exists")
	defer func() { op.End(err) }()
	return c.doExists(ctx, key)
}

func (c *CacheService) Delete(ctx context.Context, key string) (deleted bool, err error) {
	ctx, op := c.begin(ctx, "delete")
	defer func() { op.End(err) }()
	return c.doDelete(ctx, key)
}

func (c *CacheService) Keys(ctx context.Context, pattern string) (keys []string, err error) {
	ctx, op := c.begin(ctx, "keys")
	defer func() { op.End(err) }()
	return c.doKeys(ctx, pattern)
}

func (c *CacheService) ZAdd(ctx context.Context, key string, score float64, member any) (err error) {
	ctx, op := c.begin(ctx, "zadd")
	defer func() { op.End(err) }()
	return c.doZAdd(ctx, key, score, member)
}

func (c *CacheService) ZRange(ctx context.Context, key string, start, stop int64) (members []string, err error) {
	ctx, op := c.begin(ctx, "zrange")
	defer func() { op.End(err) }()
	return c.doZRange(ctx, key, start, stop)
}

func (c *CacheService) ZCard(ctx context.Context, key string) (n int64, err error) {
	ctx, op := c.begin(ctx, "zcard")
	defer func() { op.End(err) }()
	return c.doZCard(ctx, key)
}

func (c *CacheService) ZRem(ctx context.Context, key string, members ...any) (n int64, err error) {
	ctx, op := c.begin(ctx, "zrem")
	defer func() { op.End(err) }()
	return c.doZRem(ctx, key, members...)
}

func (c *CacheService) ZRemRangeByScore(ctx context.Context, key string, lowest, highest string) (n int64, err error) {
	ctx, op := c.begin(ctx, "zremrangebyscore")
	defer func() { op.End(err) }()
	return c.doZRemRangeByScore(ctx, key, lowest, highest)
}

func (c *CacheService) Incr(ctx context.Context, key string, ttl time.Duration) (n int64, err error) {
	ctx, op := c.begin(ctx, "incr")
	defer func() { op.End(err) }()
	return c.doIncr(ctx, key, ttl)
}

func (c *CacheService) Expire(ctx context.Context, key string, ttl time.Duration) (ok bool, err error) {
	ctx, op := c.begin(ctx, "expire")
	defer func() { op.End(err) }()
	return c.doExpire(ctx, key, ttl)
}
