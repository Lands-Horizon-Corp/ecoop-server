package cache

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	valkey "github.com/valkey-io/valkey-go"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

type CacheService struct {
	mu     sync.RWMutex
	client valkey.Client

	inflight *utils.Inflight
	obs      observability.IObserver

	password  string
	addresses []string
}

// WithObserver gives the service an observer for its metrics, traces and logs.
// Without one it observes nothing.
func WithObserver(o observability.IObserver) Option {
	return func(c *CacheService) { c.obs = observability.Pick(o) }
}

func NewCacheService(addresses []string, password string, inflight *utils.Inflight, opts ...Option) ICacheService {
	if inflight == nil {
		inflight = utils.NewInflight()
	}
	c := &CacheService{
		password:  password,
		addresses: addresses,
		inflight:  inflight,
		obs:       observability.Nop(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	return c
}

// begin starts the Operation of one cache call. A service built as a bare
// struct literal has no observer, so it falls back to Nop (without writing, so
// concurrent calls cannot race).
func (c *CacheService) begin(ctx context.Context, name string) (context.Context, observability.Operation) {
	if c.obs == nil {
		return observability.Nop().Begin(ctx, observability.ComponentCache, name)
	}
	return c.obs.Begin(ctx, observability.ComponentCache, name)
}

func (c *CacheService) Run(ctx context.Context) error {
	if len(c.addresses) == 0 {
		return ErrNoAddresses
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.client != nil {
		return nil
	}
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: c.addresses, Password: c.password})
	if err != nil {
		return fmt.Errorf("cache: connecting: %w", err)
	}
	if err := client.Do(ctx, client.B().Ping().Build()).Error(); err != nil {
		client.Close()
		return fmt.Errorf("cache: ping: %w", err)
	}
	c.client = client
	return nil
}

func (c *CacheService) Stop(ctx context.Context) error {
	c.mu.Lock()
	client := c.client
	c.client = nil
	c.mu.Unlock()
	if client == nil {
		return nil
	}
	if err := c.inflight.Wait(ctx); err != nil {
		go client.Close()
		return fmt.Errorf("cache: stop: in-flight operations still running: %w", err)
	}
	client.Close()
	return nil
}

func (c *CacheService) Config() Config {
	return Config{Addresses: slices.Clone(c.addresses), Password: c.password}
}

func (c *CacheService) Client() valkey.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.client
}

func (c *CacheService) doPing(ctx context.Context) error {
	client, done, err := c.running()
	if err != nil {
		return err
	}
	defer done()
	if err := client.Do(ctx, client.B().Ping().Build()).Error(); err != nil {
		return fmt.Errorf("cache: ping: %w", err)
	}
	return nil
}

func (c *CacheService) doFlush(ctx context.Context) error {
	client, done, err := c.running()
	if err != nil {
		return err
	}
	defer done()
	for addr, node := range client.Nodes() {
		err := node.Do(ctx, node.B().Flushdb().Build()).Error()
		if verr, ok := valkey.IsValkeyErr(err); err != nil && (!ok || !strings.HasPrefix(verr.Error(), "READONLY")) {
			return fmt.Errorf("cache: flushing %s: %w", addr, err)
		}
	}
	return nil
}

func (c *CacheService) doGet(ctx context.Context, key string) ([]byte, error) {
	client, done, err := c.running()
	if err != nil {
		return nil, err
	}
	defer done()
	val, err := client.Do(ctx, client.B().Get().Key(key).Build()).AsBytes()
	if valkey.IsValkeyNil(err) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("cache: get: %w", err)
	}
	return val, nil
}

func (c *CacheService) doSet(ctx context.Context, key string, value any, ttl time.Duration) error {
	client, done, err := c.running()
	if err != nil {
		return err
	}
	defer done()
	val, err := utils.Encode(value)
	if err != nil {
		return fmt.Errorf("cache: encoding value: %w", err)
	}
	cmd := client.B().Set().Key(key).Value(val)
	if ttl > 0 {
		err = client.Do(ctx, cmd.PxMilliseconds(utils.TTLMillis(ttl)).Build()).Error()
	} else {
		err = client.Do(ctx, cmd.Build()).Error()
	}
	if err != nil {
		return fmt.Errorf("cache: set: %w", err)
	}
	return nil
}

func (c *CacheService) doSetNX(ctx context.Context, key string, value any, ttl time.Duration) (bool, error) {
	client, done, err := c.running()
	if err != nil {
		return false, err
	}
	defer done()
	val, err := utils.Encode(value)
	if err != nil {
		return false, fmt.Errorf("cache: encoding value: %w", err)
	}
	cmd := client.B().Set().Key(key).Value(val).Nx()
	if ttl > 0 {
		err = client.Do(ctx, cmd.PxMilliseconds(utils.TTLMillis(ttl)).Build()).Error()
	} else {
		err = client.Do(ctx, cmd.Build()).Error()
	}
	if valkey.IsValkeyNil(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("cache: setnx: %w", err)
	}
	return true, nil
}

func (c *CacheService) doExists(ctx context.Context, key string) (bool, error) {
	client, done, err := c.running()
	if err != nil {
		return false, err
	}
	defer done()
	n, err := client.Do(ctx, client.B().Exists().Key(key).Build()).AsInt64()
	if err != nil {
		return false, fmt.Errorf("cache: exists: %w", err)
	}
	return n > 0, nil
}

func (c *CacheService) doDelete(ctx context.Context, key string) (bool, error) {
	client, done, err := c.running()
	if err != nil {
		return false, err
	}
	defer done()
	n, err := client.Do(ctx, client.B().Del().Key(key).Build()).AsInt64()
	if err != nil {
		return false, fmt.Errorf("cache: delete: %w", err)
	}
	return n > 0, nil
}

func (c *CacheService) doKeys(ctx context.Context, pattern string) ([]string, error) {
	client, done, err := c.running()
	if err != nil {
		return nil, err
	}
	defer done()
	seen := make(map[string]struct{})
	keys := []string{}
	for addr, node := range client.Nodes() {
		var cursor uint64
		for {
			entry, err := node.Do(ctx, node.B().Scan().Cursor(cursor).Match(pattern).Count(ScanCount).Build()).AsScanEntry()
			if err != nil {
				return nil, fmt.Errorf("cache: scanning %s: %w", addr, err)
			}
			for _, k := range entry.Elements {
				if _, dup := seen[k]; !dup {
					seen[k] = struct{}{}
					keys = append(keys, k)
				}
			}
			if cursor = entry.Cursor; cursor == 0 {
				break
			}
		}
	}
	return keys, nil
}

func (c *CacheService) doZAdd(ctx context.Context, key string, score float64, member any) error {
	client, done, err := c.running()
	if err != nil {
		return err
	}
	defer done()
	m, err := utils.Encode(member)
	if err != nil {
		return fmt.Errorf("cache: encoding member: %w", err)
	}
	if err := client.Do(ctx, client.B().Zadd().Key(key).ScoreMember().ScoreMember(score, m).Build()).Error(); err != nil {
		return fmt.Errorf("cache: zadd: %w", err)
	}
	return nil
}

func (c *CacheService) doZRange(ctx context.Context, key string, start int64, stop int64) ([]string, error) {
	client, done, err := c.running()
	if err != nil {
		return nil, err
	}
	defer done()
	cmd := client.B().Zrange().Key(key).Min(strconv.FormatInt(start, 10)).Max(strconv.FormatInt(stop, 10)).Build()
	out, err := client.Do(ctx, cmd).AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("cache: zrange: %w", err)
	}
	return out, nil
}

func (c *CacheService) doZCard(ctx context.Context, key string) (int64, error) {
	client, done, err := c.running()
	if err != nil {
		return 0, err
	}
	defer done()
	n, err := client.Do(ctx, client.B().Zcard().Key(key).Build()).AsInt64()
	if err != nil {
		return 0, fmt.Errorf("cache: zcard: %w", err)
	}
	return n, nil
}

func (c *CacheService) doZRem(ctx context.Context, key string, members ...any) (int64, error) {
	client, done, err := c.running()
	if err != nil {
		return 0, err
	}
	defer done()
	if len(members) == 0 {
		return 0, nil
	}
	ms := make([]string, len(members))
	for i, m := range members {
		if ms[i], err = utils.Encode(m); err != nil {
			return 0, fmt.Errorf("cache: encoding member: %w", err)
		}
	}
	n, err := client.Do(ctx, client.B().Zrem().Key(key).Member(ms...).Build()).AsInt64()
	if err != nil {
		return 0, fmt.Errorf("cache: zrem: %w", err)
	}
	return n, nil
}

func (c *CacheService) doZRemRangeByScore(ctx context.Context, key string, lowest string, highest string) (int64, error) {
	client, done, err := c.running()
	if err != nil {
		return 0, err
	}
	defer done()
	n, err := client.Do(ctx, client.B().Zremrangebyscore().Key(key).Min(lowest).Max(highest).Build()).AsInt64()
	if err != nil {
		return 0, fmt.Errorf("cache: zremrangebyscore: %w", err)
	}
	return n, nil
}

func (c *CacheService) doIncr(ctx context.Context, key string, ttl time.Duration) (int64, error) {
	client, done, err := c.running()
	if err != nil {
		return 0, err
	}
	defer done()
	var n int64
	if ttl <= 0 {
		n, err = client.Do(ctx, client.B().Incr().Key(key).Build()).AsInt64()
	} else {
		n, err = incrScript.Exec(ctx, client, []string{key}, []string{strconv.FormatInt(utils.TTLMillis(ttl), 10)}).AsInt64()
	}
	if err != nil {
		return 0, fmt.Errorf("cache: incr: %w", err)
	}
	return n, nil
}

func (c *CacheService) doExpire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	client, done, err := c.running()
	if err != nil {
		return false, err
	}
	defer done()
	ok, err := client.Do(ctx, client.B().Pexpire().Key(key).Milliseconds(utils.TTLMillis(ttl)).Build()).AsBool()
	if err != nil {
		return false, fmt.Errorf("cache: expire: %w", err)
	}
	return ok, nil
}

func (c *CacheService) running() (client valkey.Client, done func(), err error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.client == nil {
		return nil, nil, ErrNotRunning
	}
	return c.client, c.inflight.Track(), nil
}
