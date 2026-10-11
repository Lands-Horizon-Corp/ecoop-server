package cache

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	valkey "github.com/valkey-io/valkey-go"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

type CacheService struct {
	mu        sync.RWMutex
	client    valkey.Client
	inflight  *utils.Inflight
	secret    string
	addresses []string
}

func NewCacheService(secret string, addresses []string, inflight *utils.Inflight) CacheServices {
	if inflight == nil {
		inflight = utils.NewInflight()
	}
	return &CacheService{
		secret:    secret,
		addresses: addresses,
		inflight:  inflight,
	}
}

func (c *CacheService) Run(ctx context.Context) error {
	if len(c.addresses) == 0 {
		return ErrNoAddresses
	}
	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: c.addresses,
		Password:    c.secret,
	})
	if err != nil {
		return fmt.Errorf("cache: connecting: %w", err)
	}
	if err := client.Do(ctx, client.B().Ping().Build()).Error(); err != nil {
		client.Close()
		return fmt.Errorf("cache: ping: %w", err)
	}
	c.mu.Lock()
	old := c.client
	c.client = client
	c.mu.Unlock()
	if old != nil {
		// Give operations still on the old client time to finish, then close it.
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), ReplaceDrainTimeout)
			defer cancel()
			_ = c.inflight.Wait(ctx)
			old.Close()
		}()
	}
	return nil
}

// Stop rejects new operations with ErrNotRunning, waits for in-flight ones to
// finish, then closes the connection. If ctx ends first, the connection is
// closed anyway, cutting off whatever is still running, and an error is returned.
func (c *CacheService) Stop(ctx context.Context) error {
	c.mu.Lock()
	client := c.client
	c.client = nil
	c.mu.Unlock()
	if client == nil {
		return nil
	}
	if err := c.inflight.Wait(ctx); err != nil {
		// Close can block on unfinished I/O; don't hold the caller past its deadline.
		go client.Close()
		return fmt.Errorf("cache: stop: in-flight operations still running: %w", err)
	}
	client.Close()
	return nil
}

func (c *CacheService) Client() valkey.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.client
}

func (c *CacheService) running() (client valkey.Client, done func(), err error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.client == nil {
		return nil, nil, ErrNotRunning
	}
	return c.client, c.inflight.Track(), nil
}

func (c *CacheService) Ping(ctx context.Context) error {
	client, done, err := c.running()
	if err != nil {
		return err
	}
	defer done()
	return client.Do(ctx, client.B().Ping().Build()).Error()
}

func (c *CacheService) Flush(ctx context.Context) error {
	client, done, err := c.running()
	if err != nil {
		return err
	}
	defer done()
	for addr, node := range client.Nodes() {
		if err := node.Do(ctx, node.B().Flushdb().Build()).Error(); err != nil && !isReadOnly(err) {
			return fmt.Errorf("cache: flushing %s: %w", addr, err)
		}
	}
	return nil
}

func (c *CacheService) Get(ctx context.Context, key string) ([]byte, error) {
	client, done, err := c.running()
	if err != nil {
		return nil, err
	}
	defer done()
	val, err := client.Do(ctx, client.B().Get().Key(key).Build()).AsBytes()
	if valkey.IsValkeyNil(err) {
		return nil, ErrNotFound
	}
	return val, err
}

func (c *CacheService) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	client, done, err := c.running()
	if err != nil {
		return err
	}
	defer done()
	val, err := encode(value)
	if err != nil {
		return err
	}
	cmd := client.B().Set().Key(key).Value(val)
	if ttl > 0 {
		return client.Do(ctx, cmd.PxMilliseconds(ttlMillis(ttl)).Build()).Error()
	}
	return client.Do(ctx, cmd.Build()).Error()
}

func (c *CacheService) SetNX(ctx context.Context, key string, value any, ttl time.Duration) (bool, error) {
	client, done, err := c.running()
	if err != nil {
		return false, err
	}
	defer done()
	val, err := encode(value)
	if err != nil {
		return false, err
	}
	cmd := client.B().Set().Key(key).Value(val).Nx()
	if ttl > 0 {
		err = client.Do(ctx, cmd.PxMilliseconds(ttlMillis(ttl)).Build()).Error()
	} else {
		err = client.Do(ctx, cmd.Build()).Error()
	}
	if valkey.IsValkeyNil(err) {
		return false, nil
	}
	return err == nil, err
}

func (c *CacheService) Exists(ctx context.Context, key string) (bool, error) {
	client, done, err := c.running()
	if err != nil {
		return false, err
	}
	defer done()
	n, err := client.Do(ctx, client.B().Exists().Key(key).Build()).AsInt64()
	return n > 0, err
}

func (c *CacheService) Delete(ctx context.Context, key string) error {
	client, done, err := c.running()
	if err != nil {
		return err
	}
	defer done()
	return client.Do(ctx, client.B().Del().Key(key).Build()).Error()
}

func (c *CacheService) Keys(ctx context.Context, pattern string) ([]string, error) {
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

func (c *CacheService) ZAdd(ctx context.Context, key string, score float64, member any) error {
	client, done, err := c.running()
	if err != nil {
		return err
	}
	defer done()
	m, err := encode(member)
	if err != nil {
		return err
	}
	return client.Do(ctx, client.B().Zadd().Key(key).ScoreMember().ScoreMember(score, m).Build()).Error()
}

func (c *CacheService) ZRange(ctx context.Context, key string, start int64, stop int64) ([]string, error) {
	client, done, err := c.running()
	if err != nil {
		return nil, err
	}
	defer done()
	cmd := client.B().Zrange().Key(key).Min(strconv.FormatInt(start, 10)).Max(strconv.FormatInt(stop, 10)).Build()
	return client.Do(ctx, cmd).AsStrSlice()
}

func (c *CacheService) ZCard(ctx context.Context, key string) (int64, error) {
	client, done, err := c.running()
	if err != nil {
		return 0, err
	}
	defer done()
	return client.Do(ctx, client.B().Zcard().Key(key).Build()).AsInt64()
}

func (c *CacheService) ZRem(ctx context.Context, key string, members ...any) (int64, error) {
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
		if ms[i], err = encode(m); err != nil {
			return 0, err
		}
	}
	return client.Do(ctx, client.B().Zrem().Key(key).Member(ms...).Build()).AsInt64()
}

func (c *CacheService) ZRemRangeByScore(ctx context.Context, key string, min string, max string) (int64, error) {
	client, done, err := c.running()
	if err != nil {
		return 0, err
	}
	defer done()
	return client.Do(ctx, client.B().Zremrangebyscore().Key(key).Min(min).Max(max).Build()).AsInt64()
}

func (c *CacheService) Incr(ctx context.Context, key string, ttl time.Duration) (int64, error) {
	client, done, err := c.running()
	if err != nil {
		return 0, err
	}
	defer done()
	if ttl <= 0 {
		return client.Do(ctx, client.B().Incr().Key(key).Build()).AsInt64()
	}
	return incrScript.Exec(ctx, client, []string{key}, []string{strconv.FormatInt(ttlMillis(ttl), 10)}).AsInt64()
}

func (c *CacheService) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	client, done, err := c.running()
	if err != nil {
		return false, err
	}
	defer done()
	return client.Do(ctx, client.B().Pexpire().Key(key).Milliseconds(ttlMillis(ttl)).Build()).AsBool()
}

func encode(value any) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case []byte:
		return valkey.BinaryString(v), nil
	case int:
		return strconv.Itoa(v), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case uint32:
		return strconv.FormatUint(uint64(v), 10), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32), nil
	case bool:
		return strconv.FormatBool(v), nil
	case nil:
		return "", ErrNilValue
	}
	if isNil(value) {
		return "", ErrNilValue
	}
	b, err := sonic.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("cache: encoding value: %w", err)
	}
	return string(b), nil
}

func isReadOnly(err error) bool {
	verr, ok := valkey.IsValkeyErr(err)
	return ok && strings.HasPrefix(verr.Error(), "READONLY")
}

// incrScript increments and, when a ttl is given, sets the expiry only if the
// key has none. Running both in one script keeps them atomic: if INCR fails
// (e.g. the value is not an integer) the script aborts before PEXPIRE.
var incrScript = valkey.NewLuaScript(`
local n = redis.call('INCR', KEYS[1])
local ttl = tonumber(ARGV[1])
if ttl > 0 then
	redis.call('PEXPIRE', KEYS[1], ttl, 'NX')
end
return n
`)

// ttlMillis converts ttl to whole milliseconds, rounding positive values
// below 1ms up to 1ms so they are not rejected or treated as "expire now".
func ttlMillis(ttl time.Duration) int64 {
	if ttl > 0 && ttl < time.Millisecond {
		return 1
	}
	return ttl.Milliseconds()
}

// isNil reports typed nils (nil pointers, maps, slices, ...) that would
// otherwise be stored as the JSON literal "null".
func isNil(value any) bool {
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Interface, reflect.Func, reflect.Chan:
		return v.IsNil()
	}
	return false
}
