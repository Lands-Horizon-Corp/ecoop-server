package regressions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/cache"
	"github.com/redis/go-redis/v9"
)

// These tests check that consumers depend only on cache.CacheServices.
//
// profileStore and rateLimiter take the interface and nothing else. The same
// scenario runs on the Redis-backed CacheService and on memoryCache below. If
// either one needed Redis-specific behaviour, the scenario would fail on the
// other.

// ---- consumers --------------------------------------------------------------

type userProfile struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// profileStore caches user profiles. It knows only the cache interface.
type profileStore struct {
	cache cache.CacheServices
}

func (s profileStore) Save(ctx context.Context, p userProfile) error {
	return s.cache.Set(ctx, "profile:"+p.ID, p, time.Hour)
}

func (s profileStore) Load(ctx context.Context, id string) (userProfile, bool, error) {
	raw, err := s.cache.Get(ctx, "profile:"+id)
	if errors.Is(err, cache.ErrNotFound) {
		return userProfile{}, false, nil
	}
	if err != nil {
		return userProfile{}, false, fmt.Errorf("load profile: %w", err)
	}
	var p userProfile
	if err := json.Unmarshal(raw, &p); err != nil {
		return userProfile{}, false, fmt.Errorf("decode profile: %w", err)
	}
	return p, true, nil
}

func (s profileStore) Forget(ctx context.Context, id string) error {
	return s.cache.Delete(ctx, "profile:"+id)
}

// rateLimiter allows at most limit calls per key in each fixed window.
type rateLimiter struct {
	cache  cache.CacheServices
	limit  int64
	window time.Duration
}

func (r rateLimiter) Allow(ctx context.Context, key string) (bool, error) {
	n, err := r.cache.Incr(ctx, "rl:"+key, r.window)
	if err != nil {
		return false, fmt.Errorf("rate limit: %w", err)
	}
	return n <= r.limit, nil
}

// ---- scenario run on every implementation -----------------------------------

// runConsumerScenario asserts the behaviour the consumers expect from any
// cache.CacheServices. It must pass unchanged for every implementation.
func runConsumerScenario(t *testing.T, c cache.CacheServices) {
	t.Helper()
	ctx := context.Background()
	store := profileStore{cache: c}
	limiter := rateLimiter{cache: c, limit: 3, window: time.Minute}

	t.Run("ProfileRoundTrip", func(t *testing.T) {
		want := userProfile{ID: "u-1", Name: "Jane"}
		if err := store.Save(ctx, want); err != nil {
			t.Fatalf("save: %v", err)
		}
		got, ok, err := store.Load(ctx, "u-1")
		if err != nil || !ok || got != want {
			t.Fatalf("load = %+v, found=%v, err=%v; want %+v", got, ok, err, want)
		}
	})

	t.Run("MissingProfileIsNotAnError", func(t *testing.T) {
		_, ok, err := store.Load(ctx, "nobody")
		if err != nil || ok {
			t.Fatalf("load missing = found=%v, err=%v; want found=false, err=nil", ok, err)
		}
	})

	t.Run("ForgetRemovesProfile", func(t *testing.T) {
		if err := store.Save(ctx, userProfile{ID: "u-2", Name: "Omar"}); err != nil {
			t.Fatalf("save: %v", err)
		}
		if err := store.Forget(ctx, "u-2"); err != nil {
			t.Fatalf("forget: %v", err)
		}
		if _, ok, _ := store.Load(ctx, "u-2"); ok {
			t.Fatal("profile still present after Forget")
		}
	})

	t.Run("RateLimitFixedWindow", func(t *testing.T) {
		for i := 1; i <= 3; i++ {
			ok, err := limiter.Allow(ctx, "ip-1")
			if err != nil || !ok {
				t.Fatalf("call %d allowed=%v, err=%v; want allowed", i, ok, err)
			}
		}
		ok, err := limiter.Allow(ctx, "ip-1")
		if err != nil || ok {
			t.Fatalf("call 4 allowed=%v, err=%v; want blocked", ok, err)
		}
		// A different key has its own budget.
		if ok, err := limiter.Allow(ctx, "ip-2"); err != nil || !ok {
			t.Fatalf("other key allowed=%v, err=%v; want allowed", ok, err)
		}
	})
}

func TestCacheConsumers_RunOnAnyCacheImplementation(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		runConsumerScenario(t, newMemoryCache())
	})

	t.Run("redis via miniredis", func(t *testing.T) {
		factory, _ := newMiniredisCache(t)
		runConsumerScenario(t, newIsolatedCache(t, factory))
	})
}

func TestCacheConsumers_ErrorsPropagateThroughInterface(t *testing.T) {
	ctx := context.Background()
	errBackend := errors.New("backend down")

	t.Run("rate limiter returns backend error", func(t *testing.T) {
		mem := newMemoryCache()
		mem.err = errBackend
		_, err := rateLimiter{cache: mem, limit: 1, window: time.Minute}.Allow(ctx, "k")
		if !errors.Is(err, errBackend) {
			t.Fatalf("err = %v; want wrapped backend error", err)
		}
	})

	t.Run("profile load does not treat backend error as missing", func(t *testing.T) {
		mem := newMemoryCache()
		mem.err = errBackend
		_, ok, err := profileStore{cache: mem}.Load(ctx, "u-1")
		if !errors.Is(err, errBackend) {
			t.Fatalf("err = %v; want wrapped backend error", err)
		}
		if ok {
			t.Fatal("backend failure reported as found")
		}
	})
}

// ---- interface conformance --------------------------------------------------

// The fake and the real service must both implement the interface. Any
// method added to cache.CacheServices then breaks the build here, not at runtime.
var (
	_ cache.CacheServices = (*memoryCache)(nil)
)

func TestCacheService_ExposesOnlyTheInterface(t *testing.T) {
	iface := reflect.TypeFor[cache.CacheServices]()
	svc := cache.NewCacheService([]string{"redis://127.0.0.1:1"}, "", "", "")
	concrete := reflect.TypeOf(svc)

	if !concrete.Implements(iface) {
		t.Fatalf("%v does not implement %v", concrete, iface)
	}
	// Callers must not be able to reach methods outside the interface without a type assertion.
	if concrete.NumMethod() != iface.NumMethod() {
		t.Fatalf("concrete type has %d exported methods; interface has %d — extra methods leak past the abstraction",
			concrete.NumMethod(), iface.NumMethod())
	}
}

// ---- in-memory implementation -----------------------------------------------

// memoryCache is an in-memory cache.CacheServices for tests. TTLs are honoured
// against the wall clock, like Redis. Sorted-set methods are not implemented,
// because no consumer under test uses them.
type memoryCache struct {
	mu      sync.Mutex
	entries map[string]memoryEntry
	err     error // when set, every call returns it
}

type memoryEntry struct {
	value   []byte
	expires time.Time // zero means no expiry
}

var (
	errMemoryUnsupported = errors.New("memoryCache: sorted sets not implemented")
	errMemoryNoClient    = errors.New("memoryCache: has no redis client")
)

func newMemoryCache() *memoryCache {
	return &memoryCache{entries: make(map[string]memoryEntry)}
}

// live returns the entry if present and unexpired. The caller must hold mu.
func (m *memoryCache) live(key string) (memoryEntry, bool) {
	e, ok := m.entries[key]
	if !ok {
		return memoryEntry{}, false
	}
	if !e.expires.IsZero() && !time.Now().Before(e.expires) {
		delete(m.entries, key)
		return memoryEntry{}, false
	}
	return e, true
}

// put stores value. The caller must hold mu.
func (m *memoryCache) put(key string, value []byte, ttl time.Duration) {
	e := memoryEntry{value: value}
	if ttl > 0 {
		e.expires = time.Now().Add(ttl)
	}
	m.entries[key] = e
}

func (m *memoryCache) Client() (*redis.Client, error) { return nil, errMemoryNoClient }

func (m *memoryCache) Run(ctx context.Context) error  { return m.err }
func (m *memoryCache) Stop(ctx context.Context) error { return nil }

func (m *memoryCache) Ping(ctx context.Context) error { return m.err }

func (m *memoryCache) Flush(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.entries = make(map[string]memoryEntry)
	return nil
}

func (m *memoryCache) Get(ctx context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	e, ok := m.live(key)
	if !ok {
		return nil, cache.ErrNotFound
	}
	return append([]byte(nil), e.value...), nil
}

func (m *memoryCache) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	data, err := memoryEncode(value)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.put(key, data, ttl)
	return nil
}

func (m *memoryCache) SetNX(ctx context.Context, key string, value any, ttl time.Duration) (bool, error) {
	data, err := memoryEncode(value)
	if err != nil {
		return false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return false, m.err
	}
	if _, ok := m.live(key); ok {
		return false, nil
	}
	m.put(key, data, ttl)
	return true, nil
}

func (m *memoryCache) Exists(ctx context.Context, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return false, m.err
	}
	_, ok := m.live(key)
	return ok, nil
}

func (m *memoryCache) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	delete(m.entries, key)
	return nil
}

func (m *memoryCache) Keys(ctx context.Context, pattern string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	var keys []string
	for k := range m.entries {
		if _, ok := m.live(k); !ok {
			continue
		}
		if matched, _ := path.Match(pattern, k); matched {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func (m *memoryCache) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, errors.New("memoryCache: expire ttl must be positive")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return false, m.err
	}
	e, ok := m.live(key)
	if !ok {
		return false, nil
	}
	e.expires = time.Now().Add(ttl)
	m.entries[key] = e
	return true, nil
}

// Incr keeps the TTL set when the key was created, like the Lua script in CacheService.
func (m *memoryCache) Incr(ctx context.Context, key string, ttl time.Duration) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return 0, m.err
	}
	var n int64
	e, ok := m.live(key)
	if ok {
		parsed, err := strconv.ParseInt(string(e.value), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("memoryCache: incr: %w", err)
		}
		n = parsed
	}
	n++
	e.value = []byte(strconv.FormatInt(n, 10))
	if !ok {
		m.put(key, e.value, ttl)
	} else {
		m.entries[key] = e
	}
	return n, nil
}

func (m *memoryCache) ZAdd(ctx context.Context, key string, score float64, member any) error {
	return m.sortedSetErr()
}

func (m *memoryCache) ZRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return nil, m.sortedSetErr()
}

func (m *memoryCache) ZRangeWithScores(ctx context.Context, key string, start, stop int64) ([]redis.Z, error) {
	return nil, m.sortedSetErr()
}

func (m *memoryCache) ZCard(ctx context.Context, key string) (int64, error) {
	return 0, m.sortedSetErr()
}

func (m *memoryCache) ZRem(ctx context.Context, key string, members ...any) (int64, error) {
	return 0, m.sortedSetErr()
}

func (m *memoryCache) ZRemRangeByScore(ctx context.Context, key string, min, max string) (int64, error) {
	return 0, m.sortedSetErr()
}

func (m *memoryCache) sortedSetErr() error {
	if m.err != nil {
		return m.err
	}
	return errMemoryUnsupported
}

// memoryEncode mirrors CacheService: []byte and string are stored as-is, anything else as JSON.
func memoryEncode(value any) ([]byte, error) {
	switch v := value.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return json.Marshal(v)
	}
}
