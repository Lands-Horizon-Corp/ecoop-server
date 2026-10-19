package utils

import (
	"container/list"
	"math"
	"sync"
	"time"
)

// Defaults for NewLimiter when an argument is not positive.
const (
	defaultLimiterKeys = 100_000
	defaultLimiterIdle = 3 * time.Minute
)

// Limiter is a token bucket per key: a client address, a user, a route. It is
// safe for concurrent use.
//
// The table of keys is bounded. A caller that arrives under a million forged
// keys fills it and then evicts its own oldest entries, so memory never grows
// without limit; the cost is that an evicted key starts again with a full
// bucket, which is why the bound should be far above the real number of keys.
type Limiter struct {
	rate  float64 // tokens per second
	burst float64
	idle  time.Duration
	max   int
	now   func() time.Time

	mu      sync.Mutex
	entries map[string]*list.Element
	order   *list.List // front = most recently used
}

type bucket struct {
	key    string
	tokens float64
	last   time.Time
}

// LimiterOption configures a Limiter.
type LimiterOption func(*Limiter)

// WithLimiterClock replaces the clock, so a test can move time by hand.
func WithLimiterClock(now func() time.Time) LimiterOption {
	return func(l *Limiter) {
		if now != nil {
			l.now = now
		}
	}
}

// NewLimiter returns a limiter that refills rate tokens per second up to burst.
// A key quiet for longer than idle starts again with a full bucket, so the table
// does not keep an entry for everyone who ever called. At most maxKeys keys are
// tracked. idle and maxKeys default to 3 minutes and 100,000 when not positive.
// A rate that is not positive never refills: a key gets its burst, then waits
// out idle.
func NewLimiter(rate float64, burst int, idle time.Duration, maxKeys int, opts ...LimiterOption) *Limiter {
	if idle <= 0 {
		idle = defaultLimiterIdle
	}
	if maxKeys <= 0 {
		maxKeys = defaultLimiterKeys
	}
	l := &Limiter{
		rate: rate, burst: float64(max(burst, 0)), idle: idle, max: maxKeys, now: time.Now,
		entries: make(map[string]*list.Element), order: list.New(),
	}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

// Allow takes one token for key. When there is none it returns how long until
// there will be, for a Retry-After header.
func (l *Limiter) Allow(key string) (ok bool, retryAfter time.Duration) {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	b := l.refill(key, now)
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	if l.rate <= 0 {
		return false, l.idle
	}
	return false, time.Duration((1 - b.tokens) / l.rate * float64(time.Second))
}

// Len is the number of keys currently tracked.
func (l *Limiter) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

// refill returns the key's bucket brought up to date, creating it (and evicting
// the least recently used key when the table is full) if it is new. l.mu is held.
func (l *Limiter) refill(key string, now time.Time) *bucket {
	if el, found := l.entries[key]; found {
		b := el.Value.(*bucket)
		l.order.MoveToFront(el)
		// A clock that went backwards must not take tokens away.
		elapsed := max(0, now.Sub(b.last))
		if elapsed > l.idle {
			b.tokens = l.burst
		} else {
			b.tokens = math.Min(l.burst, b.tokens+elapsed.Seconds()*l.rate)
		}
		b.last = now
		return b
	}
	if len(l.entries) >= l.max {
		if oldest := l.order.Back(); oldest != nil {
			delete(l.entries, oldest.Value.(*bucket).key)
			l.order.Remove(oldest)
		}
	}
	b := &bucket{key: key, tokens: l.burst, last: now}
	l.entries[key] = l.order.PushFront(b)
	return b
}
