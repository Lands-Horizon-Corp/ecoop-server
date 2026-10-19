package api

import (
	"context"
	"math"
	"strconv"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// sharedWindowTimeout bounds one call to the cache from the request path.
const sharedWindowTimeout = 250 * time.Millisecond

// sharedWindow is the cache-backed counter that makes a limit hold across
// replicas: one fixed window per client, counted with INCR so two replicas see
// the same number.
type sharedWindow struct {
	cache  cache.ICacheService
	prefix string
	limit  int64
	window time.Duration
	now    func() time.Time
}

// allow counts one request. A cache error is returned to the caller, which
// decides whether to fail open or closed; it is never swallowed here.
func (s *sharedWindow) allow(ctx context.Context, key string) (bool, time.Duration, error) {
	// The cache is on every request's path, so a slow one must cost a request a
	// fraction of a second at most, after which the local limit alone applies.
	ctx, cancel := context.WithTimeout(ctx, sharedWindowTimeout)
	defer cancel()
	now := s.now()
	slot := now.Unix() / int64(s.window/time.Second)
	k := s.prefix + key + ":" + strconv.FormatInt(slot, 10)
	n, err := s.cache.Incr(ctx, k, s.window+time.Second)
	if err != nil {
		return false, 0, err
	}
	if n > s.limit {
		reset := time.Unix((slot+1)*int64(s.window/time.Second), 0)
		return false, max(time.Second, reset.Sub(now)), nil
	}
	return true, 0, nil
}

// rateLimit applies the per-client limit to every request. The local bucket
// decides first and costs nothing; when a shared window is configured it adds a
// limit that all replicas agree on. If the cache is down the local bucket still
// holds, which is a deliberate fail-safe: a Redis outage must not switch the
// protection off, and must not take the API down with it. onLimited is called
// once for each request refused.
func rateLimit(l *utils.Limiter, shared *sharedWindow, ip *clientIP, onLimited func()) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if l == nil {
			c.Next(ctx)
			return
		}
		key := ip.of(c)
		if key == "" {
			key = "unknown" // one shared, strict bucket for what cannot be placed
		}
		if ok, wait := l.Allow(key); !ok {
			tooMany(c, wait, onLimited)
			return
		}
		if shared != nil {
			if ok, wait, err := shared.allow(ctx, key); err == nil && !ok {
				tooMany(c, wait, onLimited)
				return
			}
		}
		c.Next(ctx)
	}
}

func tooMany(c *app.RequestContext, wait time.Duration, onLimited func()) {
	if onLimited != nil {
		onLimited()
	}
	c.Header("Retry-After", strconv.Itoa(max(1, int(math.Ceil(wait.Seconds())))))
	reject(c, consts.StatusTooManyRequests, CodeTooManyRequests)
}

// Keyed returns a middleware that limits by an application key (a user id from
// the session, a tenant, an endpoint), for the flows that IP limits do not
// protect: login, OTP, password reset, exports, money movement. keyFn returns
// "" for a request it cannot key, which is limited as one shared anonymous key.
func Keyed(rate float64, burst int, keyFn func(c *app.RequestContext) string) app.HandlerFunc {
	l := utils.NewLimiter(rate, burst, DefaultRateWindow, maxRateKeys)
	return func(ctx context.Context, c *app.RequestContext) {
		k := keyFn(c)
		if k == "" {
			k = "anonymous"
		}
		if ok, wait := l.Allow(k); !ok {
			tooMany(c, wait, nil)
			return
		}
		c.Next(ctx)
	}
}
