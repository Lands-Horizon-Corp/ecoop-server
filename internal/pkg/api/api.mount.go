package api

import (
	"context"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/adaptor"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Paths the API reserves for itself. A route registered under one is refused.
const (
	inventoryPath = "/routes"
	pprofPath     = "/debug/pprof"
)

// mount applies the middleware in the order the security model needs, then the
// routes. The order is deliberate and tested:
//
//  1. request id        every later log line and rejection can name the request
//  2. security headers  set before anything can write, so no response lacks them
//  3. draining          once Stop has begun, new requests are refused
//  4. recovery          a panic below becomes a generic 500 that still has headers
//  5. observe           the request is one operation, whatever rejects it
//  6. method, host      refuse what should never reach the rest
//  7. https redirect    after host, so only an allowed host is redirected
//  8. suspicious paths  cheap, before any I/O
//  9. cors              answers preflights before they cost a rate-limit token
//  10. rate limit       per client address
//  11. body limit       a declared oversize is refused before a handler reads it
func (a *APIService) mount(h *server.Hertz) {
	cfg := a.cfg
	origins, hosts := allowlists(cfg)
	lim, shared := a.limiters()
	meter := a.obs.Meter(observability.ComponentAPI)
	suspicious := meter.Counter("api_suspicious_requests_total", "scanner paths answered 404")
	limited := meter.Counter("api_rate_limited_total", "requests refused by a rate limit")

	h.Use(
		requestID(),
		securityHeaders(cfg.Secured),
		a.draining(),
		recovery(a.log),
		Observe(a.obs, SkipRoutes(LivenessPath, ReadinessPath)),
		methodAllowlist(cfg.Methods),
		hostAllowlist(hosts, a.log),
		httpsRedirect(cfg.Secured, a.ip),
		blockSuspicious(cfg.BlockSuspiciousPaths, func() { suspicious.Add(context.Background(), 1) }),
		cors(origins),
		rateLimit(lim, shared, a.ip, func() { limited.Add(context.Background(), 1) }),
		bodyLimit(cfg.MaxBodyBytes),
	)

	h.GET(LivenessPath, Liveness())
	h.GET(ReadinessPath, Readiness(a.obs, 0, a.checks...))
	h.NoRoute(func(_ context.Context, c *app.RequestContext) { reject(c, consts.StatusNotFound, CodeNotFound) })
	h.NoMethod(func(_ context.Context, c *app.RequestContext) {
		reject(c, consts.StatusMethodNotAllowed, CodeBadMethod)
	})

	if cfg.EnablePprof {
		a.mountPprof(h)
	}
	a.routes.mountAll(h)
	h.GET(inventoryPath, a.loopbackOnly(), a.routeInventory())
}

// allowlists returns the origins and the Host values the API answers to, in
// canonical form. Validate has already accepted every entry, so none can fail
// here. With no Host list the hosts of the origins stand in, which is only a
// convenience for a single-host deployment.
func allowlists(cfg Config) (origins, hosts []string) {
	for _, o := range cfg.AllowedOrigins {
		n, _ := normalizeOrigin(o, cfg.Secured)
		origins = append(origins, n)
	}
	for _, h := range cfg.AllowedHosts {
		n, _ := normalizeHost(h)
		hosts = append(hosts, n)
	}
	if len(cfg.AllowedHosts) == 0 {
		for _, o := range origins {
			hosts = append(hosts, hostOf(o))
		}
	}
	return origins, hosts
}

// limiters builds the per-client limit: a local bucket, and when a cache is
// present a shared window as well. The window is one second wide, so the shared
// count means "per second per client", the same unit as the local bucket, and
// every replica sees the same number. Both are nil when no rate limit is set.
func (a *APIService) limiters() (*utils.Limiter, *sharedWindow) {
	cfg := a.cfg
	if cfg.RateLimit <= 0 {
		return nil, nil
	}
	local := utils.NewLimiter(cfg.RateLimit, cfg.RateBurst, cfg.RateWindow, maxRateKeys)
	if utils.IsNil(a.cache) {
		return local, nil
	}
	return local, &sharedWindow{
		cache: a.cache, prefix: "api:rl:", now: time.Now, window: time.Second,
		limit: int64(max(1, cfg.RateBurst)),
	}
}

// loopbackOnly answers 404 to everyone but a client on the loopback address, and
// to everyone when Secured: the profiling routes and the route inventory describe
// the attack surface, and a mistaken environment flag must not publish them. The
// peer address is the connection's, never a forwarded header, so a proxy cannot
// claim to be loopback.
func (a *APIService) loopbackOnly() app.HandlerFunc {
	secured := a.cfg.Secured
	return func(ctx context.Context, c *app.RequestContext) {
		peer, ok := peerAddr(c)
		if secured || !ok || !peer.IsLoopback() {
			reject(c, consts.StatusNotFound, CodeNotFound)
			return
		}
		c.Next(ctx)
	}
}

// mountPprof registers the profiling routes behind loopbackOnly. It is
// unreachable when Secured (Validate refuses the combination as well).
func (a *APIService) mountPprof(h *server.Hertz) {
	g := h.Group(pprofPath, a.loopbackOnly())
	serve := func(f http.HandlerFunc) app.HandlerFunc { return adaptor.HertzHandler(f) }
	g.GET("/", serve(pprof.Index))
	g.GET("/cmdline", serve(pprof.Cmdline))
	g.GET("/profile", serve(pprof.Profile))
	g.GET("/symbol", serve(pprof.Symbol))
	g.GET("/trace", serve(pprof.Trace))
	g.GET("/:name", func(ctx context.Context, c *app.RequestContext) {
		adaptor.HertzHandler(pprof.Handler(c.Param("name")))(ctx, c)
	})
}
