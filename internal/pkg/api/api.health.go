package api

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
)

// ObserveOption configures Observe.
type ObserveOption func(*observeConfig)

type observeConfig struct {
	trustTraceHeaders bool
	exposeTraceID     bool
	skip              map[string]bool
}

// TrustTraceHeaders makes Observe continue the trace named by the request's
// traceparent. Use it only for traffic that comes from a trusted hop (an
// internal gateway or another of our services), never for requests that come
// straight from the internet. Off by default.
func TrustTraceHeaders() ObserveOption { return func(c *observeConfig) { c.trustTraceHeaders = true } }

// ExposeTraceID adds a traceparent header to the response, so a user or a
// support agent can quote the trace of a failing request. Off by default.
func ExposeTraceID() ObserveOption { return func(c *observeConfig) { c.exposeTraceID = true } }

// SkipRoutes leaves the listed route templates out of the metrics and traces,
// for health probes and the metrics endpoint itself.
func SkipRoutes(routes ...string) ObserveOption {
	return func(c *observeConfig) {
		if c.skip == nil {
			c.skip = map[string]bool{}
		}
		for _, r := range routes {
			c.skip[r] = true
		}
	}
}

// Paths of the probes.
const (
	LivenessPath  = "/healthz"
	ReadinessPath = "/readyz"
)

// requestCarrier reads trace headers from a request. Only the W3C names are
// ever looked up, so no other header is exposed to the propagator.
type requestCarrier struct{ c *app.RequestContext }

func (r requestCarrier) Get(key string) string { return string(r.c.Request.Header.Peek(key)) }
func (r requestCarrier) Set(string, string)    {}

// responseCarrier writes trace headers to a response.
type responseCarrier struct{ c *app.RequestContext }

func (r responseCarrier) Get(key string) string { return string(r.c.Response.Header.Peek(key)) }
func (r responseCarrier) Set(key, value string) { r.c.Response.Header.Set(key, value) }

// Check is one dependency's readiness: it returns nil when the dependency
// answers, or an error. Its text is never sent to the client.
type Check struct {
	Name string
	Fn   func(ctx context.Context) error
}

// Liveness answers 200 while the process runs. It checks nothing else: a probe
// that fails because the database is down would make the orchestrator restart a
// healthy process, which does not bring the database back.
func Liveness() app.HandlerFunc {
	return func(_ context.Context, c *app.RequestContext) {
		c.Header("Cache-Control", "no-store")
		c.JSON(http.StatusOK, map[string]string{"status": "alive"})
	}
}

// Readiness answers 200 when every check passes and 503 when any fails, so the
// orchestrator stops sending traffic without restarting the process. The checks
// run concurrently under one timeout (5s if timeout is 0). The response names
// which check failed and says only "unhealthy": the error's text can carry an
// address, a user name or a driver message, and is returned to anyone who can
// reach the probe.
//
// Each check's result is also recorded as an operation of the observer, so
// readiness failures are visible in the same dashboards.
func Readiness(o observability.IObserver, timeout time.Duration, checks ...Check) app.HandlerFunc {
	o = observability.Pick(o)
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return func(ctx context.Context, c *app.RequestContext) {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		result := make(map[string]string, len(checks))
		var mu sync.Mutex
		var wg sync.WaitGroup
		for _, ck := range checks {
			wg.Go(func() {
				status := "healthy"
				// One operation per check, named by the developer's check name (never
				// by the request), so a failing dependency shows in the dashboards.
				cctx, op := o.Begin(ctx, observability.ComponentAPI, "ready/"+ck.Name)
				err := runCheck(cctx, ck)
				op.End(err)
				if err != nil {
					status = "unhealthy"
				}
				mu.Lock()
				result[ck.Name] = status
				mu.Unlock()
			})
		}
		wg.Wait()
		code, overall := http.StatusOK, "ready"
		for _, s := range result {
			if s != "healthy" {
				code, overall = http.StatusServiceUnavailable, "not_ready"
				break
			}
		}
		c.Header("Cache-Control", "no-store")
		c.JSON(code, map[string]any{"status": overall, "checks": result})
	}
}

// runCheck runs one check, turning a panic or a nil function into a failure.
func runCheck(ctx context.Context, ck Check) (err error) {
	defer func() {
		if recover() != nil {
			err = errCheckPanicked
		}
	}()
	if ck.Fn == nil {
		return errCheckPanicked
	}
	done := make(chan error, 1)
	go func() {
		defer func() {
			if recover() != nil {
				done <- errCheckPanicked
			}
		}()
		done <- ck.Fn(ctx)
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
