package api

import (
	"context"
	"log/slog"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
)

// unmatchedRoute is the operation name of a request no route matched. It is one
// fixed value on purpose: the raw path of an unknown request is whatever a
// scanner sends, and must never become a metric series.
const unmatchedRoute = "unmatched"

// Observe is the Hertz middleware that makes every request one Operation of the
// observer (component api), named by the route template ("/members/:id", never
// the concrete path), and ended with the response's outcome. Register it first,
// so it also sees what the other middleware reject.
//
// The outcome follows the status: 5xx is an error; a rate limit (429) is a
// retry; every other status, including 4xx, is a success of the API (the client
// asked for something that cannot be done, which is the API working), so a
// scanner cannot raise the error rate. The status class is a span attribute,
// never a label. A nil observer observes nothing.
//
// A panic in a later handler is not swallowed here: it ends the operation as an
// error and is raised again for the recovery middleware.
//
// Options: TrustTraceHeaders, ExposeTraceID, SkipRoutes.
func Observe(o observability.IObserver, opts ...ObserveOption) app.HandlerFunc {
	o = observability.Pick(o)
	var cfg observeConfig
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return func(ctx context.Context, c *app.RequestContext) {
		route := c.FullPath()
		if route == "" {
			route = unmatchedRoute
		}
		// A probe fires every few seconds from every node: counted, it would be
		// most of what the API reports and bury the traffic worth seeing.
		if cfg.skip[route] {
			c.Next(ctx)
			return
		}
		// The trace is continued from the request only when the caller is trusted
		// to start one (a gateway or another of our services). A request from the
		// internet can send any traceparent: honored, it lets a stranger choose
		// the trace id, force sampling on, or attach their traffic to someone
		// else's trace.
		if cfg.trustTraceHeaders {
			ctx = observability.Extract(ctx, o, requestCarrier{c})
		}
		ctx, op := o.Begin(ctx, observability.ComponentAPI, route,
			slog.String("http.request.method", string(c.Method())))
		if cfg.exposeTraceID {
			// Lets a user quote the id when reporting a problem; it is a response
			// header only and never an input.
			observability.Inject(ctx, o, responseCarrier{c})
		}
		panicked := true
		defer func() {
			status := c.Response.StatusCode()
			op.SetAttributes(
				slog.Int("http.response.status_code", status),
				slog.String("http.response.status_class", strconv.Itoa(status/100)+"xx"),
			)
			switch {
			case panicked, status >= 500:
				op.End(errServerFault)
			case status == 429:
				op.SetOutcome(observability.OutcomeRetry)
				op.End(nil)
			default:
				op.End(nil)
			}
		}()
		c.Next(ctx)
		panicked = false
	}
}
