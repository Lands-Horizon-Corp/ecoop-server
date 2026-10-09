package logger

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/client"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network"
	"github.com/cloudwego/hertz/pkg/protocol"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

var httpDurationBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10}

// InjectHertz must be called before routes are registered, since Hertz only
// applies middleware to routes added after Use. Works for HTTP and HTTPS
// (direct TLS or TLS terminated by a proxy that sets X-Forwarded-Proto).
func (l *loggerServiceImpl) InjectHertz(h *server.Hertz) error {
	if err := l.requireStarted(); err != nil {
		return err
	}
	if h == nil {
		return ErrNilTarget
	}

	duration, err := l.state.meter.Float64Histogram("http.server.request.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of HTTP server requests."),
		metric.WithExplicitBucketBoundaries(httpDurationBuckets...),
	)
	if err != nil {
		return fmt.Errorf("logger: http.server.request.duration: %w", err)
	}
	active, err := l.state.meter.Int64UpDownCounter("http.server.active_requests",
		metric.WithUnit("{request}"),
		metric.WithDescription("Number of in-flight HTTP server requests."),
	)
	if err != nil {
		return fmt.Errorf("logger: http.server.active_requests: %w", err)
	}

	// Hertz's built-in tracer hook can only be set at construction, so spans are created here instead.
	propagator := otel.GetTextMapPropagator()
	h.Use(func(ctx context.Context, c *app.RequestContext) {
		start := time.Now()
		method := string(c.Method())
		scheme := requestScheme(c)
		activeSet := metric.WithAttributes(semconv.HTTPRequestMethodKey.String(method), semconv.URLScheme(scheme))
		active.Add(ctx, 1, activeSet)
		defer active.Add(ctx, -1, activeSet)

		ctx = propagator.Extract(ctx, hertzHeaderCarrier{&c.Request.Header})
		ctx, span := l.state.tracer.Start(ctx, method,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				semconv.HTTPRequestMethodKey.String(method),
				semconv.URLScheme(scheme),
				semconv.URLPath(string(c.Path())),
				semconv.ServerAddress(string(c.Host())),
				semconv.ClientAddress(c.ClientIP()),
				semconv.UserAgentOriginal(string(c.UserAgent())),
				semconv.HTTPRequestBodySize(c.Request.Header.ContentLength()),
			),
		)

		finish := func(status int) {
			route := c.FullPath()
			if route != "" {
				span.SetName(method + " " + route)
			}
			span.SetAttributes(
				semconv.HTTPRoute(route),
				semconv.HTTPResponseStatusCode(status),
				semconv.HTTPResponseBodySize(c.Response.Header.ContentLength()),
			)
			if status >= 500 {
				span.SetStatus(codes.Error, fmt.Sprintf("HTTP %d", status))
			}
			// Route (not raw path) keeps label cardinality bounded.
			duration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(
				semconv.HTTPRequestMethodKey.String(method),
				semconv.URLScheme(scheme),
				semconv.HTTPRoute(route),
				semconv.HTTPResponseStatusCode(status),
			))
			span.End()
		}

		// Record panics on the span, then let Hertz's recovery middleware answer the request.
		defer func() {
			if r := recover(); r != nil {
				span.RecordError(fmt.Errorf("panic: %v", r), trace.WithStackTrace(true))
				span.SetStatus(codes.Error, "panic")
				finish(500)
				panic(r)
			}
		}()

		c.Next(ctx)

		for _, e := range c.Errors {
			span.RecordError(e.Err)
		}
		finish(c.Response.StatusCode())
	})
	return nil
}

// InjectHertzClient traces and measures outgoing calls made with a Hertz client
// and forwards the trace to the callee. For https:// targets the client needs
// a TLS-capable dialer: client.NewClient(client.WithDialer(standard.NewDialer())).
func (l *loggerServiceImpl) InjectHertzClient(c *client.Client) error {
	if err := l.requireStarted(); err != nil {
		return err
	}
	if c == nil {
		return ErrNilTarget
	}

	duration, err := l.state.meter.Float64Histogram("http.client.request.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of HTTP client requests."),
		metric.WithExplicitBucketBoundaries(httpDurationBuckets...),
	)
	if err != nil {
		return fmt.Errorf("logger: http.client.request.duration: %w", err)
	}

	propagator := otel.GetTextMapPropagator()
	c.Use(func(next client.Endpoint) client.Endpoint {
		return func(ctx context.Context, req *protocol.Request, resp *protocol.Response) error {
			start := time.Now()
			method := string(req.Method())
			uri := req.URI()
			scheme, host := string(uri.Scheme()), string(uri.Host())

			ctx, span := l.state.tracer.Start(ctx, method,
				trace.WithSpanKind(trace.SpanKindClient),
				trace.WithAttributes(
					semconv.HTTPRequestMethodKey.String(method),
					semconv.URLScheme(scheme),
					// Query strings are left out: they often carry tokens.
					semconv.URLFull(scheme+"://"+host+string(uri.Path())),
					semconv.ServerAddress(host),
				),
			)
			defer span.End()
			propagator.Inject(ctx, hertzHeaderCarrier{&req.Header})

			err := next(ctx, req, resp)

			status := resp.StatusCode()
			attrs := []attribute.KeyValue{
				semconv.HTTPRequestMethodKey.String(method),
				semconv.URLScheme(scheme),
				semconv.ServerAddress(host),
			}
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				attrs = append(attrs, semconv.ErrorTypeKey.String(fmt.Sprintf("%T", err)))
			} else {
				span.SetAttributes(semconv.HTTPResponseStatusCode(status))
				attrs = append(attrs, semconv.HTTPResponseStatusCode(status))
				if status >= 400 {
					span.SetStatus(codes.Error, fmt.Sprintf("HTTP %d", status))
				}
			}
			duration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(attrs...))
			return err
		}
	})
	return nil
}

// requestScheme reports https for direct TLS connections and for proxies
// (Railway, load balancers) that terminate TLS and set X-Forwarded-Proto.
func requestScheme(c *app.RequestContext) string {
	if proto := c.Request.Header.Get("X-Forwarded-Proto"); proto != "" {
		return strings.ToLower(strings.TrimSpace(strings.Split(proto, ",")[0]))
	}
	if _, ok := c.GetConn().(network.ConnTLSer); ok {
		return "https"
	}
	if s := string(c.Request.URI().Scheme()); s != "" {
		return s
	}
	return "http"
}

type hertzHeaderCarrier struct {
	h *protocol.RequestHeader
}

func (c hertzHeaderCarrier) Get(key string) string { return string(c.h.Peek(key)) }

func (c hertzHeaderCarrier) Set(key, value string) { c.h.Set(key, value) }

func (c hertzHeaderCarrier) Keys() []string {
	var keys []string
	c.h.VisitAll(func(k, _ []byte) { keys = append(keys, string(k)) })
	return keys
}
