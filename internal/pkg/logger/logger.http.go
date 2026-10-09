package logger

import (
	"net/http"
	"time"

	"github.com/pusher/pusher-http-go/v5"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// InjectHTTPClient traces and measures every request made with c (HTTP and
// HTTPS) and forwards the trace to the callee. Use req.WithContext(ctx) or
// http.NewRequestWithContext so the call joins the current trace.
func (l *loggerServiceImpl) InjectHTTPClient(c *http.Client) error {
	if err := l.requireStarted(); err != nil {
		return err
	}
	if c == nil {
		return ErrNilTarget
	}
	if _, ok := c.Transport.(*otelhttp.Transport); ok {
		return nil // already instrumented
	}
	c.Transport = otelhttp.NewTransport(c.Transport) // nil falls back to http.DefaultTransport
	return nil
}

// InjectPusher instruments the Pusher (or Sockudo) client's HTTP calls.
// Pusher methods take no context, so these spans start their own traces.
func (l *loggerServiceImpl) InjectPusher(c *pusher.Client) error {
	if err := l.requireStarted(); err != nil {
		return err
	}
	if c == nil {
		return ErrNilTarget
	}
	if c.HTTPClient == nil {
		// Same default the pusher library would otherwise create lazily.
		c.HTTPClient = &http.Client{Timeout: 5 * time.Second}
	}
	return l.InjectHTTPClient(c.HTTPClient)
}
