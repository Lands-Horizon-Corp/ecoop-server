package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/cloudwego/hertz/pkg/protocol/http1/resp"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Stream limits.
const (
	// MaxStreamEventBytes caps one event. A larger one is replaced by a generic
	// failure event, so a handler cannot make the server buffer without bound.
	MaxStreamEventBytes = 256 << 10
	// MaxStreamEvents caps the events of one stream.
	MaxStreamEvents = 100_000
	// StreamContentType is newline-delimited JSON: one object per line.
	StreamContentType = "application/x-ndjson"
)

// Public event statuses. A client sees one of these and nothing else about a
// failure.
const (
	StatusStreamFailed = "failed"
	StatusStreamDone   = "done"
	StatusStreamLimit  = "limit"
	// StatusStreamClosing ends a stream because the server is shutting down.
	StatusStreamClosing = "closing"
)

// streamEvent is one line of the stream.
type streamEvent struct {
	Status    string   `json:"status"`
	Data      any      `json:"data,omitempty"`
	Message   []string `json:"message,omitempty"`
	RequestID string   `json:"request_id,omitempty"`
}

// streamHandler builds the handler of a RegisterSSR route. It is a plain
// app.HandlerFunc and sits behind the same middleware as every other route:
// rate limit, host, CORS, the security headers (already set, because they are
// set before the handler runs), and the route's own authentication middleware.
//
// Every response byte is written by this goroutine before the handler returns,
// through Hertz's chunked writer; nothing touches the request context after
// that, which is the one rule that makes streaming safe in a pooled-context
// server.
func (a *APIService) streamHandler(callback func(ctx context.Context, c *app.RequestContext, sendUpdate func(status string, data any, message ...string)) error) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		// One slot per open stream, shared by every stream route, so MaxStreams caps
		// the whole server and not each route separately.
		select {
		case a.slots <- struct{}{}:
			defer func() { <-a.slots }()
		default:
			c.Header("Retry-After", "5")
			reject(c, consts.StatusServiceUnavailable, CodeTooManyStreams)
			return
		}

		// The stream ends at StreamTimeout, or when the server is shutting down
		// (ctx), whichever is first. A client that leaves is noticed on the next
		// write, which fails.
		sctx, cancel := context.WithTimeout(ctx, a.cfg.StreamTimeout)
		defer cancel()
		defer context.AfterFunc(a.drain, cancel)()

		// Headers go out with the first chunk. They are set now, on top of the ones
		// the middleware already set, and the status is final from here: a failure
		// later can only be an event, never a status code.
		c.Response.Header.SetContentType(StreamContentType)
		c.Response.Header.Set("Cache-Control", "no-store")
		c.Response.Header.Set("X-Accel-Buffering", "no") // a proxy must not hold the stream back
		c.SetStatusCode(consts.StatusOK)
		w := resp.NewChunkedBodyWriter(&c.Response, c.GetWriter())
		c.Response.HijackWriter(w)

		var (
			mu     sync.Mutex
			events int
			broken bool // a write failed: the client is gone
			closed bool // the final event has been sent
		)
		write := func(ev streamEvent) bool {
			mu.Lock()
			defer mu.Unlock()
			if broken || closed {
				return false
			}
			if events >= MaxStreamEvents {
				return false
			}
			line, err := json.Marshal(ev)
			if err != nil || len(line) > MaxStreamEventBytes {
				// An event that cannot be encoded, or is too big, is not sent as it
				// is: what it contained is not for the client to learn from an error.
				line, _ = json.Marshal(streamEvent{Status: StatusStreamFailed, RequestID: requestIDOf(c)})
			}
			line = append(line, '\n')
			if _, err := w.Write(line); err != nil {
				broken = true
				cancel() // the client is gone: stop the handler's work
				return false
			}
			if err := w.Flush(); err != nil {
				broken = true
				cancel()
				return false
			}
			events++
			return true
		}
		send := func(status string, data any, message ...string) {
			if sctx.Err() != nil {
				return
			}
			write(streamEvent{Status: status, Data: data, Message: cleanMessages(message)})
		}

		err := runStream(sctx, callback, c, send)

		// The closing event. Whatever happened, the client gets one line that says
		// how the stream ended, with the request id to quote, and no detail.
		switch {
		case err == nil && sctx.Err() == nil:
			write(streamEvent{Status: StatusStreamDone, RequestID: requestIDOf(c)})
		case errors.Is(sctx.Err(), context.DeadlineExceeded):
			write(streamEvent{Status: StatusStreamLimit, RequestID: requestIDOf(c)})
		case a.drain.Err() != nil:
			write(streamEvent{Status: StatusStreamClosing, RequestID: requestIDOf(c)})
		default:
			if pe, ok := errors.AsType[*PublicError](err); ok {
				write(streamEvent{Status: StatusStreamFailed, Message: []string{pe.Code}, RequestID: requestIDOf(c)})
			} else {
				a.log.Error("stream failed", "request_id", requestIDOf(c), "route", c.FullPath(), "error_type", fmt.Sprintf("%T", err))
				write(streamEvent{Status: StatusStreamFailed, RequestID: requestIDOf(c)})
			}
		}
		mu.Lock()
		closed = true
		mu.Unlock()
	}
}

// runStream calls the callback and turns a panic into an error: after the first
// byte there is no middleware left that can still answer 500, and a panic that
// escaped would drop the connection mid-response.
func runStream(ctx context.Context, cb func(context.Context, *app.RequestContext, func(string, any, ...string)) error, c *app.RequestContext, send func(string, any, ...string)) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("stream handler panicked: %T", r) // the type only
		}
	}()
	return cb(ctx, c, send)
}

// cleanMessages bounds and cleans the human messages a handler attaches to an
// event: at most four, each cut to 200 bytes, with control characters removed.
// A handler is trusted to write a message that is safe to show; this makes sure
// a careless one cannot send a log line's worth of text or forge a new event.
func cleanMessages(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	if len(in) > 4 {
		in = in[:4]
	}
	out := make([]string, 0, len(in))
	for _, m := range in {
		out = append(out, strings.TrimSpace(utils.Truncate(utils.StripControl(m), 200)))
	}
	return out
}
