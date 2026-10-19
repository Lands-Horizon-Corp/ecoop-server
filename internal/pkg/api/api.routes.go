package api

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

type routeEntry struct {
	route   Route
	handler app.HandlerFunc
	mid     []app.HandlerFunc
	stream  bool
}

// routeTable holds the registered routes until Start mounts them, so every route
// sits behind the same middleware whatever order it was registered in.
type routeTable struct {
	mu      sync.Mutex
	entries []routeEntry
	seen    map[string]bool
}

func (t *routeTable) add(e routeEntry) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := strings.ToUpper(e.route.Method) + " " + e.route.Route
	if t.seen == nil {
		t.seen = map[string]bool{}
	}
	if t.seen[key] {
		return fmt.Errorf("%w: %s", ErrDuplicateRoute, key)
	}
	t.seen[key] = true
	t.entries = append(t.entries, e)
	return nil
}

func (t *routeTable) mountAll(h *server.Hertz) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, e := range t.entries {
		handlers := append(append([]app.HandlerFunc{}, e.mid...), e.handler)
		h.Handle(strings.ToUpper(e.route.Method), e.route.Route, handlers...)
	}
}

func validateRoute(r Route) error {
	if !strings.HasPrefix(r.Route, "/") || strings.ContainsAny(r.Route, " \t\r\n") || len(r.Route) > 256 {
		return fmt.Errorf("%w: path %q", ErrInvalidRoute, utils.Truncate(r.Route, 64))
	}
	if !knownMethods[strings.ToUpper(r.Method)] {
		return fmt.Errorf("%w: method %q", ErrInvalidRoute, utils.Truncate(r.Method, 16))
	}
	for _, reserved := range []string{LivenessPath, ReadinessPath, inventoryPath, pprofPath} {
		if r.Route == reserved || strings.HasPrefix(r.Route, reserved+"/") {
			return fmt.Errorf("%w: %s is reserved", ErrInvalidRoute, reserved)
		}
	}
	return nil
}

// addRoute is what both registration paths share: routes are accepted only
// before Start, and only when valid and not already taken.
func (a *APIService) addRoute(e routeEntry) error {
	a.mu.Lock()
	started := a.started
	a.mu.Unlock()
	if started {
		return ErrAfterStart
	}
	if err := validateRoute(e.route); err != nil {
		return err
	}
	return a.routes.add(e)
}

// RegisterRoute registers a handler. Its error becomes a response through one
// place (see respondError), so a handler returns an error and never writes a
// client-visible message of its own. Routes are registered before Start.
func (a *APIService) RegisterRoute(route Route, callback func(ctx context.Context, c *app.RequestContext) error, mid ...app.HandlerFunc) error {
	if callback == nil {
		return ErrNilHandler
	}
	return a.addRoute(routeEntry{route: route, mid: mid, handler: func(ctx context.Context, c *app.RequestContext) {
		if err := callback(ctx, c); err != nil {
			a.respondError(c, err)
		}
	}})
}

// RegisterSSR registers a server-sent stream: newline-delimited JSON events. The
// callback calls sendUpdate for each event. After the first byte is written the
// status can no longer change, so a failure becomes one final generic event with
// the request id, never the error's text. The stream is bounded by StreamTimeout,
// stops when the client leaves, and has the same middleware as every other route.
func (a *APIService) RegisterSSR(route Route, callback func(ctx context.Context, c *app.RequestContext, sendUpdate func(status string, data any, message ...string)) error, mid ...app.HandlerFunc) error {
	if callback == nil {
		return ErrNilHandler
	}
	return a.addRoute(routeEntry{route: route, mid: mid, stream: true, handler: a.streamHandler(callback)})
}

func (a *APIService) respondError(c *app.RequestContext, err error) {
	var pe *PublicError
	if errors.As(err, &pe) && pe.Status >= 400 && pe.Status < 600 {
		reject(c, pe.Status, pe.Code)
		return
	}
	a.log.Error("handler failed", "request_id", requestIDOf(c), "route", c.FullPath(), "error_type", fmt.Sprintf("%T", err))
	reject(c, consts.StatusInternalServerError, CodeInternal)
}

// routeInventory lists the non-private routes. Private routes are never listed;
// the listing itself sits behind loopbackOnly, because the route list is
// documentation of the attack surface.
func (a *APIService) routeInventory() app.HandlerFunc {
	type entry struct {
		Method string `json:"method"`
		Path   string `json:"path"`
		Note   string `json:"note,omitempty"`
		Stream bool   `json:"stream,omitempty"`
	}
	return func(_ context.Context, c *app.RequestContext) {
		var out []entry
		a.routes.mu.Lock()
		for _, e := range a.routes.entries {
			if !e.route.Private {
				out = append(out, entry{Method: strings.ToUpper(e.route.Method), Path: e.route.Route, Note: e.route.Note, Stream: e.stream})
			}
		}
		a.routes.mu.Unlock()
		sort.Slice(out, func(i, j int) bool {
			if out[i].Path != out[j].Path {
				return out[i].Path < out[j].Path
			}
			return out[i].Method < out[j].Method
		})
		c.JSON(consts.StatusOK, map[string]any{"routes": out})
	}
}
