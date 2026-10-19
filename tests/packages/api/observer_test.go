package api_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/api"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/tests/internal/obstest"
)

// No build tag: the middleware is exercised on an in-process Hertz engine, so
// it needs no network and no database.

func newObserver(t testing.TB) *observability.Service {
	t.Helper()
	return obstest.New(t)
}

// engine is a Hertz engine with the Observe middleware first, and a few routes.
func engine(o observability.IObserver) *server.Hertz {
	h := server.Default(server.WithHostPorts("127.0.0.1:0"))
	h.Use(api.Observe(o))
	h.GET("/members/:id", func(_ context.Context, c *app.RequestContext) { c.String(200, "ok "+c.Param("id")) })
	h.GET("/missing-thing", func(_ context.Context, c *app.RequestContext) { c.String(404, "no") })
	h.GET("/boom", func(_ context.Context, c *app.RequestContext) { c.String(500, "fail") })
	h.GET("/slow-down", func(_ context.Context, c *app.RequestContext) { c.String(429, "later") })
	h.GET("/panic", func(_ context.Context, _ *app.RequestContext) { panic("bad") })
	return h
}

func call(h *server.Hertz, path string) int {
	w := ut.PerformRequest(h.Engine, "GET", path, nil)
	return w.Result().StatusCode()
}

func apiCount(o *observability.Service, route, outcome string) float64 {
	return obstest.Count(o, "api", route, outcome)
}

// The operation is named by the route template, so a thousand member ids make one series.
func TestObserve_RouteTemplateNotConcretePath(t *testing.T) {
	t.Parallel()
	o := newObserver(t)
	h := engine(o)
	for i := range 200 {
		if got := call(h, fmt.Sprintf("/members/%d", i)); got != 200 {
			t.Fatalf("status = %d", got)
		}
	}
	if got := apiCount(o, "/members/:id", "success"); got != 200 {
		t.Fatalf("/members/:id success = %v, want 200", got)
	}
	n := 0
	for _, sm := range o.Snapshot() {
		if sm.Name == observability.MetricOperations && sm.Attrs[observability.KeyComponent] == "api" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("%d series for 200 different member ids, want 1", n)
	}
}

// A scanner asking for paths that do not exist is not an error of the API, and
// every unknown path is one series.
func TestObserve_UnmatchedPathsAreOneSeriesAndNotErrors(t *testing.T) {
	t.Parallel()
	o := newObserver(t)
	h := engine(o)
	for _, p := range []string{"/wp-admin", "/.env", "/etc/passwd", "/a/b/c"} {
		if got := call(h, p); got != 404 {
			t.Fatalf("%s status = %d, want 404", p, got)
		}
	}
	if got := apiCount(o, "unmatched", "success"); got != 4 {
		t.Fatalf("unmatched success = %v, want 4", got)
	}
	if _, ok := o.Find(observability.MetricOperations, map[string]string{observability.KeyOutcome: "error"}); ok {
		t.Fatal("unknown paths raised the error rate")
	}
}

// The status decides the outcome: 4xx is the API working, 429 is a retry, 5xx is an error.
func TestObserve_StatusMapsToOutcome(t *testing.T) {
	t.Parallel()
	o := newObserver(t)
	h := engine(o)
	for path, want := range map[string]int{"/missing-thing": 404, "/boom": 500, "/slow-down": 429} {
		if got := call(h, path); got != want {
			t.Fatalf("%s = %d, want %d", path, got, want)
		}
	}
	if got := apiCount(o, "/missing-thing", "success"); got != 1 {
		t.Errorf("a 404 = %v successes, want 1", got)
	}
	if got := apiCount(o, "/boom", "error"); got != 1 {
		t.Errorf("a 500 = %v errors, want 1", got)
	}
	if got := apiCount(o, "/slow-down", "retry"); got != 1 {
		t.Errorf("a 429 = %v retries, want 1", got)
	}
}

// A panic ends the operation as an error, nothing stays in flight, and the panic
// is raised again for the recovery middleware.
func TestObserve_APanicIsAnErrorAndIsNotSwallowed(t *testing.T) {
	t.Parallel()
	o := newObserver(t)
	h := engine(o) // server.Default installs Recovery, which turns the panic into a 500
	if got := call(h, "/panic"); got != 500 {
		t.Fatalf("a panicking handler = %d, want the recovery middleware's 500", got)
	}
	if got := apiCount(o, "/panic", "error"); got != 1 {
		t.Fatalf("panic counted as %v errors, want 1", got)
	}
	if sm, ok := o.Find(observability.MetricInflight, map[string]string{observability.KeyOperation: "/panic"}); !ok || sm.Value != 0 {
		t.Fatalf("inflight after a panic = %+v, want 0", sm)
	}
}

// The status class is not a metric label.
func TestObserve_StatusIsNeverALabel(t *testing.T) {
	t.Parallel()
	o := newObserver(t)
	h := engine(o)
	call(h, "/members/1")
	call(h, "/boom")
	for _, sm := range o.Snapshot() {
		for k := range sm.Attrs {
			if k == "http.response.status_code" || k == "http.response.status_class" || k == "http.request.method" {
				t.Fatalf("%s became a label: %+v", k, sm)
			}
		}
	}
}

// No observer: the middleware is a pass-through.
func TestObserve_NilObserverIsAPassThrough(t *testing.T) {
	t.Parallel()
	h := engine(nil)
	if got := call(h, "/members/9"); got != 200 {
		t.Fatalf("status = %d, want 200", got)
	}
	if got := call(h, "/boom"); got != 500 {
		t.Fatalf("status = %d, want 500", got)
	}
}
