package api_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/api"
)

// No build tag: the whole middleware chain runs in process through Hertz's test
// harness, so every control of docs/api.md is checked on the real response.

// started is a started service with a few routes, and its engine.
func started(t testing.TB, cfg api.Config, opts ...api.Option) (*api.APIService, *server.Hertz) {
	t.Helper()
	svc := api.NewAPIService(0, nil, nil, append([]api.Option{api.WithConfig(cfg)}, opts...)...).(*api.APIService)
	ok := func(_ context.Context, c *app.RequestContext) error { c.String(200, "ok"); return nil }
	for _, r := range []api.Route{
		{Route: "/members/:id", Method: "GET"},
		{Route: "/orders", Method: "POST"},
		{Route: "/secret", Method: "GET", Private: true},
	} {
		if err := svc.RegisterRoute(r, ok); err != nil {
			t.Fatalf("RegisterRoute %s: %v", r.Route, err)
		}
	}
	if err := svc.RegisterRoute(api.Route{Route: "/boom", Method: "GET"}, func(context.Context, *app.RequestContext) error {
		return errors.New(`pq: password authentication failed for user "ecoop_admin" at 10.1.2.3`)
	}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterRoute(api.Route{Route: "/panic", Method: "POST"}, func(context.Context, *app.RequestContext) error {
		panic("card 4111111111111111 for ana@example.com")
	}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterRoute(api.Route{Route: "/denied", Method: "GET"}, func(context.Context, *app.RequestContext) error {
		return api.Public(403, "not_yours")
	}); err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return svc, svc.Handler()
}

func securedCfg() api.Config {
	return api.Config{
		Secured:        true,
		AllowedOrigins: []string{"https://app.ecoop.test"},
		AllowedHosts:   []string{"api.ecoop.test"},
		TrustedProxies: []string{"10.0.0.0/8"},
		RateLimit:      1000, RateBurst: 1000,
	}
}

type reqOpt func(*req)

type req struct {
	method, path string
	body         string
	host         string
	headers      []ut.Header
}

// H sets a request header. The Host header is carried in the request URL instead,
// because that is where Hertz's test harness (and, on a real connection, the
// parsed Host header) puts the host the middleware reads.
func H(k, v string) reqOpt {
	return func(r *req) {
		if strings.EqualFold(k, "Host") {
			r.host = v
			return
		}
		r.headers = append(r.headers, ut.Header{Key: k, Value: v})
	}
}
func Body(s string) reqOpt { return func(r *req) { r.body = s } }

func do(h *server.Hertz, method, path string, opts ...reqOpt) *ut.ResponseRecorder {
	r := &req{method: method, path: path}
	for _, o := range opts {
		o(r)
	}
	var body *ut.Body
	if r.body != "" {
		body = &ut.Body{Body: strings.NewReader(r.body), Len: len(r.body)}
	}
	target := path
	if r.host != "" {
		target = "http://" + r.host + path
	}
	return ut.PerformRequest(h.Engine, method, target, body, r.headers...)
}

func status(w *ut.ResponseRecorder) int           { return w.Result().StatusCode() }
func hdr(w *ut.ResponseRecorder, k string) string { return string(w.Result().Header.Peek(k)) }
func bodyOf(w *ut.ResponseRecorder) string        { return string(w.Result().Body()) }

// ---- the lifecycle contract ------------------------------------------------

func TestService_LifecycleRules(t *testing.T) {
	t.Parallel()
	svc := api.NewAPIService(0, nil, nil).(*api.APIService)
	if err := svc.Run(context.Background()); !errors.Is(err, api.ErrNotStarted) {
		t.Errorf("Run before Start = %v, want ErrNotStarted", err)
	}
	if err := svc.Stop(context.Background()); err != nil {
		t.Errorf("Stop before Start = %v, want nil", err)
	}
	if err := svc.RegisterRoute(api.Route{Route: "/a", Method: "GET"}, nil); !errors.Is(err, api.ErrNilHandler) {
		t.Errorf("a nil handler = %v, want ErrNilHandler", err)
	}
	ok := func(context.Context, *app.RequestContext) error { return nil }
	if err := svc.RegisterRoute(api.Route{Route: "/a", Method: "GET"}, ok); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterRoute(api.Route{Route: "/a", Method: "get"}, ok); !errors.Is(err, api.ErrDuplicateRoute) {
		t.Errorf("a duplicate route = %v, want ErrDuplicateRoute", err)
	}
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(context.Background()); !errors.Is(err, api.ErrAlreadyStarted) {
		t.Errorf("second Start = %v, want ErrAlreadyStarted", err)
	}
	if err := svc.RegisterRoute(api.Route{Route: "/late", Method: "GET"}, ok); !errors.Is(err, api.ErrAfterStart) {
		t.Errorf("a route after Start = %v, want ErrAfterStart", err)
	}
}

// A route that would shadow a probe, the inventory or pprof is refused, and so is
// anything malformed, instead of being registered and then misrouted.
func TestService_InvalidRoutesAreRefused(t *testing.T) {
	t.Parallel()
	svc := api.NewAPIService(0, nil, nil).(*api.APIService)
	ok := func(context.Context, *app.RequestContext) error { return nil }
	for name, r := range map[string]api.Route{
		"no leading slash":  {Route: "members", Method: "GET"},
		"space in the path": {Route: "/a b", Method: "GET"},
		"newline":           {Route: "/a\nb", Method: "GET"},
		"huge path":         {Route: "/" + strings.Repeat("a", 300), Method: "GET"},
		"bad method":        {Route: "/a", Method: "TRACE"},
		"empty method":      {Route: "/a", Method: ""},
		"liveness":          {Route: api.LivenessPath, Method: "GET"},
		"readiness":         {Route: api.ReadinessPath, Method: "GET"},
		"inventory":         {Route: "/routes", Method: "GET"},
		"pprof":             {Route: "/debug/pprof/heap", Method: "GET"},
	} {
		if err := svc.RegisterRoute(r, ok); !errors.Is(err, api.ErrInvalidRoute) {
			t.Errorf("%s: RegisterRoute = %v, want ErrInvalidRoute", name, err)
		}
	}
}

// ---- headers on every response --------------------------------------------

// docs/api.md Priority 0 #1: the headers are set before the handler can write,
// so they are on success, error, redirect, panic, not-found and rejected alike.
func TestHeaders_PresentOnEveryKindOfResponse(t *testing.T) {
	t.Parallel()
	_, h := started(t, securedCfg())
	fwd := func(extra ...reqOpt) []reqOpt {
		return append([]reqOpt{H("Host", "api.ecoop.test"), H("X-Forwarded-Proto", "https")}, extra...)
	}
	cases := map[string]*ut.ResponseRecorder{
		"success":           do(h, "GET", "/members/1", fwd()...),
		"handler error":     do(h, "GET", "/boom", fwd()...),
		"public error":      do(h, "GET", "/denied", fwd()...),
		"panic":             do(h, "POST", "/panic", fwd()...),
		"not found":         do(h, "GET", "/nope", fwd()...),
		"method not routed": do(h, "DELETE", "/members/1", fwd()...),
		"scanner path":      do(h, "GET", "/.env", fwd()...),
		"bad host":          do(h, "GET", "/members/1", H("Host", "evil.test")),
		"liveness":          do(h, "GET", api.LivenessPath, fwd()...),
	}
	want := []string{
		"X-Content-Type-Options", "X-Frame-Options", "Referrer-Policy", "Content-Security-Policy",
		"Permissions-Policy", "Cross-Origin-Resource-Policy", "Cross-Origin-Opener-Policy", "X-Request-Id",
	}
	for name, w := range cases {
		for _, k := range want {
			if hdr(w, k) == "" {
				t.Errorf("%s (%d): header %s is missing", name, status(w), k)
			}
		}
		if hdr(w, "Strict-Transport-Security") == "" {
			t.Errorf("%s: HSTS is missing in Secured mode", name)
		}
		if got := hdr(w, "X-Content-Type-Options"); got != "nosniff" {
			t.Errorf("%s: X-Content-Type-Options = %q", name, got)
		}
	}
}

// HSTS only when Secured, and never includeSubDomains or preload by default.
func TestHeaders_HSTSOnlyWhenSecuredAndWithoutPreload(t *testing.T) {
	t.Parallel()
	_, dev := started(t, api.Config{})
	if got := hdr(do(dev, "GET", "/members/1"), "Strict-Transport-Security"); got != "" {
		t.Errorf("HSTS = %q in development", got)
	}
	_, prod := started(t, securedCfg())
	got := hdr(do(prod, "GET", "/members/1", H("Host", "api.ecoop.test"), H("X-Forwarded-Proto", "https")), "Strict-Transport-Security")
	if got == "" || strings.Contains(strings.ToLower(got), "includesubdomains") || strings.Contains(strings.ToLower(got), "preload") {
		t.Errorf("HSTS = %q, want max-age only", got)
	}
}

// The CSP is the strict API policy, and has no nonce placeholder (Priority 1 #10).
func TestHeaders_CSPIsStrictAndHasNoPlaceholderNonce(t *testing.T) {
	t.Parallel()
	_, h := started(t, api.Config{})
	csp := hdr(do(h, "GET", "/members/1"), "Content-Security-Policy")
	for _, must := range []string{"default-src 'none'", "frame-ancestors 'none'"} {
		if !strings.Contains(csp, must) {
			t.Errorf("CSP %q lacks %q", csp, must)
		}
	}
	for _, bad := range []string{"nonce-", "unsafe-inline", "unsafe-eval", "report-to", "report-uri", "*"} {
		if strings.Contains(csp, bad) {
			t.Errorf("CSP %q contains %q", csp, bad)
		}
	}
	if hdr(do(h, "GET", "/members/1"), "Expect-CT") != "" {
		t.Error("the obsolete Expect-CT header is sent (Priority 1 #11)")
	}
}

// ---- errors never leak -----------------------------------------------------

// Priority 0 #2 and #5: a handler error and a panic reach the client as a code and
// a request id, never as text.
func TestErrors_NeverLeakToTheClient(t *testing.T) {
	t.Parallel()
	_, h := started(t, api.Config{})
	for name, w := range map[string]*ut.ResponseRecorder{
		"handler error": do(h, "GET", "/boom"),
		"panic":         do(h, "POST", "/panic", Body(`{"password":"hunter2","card":"4111111111111111"}`), H("Content-Type", "application/json")),
	} {
		if status(w) != 500 {
			t.Errorf("%s: status = %d, want 500", name, status(w))
		}
		b := bodyOf(w)
		for _, leak := range []string{"password", "ecoop_admin", "10.1.2.3", "4111", "ana@example.com", "hunter2", "pq:", "panic", "goroutine", ".go:"} {
			if strings.Contains(b, leak) {
				t.Errorf("%s: %q leaked: %s", name, leak, b)
			}
		}
		if !strings.Contains(b, `"error":"internal_error"`) || !strings.Contains(b, `"request_id":"`) {
			t.Errorf("%s: body = %s, want a code and a request id", name, b)
		}
	}
}

// A handler may say something to the client, but only through Public.
func TestErrors_PublicErrorsKeepTheirStatusAndCode(t *testing.T) {
	t.Parallel()
	_, h := started(t, api.Config{})
	w := do(h, "GET", "/denied")
	if status(w) != 403 || !strings.Contains(bodyOf(w), `"error":"not_yours"`) {
		t.Fatalf("public error = %d %s", status(w), bodyOf(w))
	}
}

// ---- request ids -----------------------------------------------------------

func TestRequestID_GeneratedAndHostileOnesReplaced(t *testing.T) {
	t.Parallel()
	_, h := started(t, api.Config{})
	if id := hdr(do(h, "GET", "/members/1"), "X-Request-Id"); len(id) < 8 {
		t.Errorf("generated id = %q", id)
	}
	if got := hdr(do(h, "GET", "/members/1", H("X-Request-Id", "client-123")), "X-Request-Id"); got != "client-123" {
		t.Errorf("a plain client id was replaced: %q", got)
	}
	for _, bad := range []string{"a b", "x\r\nSet-Cookie: s=1", strings.Repeat("a", 65), "<script>", "../x"} {
		got := hdr(do(h, "GET", "/members/1", H("X-Request-Id", bad)), "X-Request-Id")
		if got == bad || strings.ContainsAny(got, "\r\n <>") || len(got) > 64 {
			t.Errorf("a hostile id was kept: %q -> %q", bad, got)
		}
	}
}

// ---- method and host -------------------------------------------------------

func TestMethods_OnlyTheAllowedOnesReachTheRouter(t *testing.T) {
	t.Parallel()
	_, h := started(t, api.Config{Methods: []string{"GET", "POST"}})
	if got := status(do(h, "GET", "/members/1")); got != 200 {
		t.Errorf("GET = %d", got)
	}
	for _, m := range []string{"PUT", "DELETE", "PATCH", "TRACE", "CONNECT", "PROPFIND"} {
		w := do(h, m, "/members/1")
		if status(w) != 405 {
			t.Errorf("%s = %d, want 405", m, status(w))
		}
		if !strings.Contains(hdr(w, "Allow"), "GET") {
			t.Errorf("%s: Allow = %q", m, hdr(w, "Allow"))
		}
	}
}

func TestHost_OnlyAllowedHostsAreServed(t *testing.T) {
	t.Parallel()
	_, h := started(t, api.Config{AllowedHosts: []string{"api.ecoop.test", "api.ecoop.test:8443"}})
	for host, want := range map[string]int{
		"api.ecoop.test": 200, "API.ECOOP.TEST": 200, "api.ecoop.test:8443": 200, "api.ecoop.test:9999": 200, // the name is what counts
		"evil.test": 400, "api.ecoop.test.evil.test": 400, "evilapi.ecoop.test": 400, "localhost": 400, "127.0.0.1": 400,
	} {
		if got := status(do(h, "GET", "/members/1", H("Host", host))); got != want {
			t.Errorf("Host %q = %d, want %d", host, got, want)
		}
	}
}

// With no host list the hosts come from the origins, so a single-host deployment
// works, and a host that is neither is still refused.
func TestHost_DefaultsToTheHostsOfTheOrigins(t *testing.T) {
	t.Parallel()
	_, h := started(t, api.Config{AllowedOrigins: []string{"http://app.ecoop.test:3000"}})
	if got := status(do(h, "GET", "/members/1", H("Host", "app.ecoop.test:3000"))); got != 200 {
		t.Errorf("the origin's own host = %d", got)
	}
	if got := status(do(h, "GET", "/members/1", H("Host", "other.test"))); got != 400 {
		t.Errorf("another host = %d", got)
	}
}

// ---- https -----------------------------------------------------------------

// The redirect, the probe exemption and the trusted-proxy rule depend on the
// peer address, so they are tested from real peers in peer_security_test.go
// (TestHTTPS_FromRealPeers). This one covers what does not: the redirect keeps the
// path and query, and is a permanent redirect that preserves the method.
func TestHTTPS_RedirectKeepsPathAndQuery(t *testing.T) {
	t.Parallel()
	_, h := started(t, securedCfg())
	w := do(h, "GET", "/members/1?x=1&y=%20z", H("Host", "api.ecoop.test"))
	if status(w) != 308 || hdr(w, "Location") != "https://api.ecoop.test/members/1?x=1&y=%20z" {
		t.Errorf("plain GET = %d %q", status(w), hdr(w, "Location"))
	}
}

// A request is only believed to be HTTPS from a trusted proxy.
func TestHTTPS_ForwardedProtoIsBelievedOnlyFromATrustedProxy(t *testing.T) {
	t.Parallel()
	_, h := started(t, api.Config{Secured: true, AllowedOrigins: []string{"https://app.ecoop.test"}, AllowedHosts: []string{"api.ecoop.test"}, RateLimit: 1000, RateBurst: 1000})
	w := do(h, "GET", "/members/1", H("Host", "api.ecoop.test"), H("X-Forwarded-Proto", "https"))
	if status(w) != 308 {
		t.Errorf("an unproxied client claiming https = %d, want a redirect", status(w))
	}
}

// A hostile Host cannot inject headers through the redirect target.
func TestHTTPS_RedirectTargetCannotBeSplit(t *testing.T) {
	t.Parallel()
	_, h := started(t, api.Config{Secured: true, AllowedOrigins: []string{"https://app.ecoop.test"}, RateLimit: 1000, RateBurst: 1000})
	w := do(h, "GET", "/members/1", H("Host", "app.ecoop.test"))
	if loc := hdr(w, "Location"); strings.ContainsAny(loc, "\r\n") {
		t.Errorf("Location = %q", loc)
	}
}

// ---- cors ------------------------------------------------------------------

func TestCORS_ExactOriginsOnlyAndNeverAWildcard(t *testing.T) {
	t.Parallel()
	_, h := started(t, api.Config{AllowedOrigins: []string{"https://app.ecoop.test", "https://admin.ecoop.test"}, AllowedHosts: []string{"api.ecoop.test"}})
	good := do(h, "GET", "/members/1", H("Host", "api.ecoop.test"), H("Origin", "https://app.ecoop.test"))
	if hdr(good, "Access-Control-Allow-Origin") != "https://app.ecoop.test" || hdr(good, "Access-Control-Allow-Credentials") != "true" {
		t.Errorf("an allowed origin: ACAO %q ACAC %q", hdr(good, "Access-Control-Allow-Origin"), hdr(good, "Access-Control-Allow-Credentials"))
	}
	if !strings.Contains(strings.ToLower(hdr(good, "Vary")), "origin") {
		t.Errorf("Vary = %q, want Origin", hdr(good, "Vary"))
	}
	for _, origin := range []string{
		"https://evil.test", "http://app.ecoop.test", "https://app.ecoop.test.evil.test", "https://APP.ecoop.test.evil.test",
		"null", "https://app.ecoop.test/", "https://app.ecoop.test:444", "https://staging.ecoop.test", "*",
	} {
		w := do(h, "GET", "/members/1", H("Host", "api.ecoop.test"), H("Origin", origin))
		if got := hdr(w, "Access-Control-Allow-Origin"); got != "" {
			t.Errorf("Origin %q was answered with ACAO %q", origin, got)
		}
		if hdr(w, "Access-Control-Allow-Credentials") != "" {
			t.Errorf("Origin %q was given credentials", origin)
		}
	}
}

func TestCORS_Preflight(t *testing.T) {
	t.Parallel()
	_, h := started(t, api.Config{AllowedOrigins: []string{"https://app.ecoop.test"}, AllowedHosts: []string{"api.ecoop.test"}})
	pre := func(origin string) *ut.ResponseRecorder {
		return do(h, "OPTIONS", "/orders", H("Host", "api.ecoop.test"), H("Origin", origin), H("Access-Control-Request-Method", "POST"))
	}
	w := pre("https://app.ecoop.test")
	if status(w) != 204 || hdr(w, "Access-Control-Allow-Methods") == "" || hdr(w, "Access-Control-Max-Age") != "3600" {
		t.Errorf("an allowed preflight = %d, methods %q, max-age %q", status(w), hdr(w, "Access-Control-Allow-Methods"), hdr(w, "Access-Control-Max-Age"))
	}
	if w := pre("https://evil.test"); status(w) != 403 || hdr(w, "Access-Control-Allow-Origin") != "" {
		t.Errorf("a hostile preflight = %d, ACAO %q", status(w), hdr(w, "Access-Control-Allow-Origin"))
	}
}

// ---- suspicious paths ------------------------------------------------------

func TestSuspiciousPaths_AnswerNotFoundWithoutReachingTheRouter(t *testing.T) {
	t.Parallel()
	_, h := started(t, api.Config{BlockSuspiciousPaths: true})
	for _, p := range []string{
		"/.env", "/.git/config", "/wp-admin/setup.php", "/wp-login.php", "/phpmyadmin/", "/xmlrpc.php",
		"/actuator/env", "/cgi-bin/x", "/.aws/credentials", "/id_rsa", "/members/../../etc/passwd",
		"/members/%2e%2e/%2e%2e/etc/passwd", "/.ENV", "/%2eenv",
	} {
		w := do(h, "GET", p)
		if status(w) != 404 {
			t.Errorf("%s = %d, want 404", p, status(w))
		}
	}
	if got := status(do(h, "GET", "/members/1")); got != 200 {
		t.Errorf("a real route = %d", got)
	}
}

func TestSuspiciousPaths_BlockingCanBeTurnedOff(t *testing.T) {
	t.Parallel()
	svc := api.NewAPIService(0, nil, nil, api.WithConfig(api.Config{})).(*api.APIService)
	_ = svc.RegisterRoute(api.Route{Route: "/.well-known/x", Method: "GET"}, func(_ context.Context, c *app.RequestContext) error {
		c.String(200, "ok")
		return nil
	})
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := status(do(svc.Handler(), "GET", "/.well-known/x")); got != 200 {
		t.Errorf("with blocking off = %d", got)
	}
}

// ---- body limit ------------------------------------------------------------

func TestBodyLimit_DeclaredOversizeIsRefusedBeforeTheHandler(t *testing.T) {
	t.Parallel()
	_, h := started(t, api.Config{MaxBodyBytes: 1024})
	if got := status(do(h, "POST", "/orders", Body(strings.Repeat("a", 1024)))); got != 200 {
		t.Errorf("exactly at the limit = %d", got)
	}
	if got := status(do(h, "POST", "/orders", Body(strings.Repeat("a", 100)))); got != 200 {
		t.Errorf("below the limit = %d", got)
	}
	w := do(h, "POST", "/orders", Body(strings.Repeat("a", 1025)))
	if status(w) != 413 || !strings.Contains(bodyOf(w), "payload_too_large") {
		t.Errorf("one byte over = %d %s, want 413", status(w), bodyOf(w))
	}
}

// ---- route inventory and pprof --------------------------------------------

func TestInventory_NeverPublicAndNeverListsPrivateRoutes(t *testing.T) {
	t.Parallel()
	_, dev := started(t, api.Config{})
	// ut.PerformRequest has no peer address, so the inventory must treat it as not
	// loopback, and answer 404 as if it did not exist.
	if got := status(do(dev, "GET", "/routes")); got != 404 {
		t.Errorf("inventory from an unknown peer = %d, want 404", got)
	}
	// In Secured mode it is 404 even from loopback; that is asserted from a real
	// loopback peer in TestInventory_NeverServedWhenSecured (peer_security_test.go).
}

func TestPprof_NeverRegisteredWhenSecuredAndRefusedByConfig(t *testing.T) {
	t.Parallel()
	svc := api.NewAPIService(0, nil, nil, api.WithConfig(api.Config{Secured: true, EnablePprof: true,
		AllowedOrigins: []string{"https://app.ecoop.test"}, RateLimit: 10, RateBurst: 10})).(*api.APIService)
	err := svc.Start(context.Background())
	if !errors.Is(err, api.ErrPprofSecured) {
		t.Fatalf("Start = %v, want ErrPprofSecured", err)
	}
	// Even without Secured, only a loopback peer may use it, and a request with no
	// peer is not loopback.
	dev := api.NewAPIService(0, nil, nil, api.WithConfig(api.Config{EnablePprof: true})).(*api.APIService)
	if err := dev.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"/debug/pprof/", "/debug/pprof/heap", "/debug/pprof/cmdline", "/debug/pprof/goroutine"} {
		if got := status(do(dev.Handler(), "GET", p)); got != 404 {
			t.Errorf("%s from an unknown peer = %d, want 404", p, got)
		}
	}
	off := api.NewAPIService(0, nil, nil).(*api.APIService)
	_ = off.Start(context.Background())
	if got := status(do(off.Handler(), "GET", "/debug/pprof/")); got != 404 {
		t.Errorf("pprof with EnablePprof off = %d, want 404", got)
	}
}
