package api_test

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/netip"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"unicode/utf8"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/api"
)

// Hostile input against the whole middleware chain: bypass attempts, injection
// strings, header splitting, the shape of every rejection, and fuzzing. Nothing
// here needs a socket or a database.

const peerIP = "203.0.113.9" // a public client

func mustRegister(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("RegisterRoute: %v", err)
	}
}

// devCfg is a development profile with a host and an origin, so the host and
// CORS controls are active without Secured's redirects getting in the way.
func devCfg() api.Config {
	return api.Config{
		AllowedOrigins: []string{"https://app.ecoop.test"},
		AllowedHosts:   []string{"api.ecoop.test", "app.ecoop.test"},
	}
}

// ---- route middleware is not bypassable ------------------------------------

// guard is a service whose protected routes have a deny-all middleware, standing
// in for authentication. Whatever the request looks like, a protected handler
// must run only after that middleware let it through, and here it never does.
type guard struct {
	h         *server.Hertz
	adminHits atomic.Int32
	openHits  atomic.Int32
	denyCalls atomic.Int32
}

func guarded(t testing.TB, cfg api.Config) *guard {
	t.Helper()
	g := &guard{}
	svc := api.NewAPIService(0, nil, nil, api.WithConfig(cfg)).(*api.APIService)
	deny := func(_ context.Context, c *app.RequestContext) {
		g.denyCalls.Add(1)
		c.AbortWithStatusJSON(401, map[string]string{"error": "unauthorized"})
	}
	admin := func(_ context.Context, c *app.RequestContext) error {
		g.adminHits.Add(1)
		c.String(200, "secret data")
		return nil
	}
	mustRegister(t, svc.RegisterRoute(api.Route{Route: "/admin/users/:id", Method: "GET"}, admin, deny))
	mustRegister(t, svc.RegisterRoute(api.Route{Route: "/admin/users/:id", Method: "DELETE"}, admin, deny))
	mustRegister(t, svc.RegisterRoute(api.Route{Route: "/open", Method: "GET"}, func(_ context.Context, c *app.RequestContext) error {
		g.openHits.Add(1)
		c.String(200, "open")
		return nil
	}))
	mustRegister(t, svc.RegisterRoute(api.Route{Route: "/broken-auth", Method: "GET"}, admin, func(context.Context, *app.RequestContext) {
		panic("token abc123 failed to verify")
	}))
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	g.h = svc.Handler()
	return g
}

// A protected route cannot be reached by writing its path another way: a
// trailing slash, another case, doubled slashes, dot segments, encoding, a
// matrix parameter, a NUL, a fragment or query that names another route.
func TestBypass_PathVariantsNeverSkipRouteMiddleware(t *testing.T) {
	t.Parallel()
	g := guarded(t, devCfg())
	if code, _, _ := as(g.h, peerIP, "GET", "/admin/users/1", nil); code != 401 {
		t.Fatalf("the protected route answered %d, want 401", code)
	}
	for _, p := range []string{
		"/admin/users/1/", "/ADMIN/users/1", "/Admin/Users/1", "//admin/users/1", "/admin//users/1",
		"/admin/users/1/.", "/admin/./users/1", "/admin/users/../users/1", "/admin/users/%31",
		"/admin%2Fusers/1", "/admin/users/1;x=1", "/admin/users/1%00", "/admin/users/1?x=/open",
		"/open/../admin/users/1", "/admin/users/1%2F", "/%61dmin/users/1", "/admin/users/1%20",
		"/admin/users/1%2e", "/admin/users", "/admin/users/", "/admin/users/1/extra",
	} {
		_, _, body := as(g.h, peerIP, "GET", p, nil)
		if strings.Contains(body, "secret data") {
			t.Errorf("%q reached the protected handler", p)
		}
	}
	if n := g.adminHits.Load(); n != 0 {
		t.Errorf("the protected handler ran %d times without passing its middleware", n)
	}
}

// A method-override header must not turn one request into another. The router
// takes its method from the request line only.
func TestBypass_MethodOverrideHeadersAreIgnored(t *testing.T) {
	t.Parallel()
	g := guarded(t, devCfg())
	for _, name := range []string{"X-HTTP-Method-Override", "X-Method-Override", "X-HTTP-Method", "_method"} {
		for _, via := range []string{"GET", "POST", "PUT", "HEAD", "OPTIONS"} {
			_, _, body := as(g.h, peerIP, via, "/admin/users/1", map[string]string{name: "GET"})
			if strings.Contains(body, "secret data") && via != "GET" {
				t.Errorf("%s: %s with %s reached the handler", name, via, via)
			}
		}
	}
	// A GET with an override naming a destructive method still runs as a GET.
	as(g.h, peerIP, "GET", "/open", map[string]string{"X-HTTP-Method-Override": "DELETE"})
	if g.openHits.Load() != 1 || g.adminHits.Load() != 0 {
		t.Errorf("an override changed what ran: open=%d admin=%d", g.openHits.Load(), g.adminHits.Load())
	}
}

// Methods that were never registered for a protected path answer 404/405 and
// never run its handler.
func TestBypass_UnregisteredMethodsNeverReachAProtectedHandler(t *testing.T) {
	t.Parallel()
	g := guarded(t, devCfg())
	for _, m := range []string{"POST", "PUT", "PATCH", "HEAD", "OPTIONS", "TRACE", "CONNECT", "PROPFIND", "get"} {
		code, _, body := as(g.h, peerIP, m, "/admin/users/1", nil)
		if code == 200 || strings.Contains(body, "secret data") {
			t.Errorf("%s answered %d", m, code)
		}
	}
	if g.adminHits.Load() != 0 {
		t.Errorf("the protected handler ran for a method it was not registered for")
	}
}

// A route middleware that panics fails closed: the client sees a generic 500,
// the handler behind it does not run, and the panic text goes nowhere.
func TestRouteMiddleware_PanicFailsClosed(t *testing.T) {
	t.Parallel()
	g := guarded(t, devCfg())
	code, h, body := as(g.h, peerIP, "GET", "/broken-auth", nil)
	if code != 500 {
		t.Errorf("status = %d, want 500", code)
	}
	if g.adminHits.Load() != 0 {
		t.Error("the handler ran after its middleware panicked")
	}
	if strings.Contains(body, "abc123") || strings.Contains(body, "token") {
		t.Errorf("the panic text reached the client: %s", body)
	}
	if h.Get("X-Content-Type-Options") != "nosniff" || h.Get("X-Request-Id") == "" {
		t.Errorf("a failed middleware lost the global headers: %v", h)
	}
}

// The global controls run before a route's own middleware, so unauthenticated
// traffic is rate limited before it can reach a login check: a flood costs the
// attacker its budget, not the authentication code its CPU.
func TestBruteForce_RateLimitRunsBeforeRouteMiddleware(t *testing.T) {
	t.Parallel()
	cfg := devCfg()
	cfg.RateLimit, cfg.RateBurst = 0.001, 3
	g := guarded(t, cfg)
	var limited int
	for range 25 {
		code, _, _ := as(g.h, peerIP, "GET", "/admin/users/1", nil)
		if code == 429 {
			limited++
		}
	}
	if n := g.denyCalls.Load(); n != 3 {
		t.Errorf("the middleware saw %d attempts from one client, want exactly the burst of 3", n)
	}
	if limited != 22 {
		t.Errorf("%d requests were limited, want 22", limited)
	}
}

// ---- injection -------------------------------------------------------------

var hostile = []string{
	`' OR 1=1--`, `"; DROP TABLE users;--`, `$(id)`, "`id`", `{"$ne":null}`, `<script>alert(1)</script>`,
	`${jndi:ldap://x.test/a}`, `{{7*7}}`, `%3Cscript%3E`, `javascript:alert(1)`, `1; cat /etc/passwd`,
}

var tells = []string{"<script", "drop table", "or 1=1", "$(id)", "`id`", "$ne", "jndi", "{{7*7}}", "javascript:", "cat /etc/passwd"}

// No hostile string comes back in a response, in a header or in a body, on any
// path: success, not found, rejected or failed. An input that is not echoed
// cannot be reflected into a page, a log viewer or an admin dashboard.
func TestInjection_PayloadsAreNeverReflectedOrFatal(t *testing.T) {
	t.Parallel()
	_, h := started(t, devCfg())
	check := func(where string, code int, hd http.Header, body string) {
		t.Helper()
		if code == 500 {
			t.Errorf("%s: hostile input caused a 500", where)
		}
		var all strings.Builder
		all.WriteString(body)
		for k, vs := range hd {
			all.WriteString(k)
			all.WriteString(strings.Join(vs, ","))
		}
		low := strings.ToLower(all.String())
		for _, tell := range tells {
			if strings.Contains(low, tell) {
				t.Errorf("%s: the response contains %q", where, tell)
			}
		}
	}
	for _, p := range hostile {
		for where, path := range map[string]string{
			"unrouted path": "/nope/" + url.PathEscape(p),
			"path param":    "/members/" + url.PathEscape(p),
			"query":         "/members/1?q=" + url.QueryEscape(p),
			"query on 404":  "/nope?q=" + url.QueryEscape(p),
		} {
			code, hd, body := as(h, peerIP, "GET", path, nil)
			check(where+" "+p, code, hd, body)
		}
		for _, name := range []string{"User-Agent", "Referer", "Origin", "X-Forwarded-For", "X-Real-Ip", "X-Request-Id", "Accept-Language", "Cookie", "Authorization"} {
			code, hd, body := as(h, peerIP, "GET", "/members/1", map[string]string{name: p})
			check("header "+name+" "+p, code, hd, body)
		}
		if !strings.ContainsAny(p, "/ ?#@\\") {
			code, hd, body := as(h, peerIP, "GET", "/members/1", map[string]string{"Host": "a" + p + ".test"})
			check("host "+p, code, hd, body)
		}
	}
}

// A CR or LF in a header value cannot add a header or split the response.
func TestInjection_HeaderSplittingCannotAddAHeader(t *testing.T) {
	t.Parallel()
	_, h := started(t, devCfg())
	valid := regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
	for _, rid := range []string{
		"abc\r\nSet-Cookie: pwn=1", "abc\nX-Injected: 1", "abc\rX-Injected: 1", "abc%0d%0aSet-Cookie: pwn=1",
		strings.Repeat("a", 65), "has space", "tab\there", "ünïcode",
	} {
		_, out, _ := as(h, peerIP, "GET", "/members/1", map[string]string{"X-Request-Id": rid})
		if got := out.Get("X-Request-Id"); !valid.MatchString(got) || got == rid {
			t.Errorf("request id %q was kept or mangled as %q", rid, got)
		}
		if out.Get("Set-Cookie") != "" || out.Get("X-Injected") != "" {
			t.Errorf("request id %q added a response header: %v", rid, out)
		}
	}
	for _, origin := range []string{"https://app.ecoop.test\r\nX-Injected: 1", "https://app.ecoop.test\nSet-Cookie: a=b"} {
		_, out, _ := as(h, peerIP, "GET", "/members/1", map[string]string{"Origin": origin})
		if out.Get("Set-Cookie") != "" || out.Get("X-Injected") != "" {
			t.Errorf("origin %q added a response header: %v", origin, out)
		}
		if got := out.Get("Access-Control-Allow-Origin"); got != "" && got != "https://app.ecoop.test" {
			t.Errorf("origin %q was echoed as %q", origin, got)
		}
	}
	// A sane id from the client is kept, so a trace can be followed across a hop.
	if _, out, _ := as(h, peerIP, "GET", "/members/1", map[string]string{"X-Request-Id": "client-id_1.2"}); out.Get("X-Request-Id") != "client-id_1.2" {
		t.Errorf("a plain request id was replaced: %q", out.Get("X-Request-Id"))
	}
}

// ---- the shape of a rejection ----------------------------------------------

// Every way the API says no has the same body: an error code and the request id,
// nothing else, as JSON that is never cached. A client (or a contract test) can
// rely on that shape, and nothing about the cause leaks.
func TestContract_EveryRejectionHasTheSameShape(t *testing.T) {
	t.Parallel()
	dev := func() *server.Hertz { _, h := started(t, devCfg()); return h }
	type tc struct {
		name   string
		status int
		code   string
		do     func() (int, http.Header, string)
	}
	cases := []tc{
		{"bad host", 400, api.CodeBadHost, func() (int, http.Header, string) {
			return as(dev(), peerIP, "GET", "/members/1", map[string]string{"Host": "evil.test"})
		}},
		{"bad method", 405, api.CodeBadMethod, func() (int, http.Header, string) {
			return as(dev(), peerIP, "TRACE", "/members/1", nil)
		}},
		{"scanner path", 404, api.CodeNotFound, func() (int, http.Header, string) {
			return as(dev(), peerIP, "GET", "/.env", nil)
		}},
		{"no such route", 404, api.CodeNotFound, func() (int, http.Header, string) {
			return as(dev(), peerIP, "GET", "/nope", nil)
		}},
		{"method not routed", 405, api.CodeBadMethod, func() (int, http.Header, string) {
			return as(dev(), peerIP, "DELETE", "/members/1", nil)
		}},
		{"hostile preflight", 403, api.CodeBadOrigin, func() (int, http.Header, string) {
			return as(dev(), peerIP, "OPTIONS", "/members/1", map[string]string{"Origin": "https://evil.test", "Access-Control-Request-Method": "GET"})
		}},
		{"declared oversize", 413, "payload_too_large", func() (int, http.Header, string) {
			_, h := started(t, api.Config{MaxBodyBytes: 10})
			return as(h, peerIP, "POST", "/orders", map[string]string{"Content-Length": "1000"})
		}},
		{"rate limited", 429, api.CodeTooManyRequests, func() (int, http.Header, string) {
			_, h := started(t, api.Config{RateLimit: 0.001, RateBurst: 1})
			as(h, peerIP, "GET", "/members/1", nil)
			return as(h, peerIP, "GET", "/members/1", nil)
		}},
		{"https required", 403, api.CodeHTTPSRequired, func() (int, http.Header, string) {
			_, h := started(t, api.Config{
				Secured: true, AllowedOrigins: []string{"https://app.ecoop.test"}, AllowedHosts: []string{"api.ecoop.test"},
				RateLimit: 1000, RateBurst: 1000,
			})
			return as(h, peerIP, "POST", "/orders", nil)
		}},
		{"handler error", 500, api.CodeInternal, func() (int, http.Header, string) {
			return as(dev(), peerIP, "GET", "/boom", nil)
		}},
		{"handler panic", 500, api.CodeInternal, func() (int, http.Header, string) {
			return as(dev(), peerIP, "POST", "/panic", nil)
		}},
		{"public error", 403, "not_yours", func() (int, http.Header, string) {
			return as(dev(), peerIP, "GET", "/denied", nil)
		}},
	}
	code := regexp.MustCompile(`^[a-z_]+$`)
	for _, c := range cases {
		status, hd, body := c.do()
		if status != c.status {
			t.Errorf("%s: status = %d, want %d (%s)", c.name, status, c.status, body)
			continue
		}
		var got map[string]any
		if err := json.Unmarshal([]byte(body), &got); err != nil {
			t.Errorf("%s: the body is not JSON: %q", c.name, body)
			continue
		}
		if len(got) != 2 || got["error"] != c.code || got["request_id"] == "" || got["request_id"] != hd.Get("X-Request-Id") {
			t.Errorf("%s: body = %s, want exactly {error: %q, request_id: <the header>}", c.name, body, c.code)
		}
		if s, _ := got["error"].(string); !code.MatchString(s) {
			t.Errorf("%s: error code %q is not a stable machine code", c.name, s)
		}
		if ct := hd.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
			t.Errorf("%s: Content-Type = %q", c.name, ct)
		}
		if hd.Get("Cache-Control") != "no-store" {
			t.Errorf("%s: an error may be cached: Cache-Control = %q", c.name, hd.Get("Cache-Control"))
		}
	}
}

// ---- CORS on every status --------------------------------------------------

// A browser can read an error only if the error carries the CORS headers, so an
// approved origin gets them on 4xx and 5xx too; an unapproved origin gets them
// on nothing, however the request ends.
func TestCORS_ApprovedOriginSeesErrorsAndOthersSeeNothing(t *testing.T) {
	t.Parallel()
	cfg := devCfg()
	cfg.RateLimit, cfg.RateBurst = 0.001, 1
	_, h := started(t, cfg)
	const good, bad = "https://app.ecoop.test", "https://evil.test"
	for i, path := range []string{"/boom", "/nope", "/boom"} { // the third is rate limited
		code, hd, _ := as(h, peerIP, "GET", path, map[string]string{"Origin": good})
		if hd.Get("Access-Control-Allow-Origin") != good || hd.Get("Access-Control-Allow-Credentials") != "true" {
			t.Errorf("approved origin, request %d (%d): CORS headers missing: %v", i, code, hd)
		}
		if !strings.Contains(hd.Get("Vary"), "Origin") {
			t.Errorf("request %d: Vary does not name Origin", i)
		}
	}
	for _, path := range []string{"/members/1", "/boom", "/nope", "/panic"} {
		_, hd, _ := as(h, "198.51.100."+string(rune('1'+len(path)%8)), "GET", path, map[string]string{"Origin": bad})
		if hd.Get("Access-Control-Allow-Origin") != "" || hd.Get("Access-Control-Allow-Credentials") != "" {
			t.Errorf("%s: an unapproved origin received CORS headers: %v", path, hd)
		}
	}
}

// Found by FuzzRequestChain: an origin that differs only in case matched, and the
// response echoed the client's bytes. The answer must be the configured value.
func TestCORS_AnswerIsTheConfiguredOriginNotTheClientsSpelling(t *testing.T) {
	_, h := started(t, devCfg())
	_, out, _ := as(h, peerIP, "GET", "/members/1", map[string]string{"Origin": "https://App.eCoop.test"})
	if got := out.Get("Access-Control-Allow-Origin"); got != "https://app.ecoop.test" {
		t.Errorf("Access-Control-Allow-Origin = %q, want the configured https://app.ecoop.test", got)
	}
}

// Unicode case folding must not turn a look-alike into an allowed name: the
// Kelvin sign (U+212A) lower-cases to the ASCII letter k.
func TestLookAlikes_AreNotTheAllowedHostOrOrigin(t *testing.T) {
	t.Parallel()
	cfg := api.Config{
		AllowedOrigins: []string{"https://kapp.ecoop.test"},
		AllowedHosts:   []string{"kapi.ecoop.test"},
	}
	_, h := started(t, cfg)
	if code, _, _ := as(h, peerIP, "GET", "/members/1", map[string]string{"Host": "kapi.ecoop.test"}); code != 200 {
		t.Fatalf("the real host = %d", code)
	}
	if code, _, _ := as(h, peerIP, "GET", "/members/1", map[string]string{"Host": "\u212Aapi.ecoop.test"}); code != 400 {
		t.Errorf("a Kelvin-sign look-alike host = %d, want 400", code)
	}
	_, out, _ := as(h, peerIP, "GET", "/members/1", map[string]string{"Host": "kapi.ecoop.test", "Origin": "https://\u212Aapp.ecoop.test"})
	if got := out.Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("a look-alike origin was allowed as %q", got)
	}
}

// ---- concurrency -----------------------------------------------------------

// Under concurrent load every request gets its own id, and a limit counts every
// request exactly once: not one more than the burst gets through.
func TestConcurrency_RequestIDsAreUniqueAndTheBurstIsExact(t *testing.T) {
	t.Parallel()
	_, h := started(t, api.Config{})
	const n = 300
	ids := make(chan string, n)
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			_, hd, _ := as(h, peerIP, "GET", "/members/1", nil)
			ids <- hd.Get("X-Request-Id")
		})
	}
	wg.Wait()
	close(ids)
	seen := map[string]bool{}
	for id := range ids {
		if id == "" || seen[id] {
			t.Fatalf("request id %q is empty or repeated", id)
		}
		seen[id] = true
	}

	_, lim := started(t, api.Config{RateLimit: 0.001, RateBurst: 20})
	var ok, limited atomic.Int32
	for range 400 {
		wg.Go(func() {
			code, _, _ := as(lim, peerIP, "GET", "/members/1", nil)
			switch code {
			case 200:
				ok.Add(1)
			case 429:
				limited.Add(1)
			}
		})
	}
	wg.Wait()
	if ok.Load() != 20 || limited.Load() != 380 {
		t.Errorf("400 concurrent requests against a burst of 20: %d passed, %d limited; want 20 and 380", ok.Load(), limited.Load())
	}
}

// ---- fuzzing ---------------------------------------------------------------

func printable(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

var fuzzMethods = []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "TRACE", "CONNECT", "PROPFIND"}

// FuzzRequestChain throws arbitrary method, path, query, host, origin and header
// values at the whole chain. Whatever it sends: no panic; a valid status; the
// request id and the security headers on every response; a response header
// never carries a line break or a cookie; CORS names only the approved origin;
// and a long input is never echoed back. Inputs with control characters are
// skipped: a real connection's parser refuses them before the chain sees them,
// and header splitting is tested directly above.
func FuzzRequestChain(f *testing.F) {
	_, h := started(f, api.Config{
		AllowedOrigins: []string{"https://app.ecoop.test"}, AllowedHosts: []string{"api.ecoop.test"},
		BlockSuspiciousPaths: true, RateLimit: 1e9, RateBurst: 1 << 30,
	})
	for _, s := range []struct {
		m                                       uint8
		path, query, host, origin, xff, rid, ua string
	}{
		{0, "/members/1", "", "api.ecoop.test", "", "", "", ""},
		{1, "/orders", "a=b", "api.ecoop.test", "https://app.ecoop.test", "1.2.3.4", "abc", "curl/8"},
		{6, "/members/1", "", "api.ecoop.test", "https://evil.test", "", "", ""},
		{0, "/.env", "", "api.ecoop.test", "", "", "", ""},
		{0, "/boom", "", "api.ecoop.test", "", "", "", ""},
		{1, "/panic", "", "evil.test", "", "", "", ""},
		{0, "//a/../b/%2e%2e/%00", "x=%ff", "api.ecoop.test:8443", "null", "::1, 10.0.0.1", "id with space", "ünï"},
		{9, "/admin", "q='OR'1=1--", "api.ecoop.test", "https://app.ecoop.test", "999.1.1.1", "", "<script>"},
	} {
		f.Add(s.m, s.path, s.query, s.host, s.origin, s.xff, s.rid, s.ua)
	}
	valid := regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
	f.Fuzz(func(t *testing.T, m uint8, path, query, host, origin, xff, rid, ua string) {
		for _, s := range []string{path, query, host, origin, xff, rid, ua} {
			if !printable(s) {
				t.Skip()
			}
		}
		if strings.ContainsAny(host, "/?#@ \\") || strings.ContainsAny(path+query, " #") {
			t.Skip()
		}
		p := path
		if !strings.HasPrefix(p, "/") {
			p = "/" + p
		}
		if query != "" {
			p += "?" + query
		}
		hdrs := map[string]string{}
		for k, v := range map[string]string{"Host": host, "Origin": origin, "X-Forwarded-For": xff, "X-Request-Id": rid, "User-Agent": ua} {
			if v != "" {
				hdrs[k] = v
			}
		}
		code, out, body := as(h, peerIP, fuzzMethods[int(m)%len(fuzzMethods)], p, hdrs)

		if code < 100 || code > 599 {
			t.Fatalf("status %d", code)
		}
		id := out.Get("X-Request-Id")
		if !valid.MatchString(id) {
			t.Fatalf("request id %q is missing or invalid (status %d)", id, code)
		}
		if rid != "" && valid.MatchString(rid) && id != rid {
			t.Fatalf("a valid client id %q was replaced by %q", rid, id)
		}
		for _, k := range []string{"X-Content-Type-Options", "Content-Security-Policy", "X-Frame-Options"} {
			if out.Get(k) == "" {
				t.Fatalf("%s is missing on a %d", k, code)
			}
		}
		if out.Get("Set-Cookie") != "" {
			t.Fatalf("a cookie was set: %v", out)
		}
		for k, vs := range out {
			for _, v := range vs {
				if strings.ContainsAny(v, "\r\n") {
					t.Fatalf("header %s carries a line break: %q", k, v)
				}
			}
		}
		if v := out.Get("Access-Control-Allow-Origin"); v != "" && v != "https://app.ecoop.test" {
			t.Fatalf("CORS allowed %q", v)
		}
		for _, s := range []string{path, query, host, origin, xff, ua} {
			if len(s) >= 12 && s != id && strings.Contains(body, s) {
				t.Fatalf("the response echoes an input: %q in %q", s, body)
			}
		}
	})
}

// FuzzFromEnv: whatever the environment holds, FromEnv does not panic, and a
// configuration it accepts is one the service will start with and that has every
// protection Secured promises. An unsafe value is an error, never a quiet default.
func FuzzFromEnv(f *testing.F) {
	f.Add("0.0.0.0:8080", "true", "https://a.test", "a.test", "10.0.0.0/8", "1048576", "10", "30", "15s")
	f.Add("", "", "", "", "", "", "", "", "")
	f.Add("x", "TRUE", "http://a.test", "*", "0.0.0.0/0", "-1", "NaN", "0", "fifteen")
	f.Add(":80", "true", "https://a.test/", "https://a.test", "::/0", "9999999999999999999", "+Inf", "-3", "-1s")
	f.Fuzz(func(t *testing.T, addr, secured, origins, hosts, proxies, body, rate, burst, timeout string) {
		vals := map[string]string{
			"API_ADDR": addr, "API_SECURED": secured, "API_ALLOWED_ORIGINS": origins, "API_ALLOWED_HOSTS": hosts,
			"API_TRUSTED_PROXIES": proxies, "API_MAX_BODY_BYTES": body, "API_RATE_LIMIT": rate, "API_RATE_BURST": burst,
			"API_READ_TIMEOUT": timeout, "API_WRITE_TIMEOUT": timeout,
		}
		cfg, err := api.FromEnv(func(k string) (string, bool) { v, ok := vals[k]; return v, ok })
		if err != nil {
			return
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("FromEnv accepted what Validate refuses: %v", err)
		}
		if math.IsNaN(cfg.RateLimit) || math.IsInf(cfg.RateLimit, 0) || cfg.RateLimit < 0 {
			t.Fatalf("an unusable rate limit was accepted: %v", cfg.RateLimit)
		}
		for _, p := range cfg.TrustedProxies {
			pre, err := netip.ParsePrefix(p)
			if err != nil {
				a, aerr := netip.ParseAddr(strings.TrimSpace(p))
				if aerr != nil {
					t.Fatalf("an unparseable trusted proxy %q was accepted", p)
				}
				pre = netip.PrefixFrom(a, a.BitLen())
			}
			if pre.Bits() == 0 {
				t.Fatalf("trusting every address was accepted: %q", p)
			}
		}
		if cfg.Secured {
			if len(cfg.AllowedOrigins) == 0 || cfg.RateLimit <= 0 || cfg.EnablePprof {
				t.Fatalf("a Secured config lacks origins, a rate limit, or has pprof: %+v", cfg)
			}
			for _, o := range cfg.AllowedOrigins {
				if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(o)), "https://") {
					t.Fatalf("a Secured config allows a non-https origin %q", o)
				}
			}
		}
		if err := api.NewAPIService(0, nil, nil, api.WithConfig(cfg)).Start(context.Background()); err != nil {
			t.Fatalf("a configuration FromEnv accepted does not start: %v", err)
		}
	})
}
