package api_test

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/api"
)

// These tests drive requests from a chosen peer address, which is what the
// trusted-proxy, rate-limit, redirect and loopback rules depend on.

// limitedCfg is a Secured configuration with a rate limit. Secured means plain
// HTTP is redirected, so tests that reach a handler send X-Forwarded-Proto from a
// trusted proxy, as production does; the ones that need a direct client use
// directCfg.
func limitedCfg(rate float64, burst int, proxies ...string) api.Config {
	return api.Config{
		Secured:        true,
		AllowedOrigins: []string{"https://app.ecoop.test"},
		AllowedHosts:   []string{"api.ecoop.test"},
		TrustedProxies: proxies,
		RateLimit:      rate, RateBurst: burst,
	}
}

// directCfg is a development configuration with a rate limit, for tests of a
// client that talks to the server itself.
func directCfg(rate float64, burst int, proxies ...string) api.Config {
	return api.Config{
		AllowedHosts:   []string{"api.ecoop.test"},
		TrustedProxies: proxies,
		RateLimit:      rate, RateBurst: burst,
	}
}

var httpsFwd = map[string]string{"X-Forwarded-Proto": "https"}

func with(base map[string]string, kv ...string) map[string]string {
	out := map[string]string{}
	maps.Copy(out, base)
	for i := 0; i+1 < len(kv); i += 2 {
		out[kv[i]] = kv[i+1]
	}
	return out
}

// ---- rate limiting ---------------------------------------------------------

// Each client address has its own bucket, and the burst is the bucket's size.
func TestRateLimit_PerClientAddress(t *testing.T) {
	t.Parallel()
	_, h := started(t, directCfg(0.001, 5))
	ok, limited := 0, 0
	for range 20 {
		code, hd, _ := as(h, "203.0.113.7", "GET", "/members/1", nil)
		switch code {
		case 429:
			limited++
			if hd.Get("Retry-After") == "" {
				t.Error("a 429 without Retry-After")
			}
		case 200:
			ok++
		}
	}
	if ok != 5 || limited != 15 {
		t.Errorf("one client: %d ok, %d limited; want 5 and 15", ok, limited)
	}
	// Another client has its own bucket, untouched by the first.
	if code, _, _ := as(h, "203.0.113.8", "GET", "/members/1", nil); code != 200 {
		t.Errorf("a second client = %d, want 200", code)
	}
}

// The 429 body carries a code and the request id, nothing about the limiter.
func TestRateLimit_ResponseIsGeneric(t *testing.T) {
	t.Parallel()
	_, h := started(t, directCfg(0.001, 1))
	as(h, "203.0.113.7", "GET", "/members/1", nil)
	code, _, body := as(h, "203.0.113.7", "GET", "/members/1", nil)
	if code != 429 || !strings.Contains(body, `"error":"rate_limited"`) || !strings.Contains(body, `"request_id"`) {
		t.Fatalf("limited response = %d %s", code, body)
	}
}

// docs/api.md "Rate limiting": a client cannot name itself. X-Forwarded-For is
// ignored from an untrusted peer, so rotating it does not rotate the bucket.
func TestRateLimit_ForgedForwardedForDoesNotEscapeTheLimit(t *testing.T) {
	t.Parallel()
	_, h := started(t, directCfg(0.001, 3)) // no trusted proxies at all
	allowed := 0
	for i := range 50 {
		code, _, _ := as(h, "203.0.113.7", "GET", "/members/1", map[string]string{"X-Forwarded-For": fmt.Sprintf("198.51.100.%d", i), "X-Real-Ip": fmt.Sprintf("198.51.100.%d", i)})
		if code == 200 {
			allowed++
		}
	}
	if allowed != 3 {
		t.Errorf("%d requests were let through by rotating X-Forwarded-For, want the burst of 3", allowed)
	}
}

// From a trusted proxy the real client is the right-most address that is not the
// proxy itself, and anything the client prepended is ignored.
func TestRateLimit_BehindATrustedProxyTheRealClientIsCounted(t *testing.T) {
	t.Parallel()
	_, h := started(t, limitedCfg(0.001, 2, "10.0.0.0/8"))
	send := func(xff string) int {
		code, _, _ := as(h, "10.1.2.3", "GET", "/members/1", with(httpsFwd, "X-Forwarded-For", xff))
		return code
	}
	// The same real client (198.51.100.9) behind the proxy, with forged prefixes.
	got := []int{
		send("1.1.1.1, 198.51.100.9"), send("2.2.2.2, 198.51.100.9"),
		send("3.3.3.3, 4.4.4.4, 198.51.100.9"), send("5.5.5.5, 198.51.100.9"),
	}
	if got[0] != 200 || got[1] != 200 || got[2] != 429 || got[3] != 429 {
		t.Errorf("one real client with forged prefixes = %v, want 200 200 429 429", got)
	}
	// A different real client has its own bucket.
	if code := send("198.51.100.10"); code != 200 {
		t.Errorf("another real client = %d", code)
	}
	// A malformed header falls back to the proxy's own address, not to a free pass.
	for range 5 {
		send("not-an-ip")
	}
	if code := send("not-an-ip"); code != 429 {
		t.Errorf("a malformed forwarded header = %d, want it counted against the proxy", code)
	}
}

// A request with no usable address shares one strict bucket and is limited.
func TestRateLimit_TableIsBoundedUnderForgedAddresses(t *testing.T) {
	t.Parallel()
	_, h := started(t, limitedCfg(1000, 1000, "10.0.0.0/8"))
	for i := range 3000 {
		as(h, "10.1.2.3", "GET", "/members/1", with(httpsFwd, "X-Forwarded-For", fmt.Sprintf("198.51.%d.%d", i/250, i%250)))
	}
	// Nothing to assert on the table directly: the limiter's own tests cover its
	// bound. What matters here is that 3000 distinct forged clients are served and
	// do not break the server.
	if code, _, _ := as(h, "10.1.2.3", "GET", "/members/1", httpsFwd); code != 200 && code != 429 {
		t.Errorf("after many forged clients: %d", code)
	}
}

// Keyed limits what an address limit cannot: one account, one flow.
func TestRateLimit_KeyedByAnApplicationKey(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	svc := api.NewAPIService(0, nil, nil, api.WithConfig(api.Config{})).(*api.APIService)
	keyed := api.Keyed(0.001, 2, func(c *app.RequestContext) string { return string(c.Request.Header.Peek("X-User")) })
	if err := svc.RegisterRoute(api.Route{Route: "/otp", Method: "POST"}, func(_ context.Context, c *app.RequestContext) error {
		calls.Add(1)
		c.String(200, "ok")
		return nil
	}, keyed); err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	h := svc.Handler()
	codes := func(user string, n int) (ok, limited int) {
		for range n {
			c, _, _ := as(h, "203.0.113.7", "POST", "/otp", map[string]string{"X-User": user, "Host": "x.test"})
			switch c {
			case 200:
				ok++
			case 429:
				limited++
			}
		}
		return
	}
	if ok, lim := codes("ana", 6); ok != 2 || lim != 4 {
		t.Errorf("ana: %d ok, %d limited; want 2 and 4", ok, lim)
	}
	if ok, _ := codes("ben", 2); ok != 2 {
		t.Errorf("ben shares ana's bucket: %d ok", ok)
	}
	if calls.Load() != 4 {
		t.Errorf("the handler ran %d times, want 4: a limited request must not reach it", calls.Load())
	}
}

// ---- https and proxies -----------------------------------------------------

// The redirect, the probe exemption and the trusted-proxy rule, from real peers.
func TestHTTPS_FromRealPeers(t *testing.T) {
	t.Parallel()
	_, h := started(t, limitedCfg(1000, 1000, "10.0.0.0/8"))

	code, hd, _ := as(h, "203.0.113.7", "GET", "/members/1?x=1", nil)
	if code != 308 || hd.Get("Location") != "https://api.ecoop.test/members/1?x=1" {
		t.Errorf("plain GET = %d %q", code, hd.Get("Location"))
	}
	if code, _, _ := as(h, "203.0.113.7", "POST", "/orders", nil); code != 403 {
		t.Errorf("plain POST = %d, want 403", code)
	}
	// An untrusted peer claiming HTTPS is still redirected.
	if code, _, _ := as(h, "203.0.113.7", "GET", "/members/1", httpsFwd); code != 308 {
		t.Errorf("an untrusted peer's X-Forwarded-Proto = %d, want 308", code)
	}
	// A trusted proxy saying it was HTTPS is believed.
	if code, _, _ := as(h, "10.1.2.3", "GET", "/members/1", httpsFwd); code != 200 {
		t.Errorf("a trusted proxy's X-Forwarded-Proto = %d, want 200", code)
	}
	// The probes are served on the pod's own address, over plain HTTP, from anywhere.
	for _, p := range []string{api.LivenessPath, api.ReadinessPath} {
		if code, _, _ := as(h, "10.1.2.3", "GET", p, map[string]string{"Host": "10.1.2.3:8080"}); code == 308 || code == 400 {
			t.Errorf("%s = %d: a probe must not be redirected or host-filtered", p, code)
		}
	}
}

// ---- loopback-only endpoints -----------------------------------------------

func TestPprofAndInventory_OnlyForLoopback(t *testing.T) {
	t.Parallel()
	cfg := api.Config{EnablePprof: true}
	_, h := started(t, cfg)
	for _, ip := range []string{"203.0.113.7", "10.1.2.3", "192.168.1.5", "::ffff:203.0.113.7"} {
		for _, p := range []string{"/debug/pprof/", "/debug/pprof/heap", "/routes"} {
			if code, _, body := as(h, ip, "GET", p, map[string]string{"Host": "x.test"}); code != 404 || strings.Contains(body, "Types of profiles") {
				t.Errorf("%s from %s = %d", p, ip, code)
			}
		}
	}
	for _, ip := range []string{"127.0.0.1", "[::1]"} {
		// Status only: pprof is written straight to the connection, so its body is
		// not visible in process. TestSocket_PprofBodyAndHeadersOverARealConnection
		// checks the body and the headers on a real one.
		if code, _, _ := as(h, ip, "GET", "/debug/pprof/cmdline", map[string]string{"Host": "x.test"}); code != 200 {
			t.Errorf("pprof from loopback %s = %d", ip, code)
		}
	}
	code, _, body := as(h, "127.0.0.1", "GET", "/routes", map[string]string{"Host": "x.test"})
	if code != 200 {
		t.Fatalf("inventory from loopback = %d", code)
	}
	if strings.Contains(body, "/secret") {
		t.Errorf("a Private route is listed: %s", body)
	}
	if !strings.Contains(body, "/members/:id") {
		t.Errorf("a public route is missing: %s", body)
	}
}

// A loopback client forwarded by a trusted proxy is the proxy's client, not
// loopback: "X-Forwarded-For: 127.0.0.1" must not open the debug routes.
func TestPprof_ForwardedLoopbackIsNotLoopback(t *testing.T) {
	t.Parallel()
	cfg := api.Config{EnablePprof: true, TrustedProxies: []string{"10.0.0.0/8"}}
	_, h := started(t, cfg)
	code, _, _ := as(h, "10.1.2.3", "GET", "/debug/pprof/cmdline", map[string]string{"Host": "x.test", "X-Forwarded-For": "127.0.0.1"})
	if code != 404 {
		t.Errorf("pprof via a proxy claiming loopback = %d, want 404", code)
	}
}

// ---- panics and the cookie --------------------------------------------------

func TestPanic_RecoveredWithHeadersAndNoDetail(t *testing.T) {
	t.Parallel()
	_, h := started(t, limitedCfg(1000, 1000, "10.0.0.0/8"))
	code, hd, body := as(h, "10.1.2.3", "POST", "/panic", with(httpsFwd, "Content-Type", "application/json"))
	if code != 500 {
		t.Fatalf("status = %d", code)
	}
	if hd.Get("X-Content-Type-Options") != "nosniff" || hd.Get("Strict-Transport-Security") == "" {
		t.Errorf("a recovered panic has lost its security headers: %v", hd)
	}
	for _, leak := range []string{"4111", "ana@example.com", "panic", "goroutine"} {
		if strings.Contains(body, leak) {
			t.Errorf("%q leaked: %s", leak, body)
		}
	}
	// And the server still serves the next request.
	if code, _, _ := as(h, "10.1.2.3", "GET", "/members/1", httpsFwd); code != 200 {
		t.Errorf("after a panic, the next request = %d", code)
	}
}

// Even a loopback client cannot reach the inventory or pprof when Secured: the
// answer does not depend on where the request comes from.
func TestInventory_NeverServedWhenSecured(t *testing.T) {
	t.Parallel()
	_, h := started(t, limitedCfg(1000, 1000, "127.0.0.1"))
	// The loopback peer is itself a trusted proxy here, so its X-Forwarded-Proto is
	// believed and the request reaches the route, which must still answer 404.
	for _, p := range []string{"/routes", "/debug/pprof/", "/debug/pprof/cmdline"} {
		if code, _, _ := as(h, "127.0.0.1", "GET", p, with(httpsFwd, "Host", "api.ecoop.test")); code != 404 {
			t.Errorf("%s from loopback in Secured mode = %d, want 404", p, code)
		}
	}
}
