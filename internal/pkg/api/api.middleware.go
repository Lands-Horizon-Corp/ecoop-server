package api

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"slices"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// The middleware here is in the order the API applies it. Each function is a
// plain app.HandlerFunc built from the Config, so a test can run one alone.

// Public error codes. A client gets one of these and a request id, never an
// error's text: the detail is logged on the server.
const (
	CodeBadHost         = "host_not_allowed"
	CodeBadMethod       = "method_not_allowed"
	CodeBadOrigin       = "origin_not_allowed"
	CodeTooManyRequests = "rate_limited"
	CodeInternal        = "internal_error"
	CodeNotFound        = "not_found"
	CodeHTTPSRequired   = "https_required"
	CodeShuttingDown    = "shutting_down"
	CodeTooManyStreams  = "too_many_streams"
	CodePayloadTooLarge = "payload_too_large"
)

// HeaderRequestID is the correlation id header.
const HeaderRequestID = "X-Request-Id"

// reject ends the request with a JSON body that carries only a stable code and
// the request id. Headers set earlier (security headers, CORS) are kept.
func reject(c *app.RequestContext, status int, code string) {
	c.Header("Cache-Control", "no-store")
	c.AbortWithStatusJSON(status, map[string]string{"error": code, "request_id": requestIDOf(c)})
}

func requestIDOf(c *app.RequestContext) string {
	if v, ok := c.Get(keyRequestID); ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

const keyRequestID = "api.request_id"

// ---- request id ------------------------------------------------------------.

// requestID gives every request an id and returns it in the response. An id sent
// by the client is used only if it is short and plain, and is never an identity:
// it is a label for finding a log line, nothing more. Anything else is replaced.
func requestID() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		id := string(c.Request.Header.Peek(HeaderRequestID))
		if !validRequestID(id) {
			id = newID()
		}
		c.Set(keyRequestID, id)
		c.Header(HeaderRequestID, id)
		c.Next(ctx)
	}
}

func validRequestID(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for _, r := range s {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '-' && r != '_' && r != '.' {
			return false
		}
	}
	return true
}

func newID() string { return utils.RandomHex(12) }

// ---- security headers ------------------------------------------------------.

// securityHeaders sets the header policy BEFORE the handler runs. Headers set
// after c.Next are lost whenever the handler has already flushed (a stream, a
// large body), so the policy that docs/api.md asks for must go first, and it is
// then present on success, error, redirect, panic and stream alike.
func securityHeaders(secured bool) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		h := &c.Response.Header
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Cross-Origin-Resource-Policy", "same-site")
		h.Set("Cross-Origin-Opener-Policy", "same-origin")
		h.Set("Permissions-Policy", "camera=(), microphone=(), geolocation=(), payment=(), usb=()")
		// An API returns data, not pages: nothing it sends may run as a script,
		// load a resource or be framed. This is deliberately the strictest policy;
		// a route that serves HTML sets its own.
		h.Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
		if secured {
			// Only over HTTPS, and never includeSubDomains or preload by default:
			// those commit every subdomain, and a domain cannot be un-preloaded
			// quickly. Opt in once every subdomain is known to serve HTTPS.
			h.Set("Strict-Transport-Security", "max-age=31536000")
		}
		c.Next(ctx)
	}
}

// ---- recovery --------------------------------------------------------------.

// recovery turns a panic into a generic 500 with the request id. The panic value
// and stack are logged on the server with the route and request id, and never
// the request body, its headers, or its query: a payload can hold a password,
// a token or a ledger instruction, and a log line outlives the request.
func recovery(log *slog.Logger) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		defer func() {
			r := recover()
			if r == nil {
				return
			}
			log.Error("panic in a request handler",
				"request_id", requestIDOf(c), "route", c.FullPath(), "method", string(c.Method()),
				"panic", fmt.Sprintf("%T", r)) // the type only: the text of a panic can carry data
			if c.Response.Header.ContentLength() > 0 || len(c.Response.Body()) > 0 {
				c.Response.ResetBody()
			}
			reject(c, consts.StatusInternalServerError, CodeInternal)
		}()
		c.Next(ctx)
	}
}

// ---- method, host, https ---------------------------------------------------.

func methodAllowlist(methods []string) app.HandlerFunc {
	set := make(map[string]bool, len(methods))
	for _, m := range methods {
		set[strings.ToUpper(m)] = true
	}
	return func(ctx context.Context, c *app.RequestContext) {
		if !set[string(c.Method())] {
			c.Header("Allow", allowHeader(methods))
			reject(c, consts.StatusMethodNotAllowed, CodeBadMethod)
			return
		}
		c.Next(ctx)
	}
}

func allowHeader(methods []string) string {
	up := make([]string, len(methods))
	for i, m := range methods {
		up[i] = strings.ToUpper(m)
	}
	return strings.Join(up, ", ")
}

// hostAllowlist rejects a request whose Host is not one this API answers to. An
// empty list allows any host (a developer machine). Host filtering is not
// authentication; it stops a request that reached the server by an address
// nobody intended, which is how host-header attacks and cache poisoning start.
func hostAllowlist(hosts []string, log *slog.Logger) app.HandlerFunc {
	set := make(map[string]bool, len(hosts))
	for _, h := range hosts {
		set[h] = true
	}
	return func(ctx context.Context, c *app.RequestContext) {
		if len(set) == 0 {
			c.Next(ctx)
			return
		}
		// An orchestrator probes a pod by its own IP address, which is never in the
		// list of public names. Filtering the probes would fail every one of them
		// and get a healthy pod restarted; they reveal nothing, so they are exempt,
		// the same as from the HTTPS redirect.
		switch string(c.Path()) {
		case LivenessPath, ReadinessPath:
			c.Next(ctx)
			return
		}
		host := utils.ASCIILower(string(c.Host()))
		if !set[host] && !set[stripPort(host)] {
			log.Warn("request for a host that is not allowed", "request_id", requestIDOf(c), "host", utils.Truncate(utils.StripControl(host), 64))
			reject(c, consts.StatusBadRequest, CodeBadHost)
			return
		}
		c.Next(ctx)
	}
}

func stripPort(h string) string {
	if host, _, err := net.SplitHostPort(h); err == nil {
		return host
	}
	return h
}

// httpsRedirect sends plain-HTTP requests to HTTPS in secured mode, except the
// health probes, which an orchestrator calls on the pod's own address. Whether a
// request was HTTPS is believed only from a trusted proxy's X-Forwarded-Proto.
func httpsRedirect(secured bool, ip *clientIP) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if !secured || ip.isHTTPS(c) {
			c.Next(ctx)
			return
		}
		switch string(c.Path()) {
		case LivenessPath, ReadinessPath:
			c.Next(ctx)
			return
		}
		// A redirect for a request that carries a body is not safe to follow, and a
		// non-GET to a plain port is more likely a misconfigured client than a
		// browser: refuse it instead of silently redirecting a credential-bearing POST.
		if m := string(c.Method()); m != "GET" && m != "HEAD" {
			reject(c, consts.StatusForbidden, CodeHTTPSRequired)
			return
		}
		host := stripCRLF(string(c.Host()))
		target := "https://" + host + string(c.Request.URI().RequestURI())
		c.Redirect(consts.StatusPermanentRedirect, []byte(target))
		c.Abort()
	}
}

// ---- cors ------------------------------------------------------------------.

// cors answers cross-origin requests for exactly the configured origins. It
// never echoes an origin it does not know, never sends a wildcard with
// credentials, and always sends Vary: Origin so a cache cannot hand one origin's
// answer to another. CORS is a browser policy and not access control: a
// rejected origin only stops a browser from reading the response.
func cors(origins []string) app.HandlerFunc {
	allowed := make(map[string]bool, len(origins))
	for _, o := range origins {
		allowed[o] = true
	}
	return func(ctx context.Context, c *app.RequestContext) {
		c.Response.Header.Add("Vary", "Origin")
		origin := string(c.Request.Header.Peek("Origin"))
		if origin == "" {
			c.Next(ctx)
			return
		}
		// Matched in ASCII lower case only (a browser always sends its origin that
		// way), and answered with the configured value, never the client's bytes.
		origin = utils.ASCIILower(origin)
		if !allowed[origin] {
			if string(c.Method()) == "OPTIONS" {
				reject(c, consts.StatusForbidden, CodeBadOrigin)
				return
			}
			// A simple cross-origin request is still processed by the server; only
			// the browser withholds the response. State-changing routes are
			// protected by the session and CSRF checks, not by this.
			c.Next(ctx)
			return
		}
		h := &c.Response.Header
		h.Set("Access-Control-Allow-Origin", origin)
		h.Set("Access-Control-Allow-Credentials", "true")
		h.Set("Access-Control-Expose-Headers", HeaderRequestID+", Retry-After")
		if string(c.Method()) == "OPTIONS" && len(c.Request.Header.Peek("Access-Control-Request-Method")) > 0 {
			h.Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS")
			h.Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-CSRF-Token, "+HeaderRequestID)
			h.Set("Access-Control-Max-Age", "3600")
			h.Add("Vary", "Access-Control-Request-Method")
			h.Add("Vary", "Access-Control-Request-Headers")
			c.AbortWithStatus(consts.StatusNoContent)
			return
		}
		c.Next(ctx)
	}
}

// ---- body size -------------------------------------------------------------.

// bodyLimit refuses a request that declares a body larger than limit. This only
// covers a declared length and saves the work of reading it: the real limit is
// the server's own MaxRequestBodySize, which counts bytes as they arrive and
// answers 413 for chunked or lying requests too, before any handler runs.
func bodyLimit(limit int) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if limit > 0 && c.Request.Header.ContentLength() > limit {
			reject(c, consts.StatusRequestEntityTooLarge, CodePayloadTooLarge)
			return
		}
		c.Next(ctx)
	}
}

// ---- client address --------------------------------------------------------.

// clientIP resolves the address a request counts as. A forwarded header is read
// only when the connection comes from a trusted proxy, and then only the entry
// that proxy appended (the right-most one that is not itself a trusted proxy),
// because every earlier entry is whatever the client wrote. Hertz's default
// trusts every address, so any client could name itself and walk around a rate
// limit; this does not.
type clientIP struct {
	trusted []netip.Prefix
}

func newClientIP(trusted []netip.Prefix) *clientIP { return &clientIP{trusted: trusted} }

func (p *clientIP) isTrusted(a netip.Addr) bool {
	a = a.Unmap()
	for _, pre := range p.trusted {
		if pre.Contains(a) {
			return true
		}
	}
	return false
}

func peerAddr(c *app.RequestContext) (netip.Addr, bool) {
	ap, err := netip.ParseAddrPort(c.RemoteAddr().String())
	if err != nil {
		if a, aerr := netip.ParseAddr(strings.Trim(c.RemoteAddr().String(), "[]")); aerr == nil {
			return a.Unmap(), true
		}
		return netip.Addr{}, false
	}
	return ap.Addr().Unmap(), true
}

// Of returns the client's address as a string, or "" when it cannot be told.
func (p *clientIP) of(c *app.RequestContext) string {
	peer, ok := peerAddr(c)
	if !ok {
		return ""
	}
	if !p.isTrusted(peer) {
		return peer.String()
	}
	for _, name := range []string{"X-Forwarded-For", "X-Real-Ip"} {
		items := strings.Split(string(c.Request.Header.Peek(name)), ",")
		for _, item := range slices.Backward(items) {
			a, err := netip.ParseAddr(strings.TrimSpace(item))
			if err != nil {
				break // a malformed entry: stop trusting this header
			}
			if a = a.Unmap(); !p.isTrusted(a) {
				return a.String()
			}
		}
	}
	return peer.String()
}

// isHTTPS reports whether the request reached the edge over HTTPS: the connection
// itself, or a trusted proxy's X-Forwarded-Proto.
func (p *clientIP) isHTTPS(c *app.RequestContext) bool {
	if string(c.Request.URI().Scheme()) == "https" {
		return true
	}
	peer, ok := peerAddr(c)
	if !ok || !p.isTrusted(peer) {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(string(c.Request.Header.Peek("X-Forwarded-Proto"))), "https")
}

// ---- suspicious paths ------------------------------------------------------.

var suspiciousFragments = []string{
	"/.env", "/.git", "/.svn", "/.hg", "/.ds_store", "/.aws", "/.ssh", "/.htaccess", "/.htpasswd",
	"/wp-admin", "/wp-login", "/wp-content", "/wp-includes", "/xmlrpc.php", "/phpmyadmin", "/pma/",
	"/cgi-bin", "/boaform", "/actuator", "/server-status", "/vendor/phpunit", "/shell", "/eval-stdin",
	"/config.json", "/config.yml", "/docker-compose", "/backup", "/dump.sql", "/id_rsa",
	"../", "..%2f", "%2e%2e", "\\..\\",
}

// isSuspiciousPath reports whether a path is what a scanner asks for. It runs on
// the decoded and the raw path, in lower case, so an encoded probe is caught too.
func isSuspiciousPath(path string) bool {
	p := strings.ToLower(path)
	for _, f := range suspiciousFragments {
		if strings.Contains(p, f) {
			return true
		}
	}
	return false
}

// blockSuspicious answers 404 to scanner paths without reaching the router, and
// counts them on the observer's own series (one series, however many paths are
// tried). It does no I/O and starts no goroutine: a scan of a million paths costs
// a million string searches and nothing else.
func blockSuspicious(on bool, count func()) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if on && (isSuspiciousPath(string(c.Path())) || isSuspiciousPath(string(c.Request.URI().PathOriginal()))) {
			if count != nil {
				count()
			}
			reject(c, consts.StatusNotFound, CodeNotFound)
			return
		}
		c.Next(ctx)
	}
}

// ---- helpers ---------------------------------------------------------------.

var crlfStripper = strings.NewReplacer("\r", "", "\n", "", " ", "")

func stripCRLF(s string) string { return crlfStripper.Replace(s) }

// RandomNonce is a fresh unpredictable value, for a route that serves HTML and
// needs a per-response CSP nonce.
func RandomNonce() string { return utils.RandomToken(16) }
