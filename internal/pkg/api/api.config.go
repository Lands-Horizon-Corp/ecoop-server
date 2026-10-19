package api

import (
	"errors"
	"fmt"
	"math"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Config is how the API is exposed. Every field has a safe default, and Validate
// refuses what is unsafe, so a misconfiguration stops the process at startup
// instead of weakening a control quietly. FromEnv builds one from the
// environment; docs/api.md says why each rule exists.
type Config struct {
	// Addr is the listen address, host:port. Default 0.0.0.0:8080.
	Addr string

	// Secured is production mode: HTTPS is required and redirected to, HSTS and
	// the full header set are sent, cookies are Secure, and pprof is never
	// registered. It must be true for any environment reachable from outside.
	Secured bool

	// AllowedOrigins are the browser origins (scheme://host[:port], no path, no
	// trailing slash) allowed to make credentialed cross-origin requests. One
	// environment, one list: never mix production with staging or development.
	// In Secured mode every origin must be https.
	AllowedOrigins []string
	// AllowedHosts are the Host header values the API answers to. They are kept
	// apart from AllowedOrigins on purpose: a browser origin and the name this
	// server is reached by are different things. Empty means "the hosts of
	// AllowedOrigins", which is only a convenience for a single-host deployment.
	AllowedHosts []string

	// TrustedProxies are the networks (CIDR, or a bare IP) whose
	// X-Forwarded-For / X-Real-IP / X-Forwarded-Proto are believed. Empty trusts
	// nobody: the peer address is the client, and forwarded headers are ignored.
	// Never put 0.0.0.0/0 here; Validate refuses it.
	TrustedProxies []string

	// MaxBodyBytes caps a request body; a larger one is answered 413 before any
	// handler runs. Default 1 MiB.
	MaxBodyBytes int
	// MaxHeaderBytes caps the request headers. Default 16 KiB.
	MaxHeaderBytes int

	// Timeouts. ReadTimeout covers the whole request; WriteTimeout the response
	// (streams need a long one: see StreamTimeout). Defaults are in Defaults.
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
	// ShutdownTimeout is how long Stop waits for in-flight requests. Default 30s.
	ShutdownTimeout time.Duration

	// RateLimit is requests per second per client; RateBurst the burst. Zero
	// disables the limiter, which Validate only allows when not Secured.
	RateLimit float64
	RateBurst int
	// RateWindow is how long an idle client's counter is kept. Default 3m.
	RateWindow time.Duration

	// Methods are the HTTP methods the API accepts. Default GET, POST, PUT,
	// PATCH, DELETE, HEAD, OPTIONS.
	Methods []string

	// EnablePprof registers the profiling routes. It is refused when Secured, and
	// even when not Secured the routes answer only to loopback clients.
	EnablePprof bool

	// BlockSuspiciousPaths answers 404 to and counts scanner paths (.env, .git,
	// wp-admin and the like) so one scan cannot reach the router.
	BlockSuspiciousPaths bool

	// MaxStreams caps the streams (RegisterSSR) open at once, across all routes.
	// A stream holds a connection and a goroutine for as long as it runs; past the
	// cap a new one is answered 503 with Retry-After. Default 256.
	MaxStreams int
	// StreamTimeout is the longest a stream may run. Default 10m. It is separate
	// from WriteTimeout, which bounds one write and not the stream's life.
	StreamTimeout time.Duration
}

// Defaults.
const (
	DefaultAddr            = "0.0.0.0:8080"
	DefaultMaxBodyBytes    = 1 << 20
	DefaultMaxHeaderBytes  = 16 << 10
	DefaultReadTimeout     = 15 * time.Second
	DefaultWriteTimeout    = 30 * time.Second
	DefaultIdleTimeout     = 60 * time.Second
	DefaultShutdownTimeout = 30 * time.Second
	DefaultRateLimit       = 10.0
	DefaultRateBurst       = 30
	DefaultRateWindow      = 3 * time.Minute
	DefaultStreamTimeout   = 10 * time.Minute
	DefaultMaxStreams      = 256

	maxRateKeys = 100_000 // clients tracked at once; past this the oldest idle ones go
)

// Errors returned by Validate.
var (
	ErrInvalidOrigin   = errors.New("api: invalid origin")
	ErrInsecureOrigin  = errors.New("api: an http:// origin is not allowed when Secured")
	ErrInvalidHost     = errors.New("api: invalid host")
	ErrInvalidProxy    = errors.New("api: invalid trusted proxy")
	ErrTrustEveryone   = errors.New("api: trusting every address as a proxy lets any client forge its IP")
	ErrNoOrigins       = errors.New("api: Secured needs at least one allowed origin")
	ErrPprofSecured    = errors.New("api: pprof cannot be enabled when Secured")
	ErrNoRateLimit     = errors.New("api: Secured needs a rate limit")
	ErrInvalidLimit    = errors.New("api: invalid limit")
	ErrInvalidMethod   = errors.New("api: invalid method")
	ErrInvalidAddr     = errors.New("api: invalid listen address")
	ErrWriteBeforeRead = errors.New("api: WriteTimeout is shorter than ReadTimeout")
)

var knownMethods = map[string]bool{"GET": true, "POST": true, "PUT": true, "PATCH": true, "DELETE": true, "HEAD": true, "OPTIONS": true}

// WithDefaults returns c with the zero fields filled in.
func (c Config) WithDefaults() Config {
	if c.Addr == "" {
		c.Addr = DefaultAddr
	}
	if c.MaxBodyBytes == 0 {
		c.MaxBodyBytes = DefaultMaxBodyBytes
	}
	if c.MaxHeaderBytes == 0 {
		c.MaxHeaderBytes = DefaultMaxHeaderBytes
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = DefaultReadTimeout
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = DefaultWriteTimeout
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = DefaultIdleTimeout
	}
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = DefaultShutdownTimeout
	}
	if c.RateWindow == 0 {
		c.RateWindow = DefaultRateWindow
	}
	if c.MaxStreams == 0 {
		c.MaxStreams = DefaultMaxStreams
	}
	if c.StreamTimeout == 0 {
		c.StreamTimeout = DefaultStreamTimeout
	}
	if c.RateLimit == 0 && c.RateBurst == 0 && c.Secured {
		c.RateLimit, c.RateBurst = DefaultRateLimit, DefaultRateBurst
	}
	if c.RateLimit > 0 && c.RateBurst == 0 {
		c.RateBurst = int(c.RateLimit) * 3
	}
	if len(c.Methods) == 0 {
		c.Methods = []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}
	}
	return c
}

// Validate checks the configuration without opening anything. It returns every
// problem at once, so one run of the program shows them all.
func (c Config) Validate() error {
	c = c.WithDefaults()
	var errs []error
	if _, _, err := net.SplitHostPort(c.Addr); err != nil {
		errs = append(errs, fmt.Errorf("%w: %q", ErrInvalidAddr, c.Addr))
	}
	if c.Secured && len(c.AllowedOrigins) == 0 {
		errs = append(errs, ErrNoOrigins)
	}
	for _, o := range c.AllowedOrigins {
		if _, err := normalizeOrigin(o, c.Secured); err != nil {
			errs = append(errs, err)
		}
	}
	for _, h := range c.AllowedHosts {
		if _, err := normalizeHost(h); err != nil {
			errs = append(errs, err)
		}
	}
	if _, err := parseProxies(c.TrustedProxies); err != nil {
		errs = append(errs, err)
	}
	if c.Secured && c.EnablePprof {
		errs = append(errs, ErrPprofSecured)
	}
	if c.Secured && c.RateLimit <= 0 {
		errs = append(errs, ErrNoRateLimit)
	}
	if math.IsNaN(c.RateLimit) || math.IsInf(c.RateLimit, 0) {
		errs = append(errs, fmt.Errorf("%w: the rate limit is not a finite number", ErrInvalidLimit))
	}
	if c.MaxBodyBytes < 0 || c.MaxHeaderBytes < 0 || c.RateLimit < 0 || c.RateBurst < 0 ||
		c.ReadTimeout < 0 || c.WriteTimeout < 0 || c.IdleTimeout < 0 || c.ShutdownTimeout < 0 ||
		c.MaxStreams < 0 || c.StreamTimeout < 0 {
		errs = append(errs, fmt.Errorf("%w: a limit or a timeout is negative", ErrInvalidLimit))
	}
	if c.WriteTimeout < c.ReadTimeout {
		errs = append(errs, ErrWriteBeforeRead)
	}
	for _, m := range c.Methods {
		if !knownMethods[strings.ToUpper(m)] {
			errs = append(errs, fmt.Errorf("%w: %q", ErrInvalidMethod, utils.Truncate(m, 16)))
		}
	}
	return errors.Join(errs...)
}

// normalizeOrigin returns the origin in canonical form: lower-case scheme and
// host, no path, no query, no trailing slash. A value that is not exactly an
// origin is refused, so "https://a.example/" and "https://a.example/x" are
// mistakes to fix and not two entries.
func normalizeOrigin(o string, secured bool) (string, error) {
	o = strings.TrimSpace(o)
	u, err := url.Parse(o)
	shown := utils.Truncate(o, 64)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") ||
		u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("%w: %q (want scheme://host[:port])", ErrInvalidOrigin, shown)
	}
	if strings.HasSuffix(o, "/") {
		return "", fmt.Errorf("%w: %q has a trailing slash", ErrInvalidOrigin, shown)
	}
	if secured && u.Scheme != "https" {
		return "", fmt.Errorf("%w: %q", ErrInsecureOrigin, shown)
	}
	if strings.Contains(u.Host, "*") {
		return "", fmt.Errorf("%w: %q (no wildcards)", ErrInvalidOrigin, shown)
	}
	return utils.ASCIILower(u.Scheme) + "://" + utils.ASCIILower(u.Host), nil
}

// normalizeHost returns a Host value in canonical form (lower case, host[:port]).
func normalizeHost(h string) (string, error) {
	h = utils.ASCIILower(strings.TrimSpace(h))
	if h == "" || strings.ContainsAny(h, "/\\ @?#*") || strings.Contains(h, "://") {
		return "", fmt.Errorf("%w: %q", ErrInvalidHost, utils.Truncate(h, 64))
	}
	if host, port, err := net.SplitHostPort(h); err == nil {
		if p, perr := strconv.Atoi(port); perr != nil || p < 1 || p > 65535 || host == "" {
			return "", fmt.Errorf("%w: %q", ErrInvalidHost, utils.Truncate(h, 64))
		}
	}
	return h, nil
}

// hostOf is the Host an origin implies: its host and port.
func hostOf(origin string) string {
	u, err := url.Parse(origin)
	if err != nil {
		return ""
	}
	return utils.ASCIILower(u.Host)
}

// parseProxies parses the trusted networks. A bare IP is a /32 or /128.
func parseProxies(list []string) ([]netip.Prefix, error) {
	var out []netip.Prefix
	var errs []error
	for _, s := range list {
		s = strings.TrimSpace(s)
		var p netip.Prefix
		var err error
		if strings.Contains(s, "/") {
			p, err = netip.ParsePrefix(s)
		} else {
			var a netip.Addr
			if a, err = netip.ParseAddr(s); err == nil {
				p = netip.PrefixFrom(a, a.BitLen())
			}
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("%w: %q", ErrInvalidProxy, utils.Truncate(s, 64)))
			continue
		}
		if p.Bits() == 0 {
			errs = append(errs, fmt.Errorf("%w: %q", ErrTrustEveryone, utils.Truncate(s, 64)))
			continue
		}
		out = append(out, p.Masked())
	}
	return out, errors.Join(errs...)
}

// FromEnv reads the API settings. lookup is os.LookupEnv in production.
//
//	API_ADDR                 listen address (default 0.0.0.0:8080)
//	API_SECURED              true in production
//	API_ALLOWED_ORIGINS      comma-separated origins, one environment's only
//	API_ALLOWED_HOSTS        comma-separated Host values (default: those of the origins)
//	API_TRUSTED_PROXIES      comma-separated CIDRs or IPs; empty trusts none
//	API_MAX_BODY_BYTES       default 1048576
//	API_MAX_STREAMS          streams open at once (default 256); API_STREAM_TIMEOUT their longest life (10m)
//	API_RATE_LIMIT           requests per second per client; API_RATE_BURST the burst
//	API_READ_TIMEOUT, API_WRITE_TIMEOUT, API_IDLE_TIMEOUT, API_SHUTDOWN_TIMEOUT
//	                         Go durations such as 15s
//	API_ENABLE_PPROF         true only on a developer machine
//	API_BLOCK_SUSPICIOUS     true to block scanner paths (default true)
func FromEnv(lookup func(string) (string, bool)) (Config, error) {
	get := func(k string) string { v, _ := lookup(k); return strings.TrimSpace(v) }
	var c Config
	var errs []error
	c.Addr = get("API_ADDR")
	c.Secured = strings.EqualFold(get("API_SECURED"), "true")
	c.EnablePprof = strings.EqualFold(get("API_ENABLE_PPROF"), "true")
	c.BlockSuspiciousPaths = !strings.EqualFold(get("API_BLOCK_SUSPICIOUS"), "false")
	c.AllowedOrigins = utils.SplitList(get("API_ALLOWED_ORIGINS"))
	c.AllowedHosts = utils.SplitList(get("API_ALLOWED_HOSTS"))
	c.TrustedProxies = utils.SplitList(get("API_TRUSTED_PROXIES"))
	num := func(k string, dst *int) {
		if v := get(k); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 {
				errs = append(errs, fmt.Errorf("%w: %s %q", ErrInvalidLimit, k, utils.Truncate(v, 32)))
				return
			}
			*dst = n
		}
	}
	dur := func(k string, dst *time.Duration) {
		if v := get(k); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil || d < 0 {
				errs = append(errs, fmt.Errorf("%w: %s %q (want a duration such as 15s)", ErrInvalidLimit, k, utils.Truncate(v, 32)))
				return
			}
			*dst = d
		}
	}
	num("API_MAX_BODY_BYTES", &c.MaxBodyBytes)
	num("API_MAX_STREAMS", &c.MaxStreams)
	num("API_RATE_BURST", &c.RateBurst)
	dur("API_STREAM_TIMEOUT", &c.StreamTimeout)
	dur("API_READ_TIMEOUT", &c.ReadTimeout)
	dur("API_WRITE_TIMEOUT", &c.WriteTimeout)
	dur("API_IDLE_TIMEOUT", &c.IdleTimeout)
	dur("API_SHUTDOWN_TIMEOUT", &c.ShutdownTimeout)
	if v := get("API_RATE_LIMIT"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || f < 0 || math.IsNaN(f) || math.IsInf(f, 0) {
			errs = append(errs, fmt.Errorf("%w: API_RATE_LIMIT %q", ErrInvalidLimit, utils.Truncate(v, 32)))
		} else {
			c.RateLimit = f
		}
	}
	if err := c.Validate(); err != nil {
		errs = append(errs, err)
	}
	return c.WithDefaults(), errors.Join(errs...)
}
