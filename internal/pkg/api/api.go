package api

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/security"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// APIService is the HTTP server. It owns one Hertz engine, applies the security
// middleware in a fixed order to every route, and keeps the order of lifecycle
// calls (Start, Run, Stop) the other services use.
//
// The service is spread over a few files: this one is the lifecycle, api.mount.go
// the middleware chain and the profiling routes, api.routes.go the route table,
// api.stream.go the event streams.
type APIService struct {
	cfg      Config
	cache    cache.ICacheService
	security security.ISecurityService // for the authentication middleware to come
	obs      observability.IObserver
	log      *slog.Logger
	checks   []Check
	listener net.Listener

	ip     *clientIP
	h      *server.Hertz
	routes *routeTable
	slots  chan struct{} // one entry per open stream, MaxStreams deep

	// gate counts the requests being served and refuses new ones once Stop has
	// begun; drain ends the open streams at the same moment.
	gate        *utils.Gate
	drain       context.Context
	drainCancel context.CancelFunc

	mu      sync.Mutex
	started bool
	runDone chan struct{}
	runErr  atomic.Pointer[error]
	accepts *firstAccept // closed when the event loop takes its first connection
}

// firstAccept is a one-shot signal, raised from Hertz's OnAccept hook.
type firstAccept struct {
	once sync.Once
	c    chan struct{}
}

func newFirstAccept() *firstAccept { return &firstAccept{c: make(chan struct{})} }

func (f *firstAccept) raise() { f.once.Do(func() { close(f.c) }) }

// Option configures an APIService.
type Option func(*APIService)

// WithConfig sets the configuration. Without it the zero Config is used, which
// is the development profile: not Secured, any host, no origins.
func WithConfig(c Config) Option { return func(a *APIService) { a.cfg = c } }

// WithObserver gives the service an observer. Without one it observes nothing.
func WithObserver(o observability.IObserver) Option {
	return func(a *APIService) { a.obs = observability.Pick(o) }
}

// WithLogger sets the logger for security events (rejected hosts, panics). The
// default discards them.
func WithLogger(l *slog.Logger) Option { return func(a *APIService) { a.log = l } }

// WithReadiness adds dependencies to /readyz. The database, the cache and the
// storage belong here; the API reports their state and never their details.
func WithReadiness(checks ...Check) Option {
	return func(a *APIService) { a.checks = append(a.checks, checks...) }
}

// WithListener serves on an existing listener, for a test or a socket handed over
// by a supervisor. The configured Addr is then ignored.
func WithListener(l net.Listener) Option { return func(a *APIService) { a.listener = l } }

// NewAPIService creates the service. It validates nothing yet: Start does, so a
// bad configuration is reported where the process can act on it, and before a
// single request is accepted.
func NewAPIService(
	port int,
	cache cache.ICacheService,
	security security.ISecurityService,
	opts ...Option,
) IAPIService {
	a := &APIService{
		cache:    cache,
		security: security,
		obs:      observability.Nop(),
		log:      slog.New(slog.DiscardHandler),
		routes:   &routeTable{},
		gate:     utils.NewGate(nil),
	}
	a.drain, a.drainCancel = context.WithCancel(context.Background())
	if port > 0 {
		a.cfg.Addr = fmt.Sprintf("0.0.0.0:%d", port)
	}
	for _, opt := range opts {
		if opt != nil {
			opt(a)
		}
	}
	return a
}

// Start validates the configuration and builds the server. It returns an error
// for any unsafe setting, which is how the process refuses to run misconfigured.
func (a *APIService) Start(_ context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.started {
		return ErrAlreadyStarted
	}
	cfg := a.cfg.WithDefaults()
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("api configuration: %w", err)
	}
	a.cfg = cfg
	proxies, _ := parseProxies(cfg.TrustedProxies) // Validate has accepted them
	a.ip = newClientIP(proxies)
	a.slots = make(chan struct{}, cfg.MaxStreams)
	a.accepts = newFirstAccept()

	h := server.New(a.serverOptions()...)
	h.SetClientIPFunc(func(c *app.RequestContext) string { return a.ip.of(c) })
	a.mount(h)
	a.h = h
	a.gate.Open()
	a.started = true
	return nil
}

// serverOptions are the limits and timeouts of the Hertz server: what stops a
// slow or oversized request from holding the server (docs/api.md, Priority 1 #13).
func (a *APIService) serverOptions() []config.Option {
	cfg := a.cfg
	opts := []config.Option{
		server.WithMaxRequestBodySize(cfg.MaxBodyBytes),
		server.WithMaxHeaderBytes(cfg.MaxHeaderBytes),
		server.WithReadTimeout(cfg.ReadTimeout),
		server.WithWriteTimeout(cfg.WriteTimeout),
		server.WithIdleTimeout(cfg.IdleTimeout),
		server.WithExitWaitTime(cfg.ShutdownTimeout),
		server.WithHandleMethodNotAllowed(true),
		server.WithRedirectTrailingSlash(false), // a redirect must not move a request off the checked path
		server.WithRedirectFixedPath(false),
		server.WithDisablePrintRoute(true),
		server.WithKeepAlive(true),
		server.WithKeepAliveTimeout(cfg.IdleTimeout),
		// The request context ends when the client goes, so a handler stops working
		// for a caller that has left.
		server.WithSenseClientDisconnection(true),
		// The one reliable sign that the event loop is serving: Engine.IsRunning is
		// true as soon as the listener exists, before the loop does, and a Shutdown
		// that arrives in between finds nothing to stop.
		server.WithOnAccept(func(net.Conn) context.Context {
			a.accepts.raise()
			return context.Background()
		}),
	}
	if a.listener != nil {
		return append(opts, server.WithListener(a.listener))
	}
	return append(opts, server.WithHostPorts(cfg.Addr))
}

// Run serves until Stop. It returns once the server is accepting, nil on
// success, and the serve error if the server could not start.
func (a *APIService) Run(ctx context.Context) error {
	a.mu.Lock()
	if !a.started {
		a.mu.Unlock()
		return ErrNotStarted
	}
	if a.runDone != nil {
		a.mu.Unlock()
		return nil
	}
	a.runDone = make(chan struct{})
	h, done, accepts := a.h, a.runDone, a.accepts
	a.mu.Unlock()

	go func() {
		defer close(done)
		// Hertz's netpoll transport panics when its serve loop ends with an error,
		// which a listener closed under it can cause. That is a failed Run, not a
		// reason to take the process down.
		defer func() {
			if r := recover(); r != nil {
				err := fmt.Errorf("server stopped: %v", r)
				a.runErr.Store(&err)
			}
		}()
		if err := h.Run(); err != nil {
			a.runErr.Store(&err)
		}
	}()
	return a.awaitServing(ctx, h, done, accepts)
}

// dialAddr is where a client on this machine reaches the server.
func (a *APIService) dialAddr() string {
	addr := a.cfg.Addr
	if a.listener != nil {
		addr = a.listener.Addr().String()
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
		host = "127.0.0.1"
		if ip != nil && ip.To4() == nil {
			host = "::1"
		}
	}
	return net.JoinHostPort(host, port)
}

// awaitServing waits until the event loop has taken a connection, the serve
// loop ends first (a port in use, say), ctx ends, or ten seconds pass. Once the
// engine reports it is running it dials itself: a listener queues connections
// before anyone serves them, so only the loop's own OnAccept proves it is up.
func (a *APIService) awaitServing(ctx context.Context, h *server.Hertz, done <-chan struct{}, accepts *firstAccept) error {
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(5 * time.Millisecond)
	defer poll.Stop()
	var probe net.Conn
	defer func() {
		if probe != nil {
			_ = probe.Close()
		}
	}()
	for {
		select {
		case <-accepts.c:
			return nil
		case <-done:
			if p := a.runErr.Load(); p != nil {
				return fmt.Errorf("api: %w", *p)
			}
			return errors.New("api: server stopped before it began serving")
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return errors.New("api: server did not begin serving")
		case <-poll.C:
			if probe == nil && h.IsRunning() {
				d := net.Dialer{Timeout: time.Second}
				if c, err := d.DialContext(ctx, "tcp", a.dialAddr()); err == nil {
					probe = c
				}
			}
		}
	}
}

// awaitRefused returns once nothing accepts connections on the server's address,
// or ctx ends.
func (a *APIService) awaitRefused(ctx context.Context) {
	addr := a.dialAddr()
	for {
		d := net.Dialer{Timeout: 100 * time.Millisecond}
		c, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			return
		}
		_ = c.Close()
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Millisecond):
		}
	}
}

// Stop stops accepting connections and requests and waits for the requests in
// flight, for at most ShutdownTimeout or until ctx ends. It does not wait for
// idle keep-alive connections: they carry no request, and the next one they
// receive is refused and closed.
func (a *APIService) Stop(ctx context.Context) error {
	a.mu.Lock()
	if !a.started {
		a.mu.Unlock()
		return nil
	}
	h, done := a.h, a.runDone
	a.started = false
	a.mu.Unlock()

	a.drainCancel() // open streams end
	sctx, cancel := context.WithTimeout(ctx, a.cfg.ShutdownTimeout)
	defer cancel()
	if done != nil {
		// Shutdown closes the listener, then waits for every connection, idle ones
		// included. It runs on its own: what Stop waits for is the requests, below,
		// and it gives up when sctx does.
		go func() { _ = h.Shutdown(sctx) }()
	}
	err := a.gate.Close(sctx)
	if done == nil {
		return err
	}
	select {
	case <-done:
	case <-sctx.Done():
		err = cmp.Or(err, sctx.Err())
	}
	// The serve loop ends as soon as Shutdown starts, a moment before the
	// listener is closed. Wait for the port to refuse, so that a Stop that has
	// returned really leaves nothing accepting; the idle connections are not
	// waited for.
	if sctx.Err() == nil {
		a.awaitRefused(sctx)
	}
	a.mu.Lock()
	a.runDone = nil
	a.mu.Unlock()
	return err
}

// Handler is the Hertz server, for tests that exercise the whole middleware chain
// without a socket.
func (a *APIService) Handler() *server.Hertz {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.h
}

// draining counts every request into the gate, and once Stop has closed it
// refuses new ones and closes their connection, so a keep-alive client cannot
// keep using a server that is leaving. Entering the gate registers the request
// before it checks, so a request that arrives while Stop begins is either
// counted (and waited for) or refused, never lost.
func (a *APIService) draining() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		done, ok := a.gate.Enter()
		if !ok {
			c.SetConnectionClose()
			c.Header("Retry-After", "5")
			reject(c, consts.StatusServiceUnavailable, CodeShuttingDown)
			return
		}
		defer done()
		c.Next(ctx)
	}
}
