package broadcast

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/logger"
)

var channelName = regexp.MustCompile(`^[-a-zA-Z0-9_=@,.;]+$`)

// StatusError is a non-2xx answer from the server.
type StatusError struct {
	Code int
	Body string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("broadcast: server returned %d: %s", e.Code, e.Body)
}

// BroadcasterService sends events to a Pusher-protocol server (Sockudo, Pusher)
// over its signed HTTP API. Every call takes a context, so it joins the
// caller's trace and stops when the context ends. Publish and Dispatch deliver
// synchronously; Broadcast queues and returns, and the workers started by Run
// deliver it.
type BroadcasterService struct {
	appID, key, secret, host string
	secure                   bool
	http                     *http.Client
	log                      logger.LoggerService
	workers                  int
	queueSize                int
	maxRetries               int
	retryBackoff             time.Duration
	timeout                  time.Duration
	maxPayload               int

	mu       sync.RWMutex
	running  bool
	queue    chan job
	runCtx   context.Context
	cancel   context.CancelFunc
	runners  sync.WaitGroup
	inflight sync.WaitGroup

	sent     metric.Int64Counter
	duration metric.Float64Histogram
	depth    metric.Int64UpDownCounter
}

type job struct {
	ctx      context.Context // caller's trace, detached from its cancellation
	channels []string
	event    string
	data     string
	queuedAt time.Time
}

// NewBroadcastService builds a broadcaster for the server at host:port (port may
// be empty). log must be started before Run.
func NewBroadcastService(appID, key, secret, host, port string, log logger.LoggerService, opts ...Option) BroadcasterServices {
	addr := host
	if port != "" {
		addr = host + ":" + port
	}
	s := &BroadcasterService{
		appID: appID, key: key, secret: secret, host: addr,
		log:          log,
		workers:      defaultWorkers,
		queueSize:    defaultQueueSize,
		maxRetries:   defaultMaxRetries,
		retryBackoff: defaultRetryBackoff,
		timeout:      defaultTimeout,
		maxPayload:   defaultMaxPayloadKB * 1024,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Run implements [BroadcasterServices]. It checks the credentials against the
// server, then starts the background workers.
func (s *BroadcasterService) Run(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return ErrAlreadyRunning
	}
	if s.log == nil {
		return fmt.Errorf("%w: logger is nil", ErrInvalidArgument)
	}
	if s.appID == "" || s.key == "" || s.secret == "" || s.host == "" {
		return fmt.Errorf("%w: app id, key, secret and host are required", ErrInvalidArgument)
	}
	if s.workers < 1 || s.queueSize < 1 || s.timeout <= 0 || s.maxPayload <= 0 || s.maxRetries < 0 || s.retryBackoff < 0 {
		return fmt.Errorf("%w: workers, queue size, timeout and payload limit must be positive; retries and backoff not negative", ErrInvalidArgument)
	}
	if err := s.initMetrics(); err != nil {
		return err
	}
	s.http = &http.Client{Timeout: s.timeout}
	if err := s.log.InjectHTTPClient(s.http); err != nil {
		return fmt.Errorf("broadcast: instrument http client: %w", err)
	}

	ctx, log := s.log.NewContext(ctx).Trace("broadcast.connect", attribute.String("server.address", s.host))
	defer log.Span().End()
	if err := s.request(ctx, http.MethodGet, "/apps/"+s.appID+"/channels", nil); err != nil {
		log.Error(err, "broadcast server unreachable or credentials rejected", "host", s.host)
		return fmt.Errorf("broadcast: connect to %s: %w", s.host, err)
	}

	s.queue = make(chan job, s.queueSize)
	s.runCtx, s.cancel = context.WithCancel(trace.ContextWithSpanContext(context.WithoutCancel(ctx), trace.SpanContext{}))
	for range s.workers {
		s.runners.Go(func() {
			s.Runner(s.runCtx)
		})
	}
	s.running = true
	log.Success("broadcaster ready", "host", s.host, "workers", s.workers)
	return nil
}

// Stop implements [BroadcasterServices]. Queued broadcasts are delivered first;
// if ctx ends before that, in-flight deliveries are cancelled, the rest are
// dropped and the count is in the error.
func (s *BroadcasterService) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return ErrNotRunning
	}
	s.running = false
	s.mu.Unlock()

	s.inflight.Wait()
	close(s.queue)

	done := make(chan struct{})
	go func() {
		s.runners.Wait()
		close(done)
	}()
	select {
	case <-done:
		s.cancel()
		return nil
	case <-ctx.Done():
		dropped := len(s.queue)
		s.cancel()
		<-done
		return fmt.Errorf("broadcast: stop: %d queued broadcasts dropped: %w", dropped, ctx.Err())
	}
}

// Runner implements [BroadcasterServices]. It delivers queued broadcasts until
// the queue is closed by Stop or ctx ends. Run already starts the configured
// number of runners; call it yourself only to add more.
func (s *BroadcasterService) Runner(ctx context.Context) {
	s.mu.RLock()
	queue := s.queue
	s.mu.RUnlock()
	if queue == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case j, ok := <-queue:
			if !ok {
				return
			}
			s.depth.Add(ctx, -1)
			s.deliverQueued(ctx, j)
		}
	}
}

// Publish implements [BroadcasterServices].
func (s *BroadcasterService) Publish(ctx context.Context, channel, event string, payload any) error {
	return s.send(ctx, "publish", []string{channel}, event, payload)
}

// Dispatch implements [BroadcasterServices]. More than 100 channels are sent in batches of 100.
func (s *BroadcasterService) Dispatch(ctx context.Context, channels []string, event string, payload any) error {
	return s.send(ctx, "dispatch", channels, event, payload)
}

// Broadcast implements [BroadcasterServices]. Each event is queued for every
// channel and delivered in the background under ctx's trace (ctx's
// cancellation is ignored, so the request may finish first). It returns
// ErrQueueFull instead of blocking.
func (s *BroadcasterService) Broadcast(ctx context.Context, channels []Channel, events Events, payload any) error {
	names := make([]string, len(channels))
	for i, c := range channels {
		names[i] = string(c)
	}
	if len(events) == 0 {
		return fmt.Errorf("%w: no events", ErrInvalidArgument)
	}
	for _, e := range events {
		if err := validate(names, e); err != nil {
			return err
		}
	}
	data, err := s.encode(payload)
	if err != nil {
		return err
	}

	s.mu.RLock()
	if !s.running {
		s.mu.RUnlock()
		return ErrNotRunning
	}
	s.inflight.Add(1)
	s.mu.RUnlock()
	defer s.inflight.Done()

	detached := context.WithoutCancel(ctx)
	for i, e := range events {
		select {
		case s.queue <- job{ctx: detached, channels: names, event: e, data: data, queuedAt: time.Now()}:
			s.depth.Add(ctx, 1)
		default:
			s.record(ctx, "broadcast", e, "queue_full", 0)
			return fmt.Errorf("%w: queued %d of %d events", ErrQueueFull, i, len(events))
		}
	}
	return nil
}

func (s *BroadcasterService) send(ctx context.Context, method string, channels []string, event string, payload any) error {
	if err := validate(channels, event); err != nil {
		return err
	}
	data, err := s.encode(payload)
	if err != nil {
		return err
	}
	s.mu.RLock()
	if !s.running {
		s.mu.RUnlock()
		return ErrNotRunning
	}
	s.inflight.Add(1)
	s.mu.RUnlock()
	defer s.inflight.Done()

	ctx, log := s.log.NewContext(ctx).Trace("broadcast."+method+" "+event, spanAttrs(channels, event)...)
	defer log.Span().End()
	return s.deliver(ctx, log, method, channels, event, data)
}

func (s *BroadcasterService) deliverQueued(runCtx context.Context, j job) {
	// Child of the caller's trace, but cancelled when the service shuts down.
	ctx, cancel := context.WithCancel(j.ctx)
	defer cancel()
	defer context.AfterFunc(runCtx, cancel)()

	ctx, log := s.log.NewContext(ctx).Trace("broadcast.broadcast "+j.event,
		append(spanAttrs(j.channels, j.event), attribute.Float64("broadcast.queue_wait_seconds", time.Since(j.queuedAt).Seconds()))...)
	defer log.Span().End()
	_ = s.deliver(ctx, log, "broadcast", j.channels, j.event, j.data) // already logged; no caller to return to
}

// deliver sends one event to all channels (in batches), retrying transient failures.
func (s *BroadcasterService) deliver(ctx context.Context, log logger.LoggerLevel, method string, channels []string, event, data string) error {
	start := time.Now()
	var err error
	for attempt := 0; ; attempt++ {
		if err = s.trigger(ctx, channels, event, data); err == nil {
			s.record(ctx, method, event, "success", time.Since(start))
			log.Debug("event sent", "event", event, "channels", len(channels), "attempts", attempt+1)
			return nil
		}
		if !retryable(ctx, err) || attempt >= s.maxRetries {
			break
		}
		log.Warn("send failed, retrying", "event", event, "attempt", attempt+1, "error", err)
		select {
		case <-ctx.Done():
		case <-time.After(s.retryBackoff << attempt):
		}
	}
	result := "error"
	if ctx.Err() != nil {
		result = "interrupted"
		err = errors.Join(err, ctx.Err())
	}
	s.record(ctx, method, event, result, time.Since(start))
	log.Error(err, "send failed", "event", event, "channels", len(channels), "outcome", result)
	return fmt.Errorf("broadcast: %s %s: %w", method, event, err)
}

func (s *BroadcasterService) trigger(ctx context.Context, channels []string, event, data string) error {
	var errs []error
	for i := 0; i < len(channels); i += maxChannelsPerTrigger {
		batch := channels[i:min(i+maxChannelsPerTrigger, len(channels))]
		body, err := json.Marshal(struct {
			Name     string   `json:"name"`
			Channels []string `json:"channels"`
			Data     string   `json:"data"`
		}{event, batch, data})
		if err != nil {
			return fmt.Errorf("broadcast: encode body: %w", err)
		}
		if err := s.request(ctx, http.MethodPost, "/apps/"+s.appID+"/events", body); err != nil {
			errs = append(errs, fmt.Errorf("channels %d-%d: %w", i, i+len(batch)-1, err))
		}
	}
	return errors.Join(errs...)
}

// request sends a call signed the way the Pusher HTTP API requires:
// auth_signature = HMAC-SHA256(secret, "METHOD\nPATH\nsorted query").
func (s *BroadcasterService) request(ctx context.Context, method, path string, body []byte) error {
	q := url.Values{}
	q.Set("auth_key", s.key)
	q.Set("auth_timestamp", strconv.FormatInt(time.Now().Unix(), 10))
	q.Set("auth_version", "1.0")
	if body != nil {
		sum := md5.Sum(body)
		q.Set("body_md5", hex.EncodeToString(sum[:]))
	}
	mac := hmac.New(sha256.New, []byte(s.secret))
	mac.Write([]byte(method + "\n" + path + "\n" + q.Encode()))
	q.Set("auth_signature", hex.EncodeToString(mac.Sum(nil)))

	scheme := "http"
	if s.secure {
		scheme = "https"
	}
	req, err := http.NewRequestWithContext(ctx, method, scheme+"://"+s.host+path+"?"+q.Encode(), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("broadcast: build request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return &StatusError{Code: resp.StatusCode, Body: string(msg)}
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// encode turns a payload into the event's data string, as the Pusher SDKs do:
// strings and bytes are sent as-is, anything else as JSON.
func (s *BroadcasterService) encode(payload any) (string, error) {
	var data string
	switch p := payload.(type) {
	case nil:
		data = ""
	case string:
		data = p
	case []byte:
		data = string(p)
	case json.RawMessage:
		data = string(p)
	default:
		b, err := json.Marshal(p)
		if err != nil {
			return "", fmt.Errorf("%w: payload is not JSON-encodable: %v", ErrInvalidArgument, err)
		}
		data = string(b)
	}
	if len(data) > s.maxPayload {
		return "", fmt.Errorf("%w: payload is %d bytes, limit is %d", ErrInvalidArgument, len(data), s.maxPayload)
	}
	return data, nil
}

// retryable reports network errors, 5xx and 429 as transient. Nothing is
// retried once the caller's context has ended.
func retryable(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	if se, ok := errors.AsType[*StatusError](err); ok {
		return se.Code >= 500 || se.Code == http.StatusTooManyRequests
	}
	var nerr net.Error
	var uerr *url.Error
	return errors.As(err, &nerr) || errors.As(err, &uerr)
}

func validate(channels []string, event string) error {
	if len(channels) == 0 {
		return fmt.Errorf("%w: no channels", ErrInvalidArgument)
	}
	if event == "" || len(event) > maxNameLength {
		return fmt.Errorf("%w: event name must be 1-%d characters", ErrInvalidArgument, maxNameLength)
	}
	for _, c := range channels {
		if len(c) > maxNameLength || !channelName.MatchString(c) {
			return fmt.Errorf("%w: invalid channel name %q", ErrInvalidArgument, c)
		}
	}
	return nil
}

func spanAttrs(channels []string, event string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("broadcast.event", event),
		attribute.Int("broadcast.channel_count", len(channels)),
		attribute.StringSlice("broadcast.channels", channels[:min(len(channels), 10)]),
	}
}

func (s *BroadcasterService) record(ctx context.Context, method, event, outcome string, d time.Duration) {
	set := metric.WithAttributes(
		attribute.String("broadcast.method", method),
		attribute.String("broadcast.event", event),
		attribute.String("outcome", outcome),
	)
	s.sent.Add(ctx, 1, set)
	if d > 0 {
		s.duration.Record(ctx, d.Seconds(), set)
	}
}

func (s *BroadcasterService) initMetrics() error {
	m := s.log.Meter()
	var err error
	if s.sent, err = m.Int64Counter("broadcast.events",
		metric.WithDescription("Events sent to the broadcast server, by method, event and outcome.")); err != nil {
		return fmt.Errorf("broadcast: metric: %w", err)
	}
	if s.duration, err = m.Float64Histogram("broadcast.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Time to deliver an event, including retries.")); err != nil {
		return fmt.Errorf("broadcast: metric: %w", err)
	}
	if s.depth, err = m.Int64UpDownCounter("broadcast.queue.depth",
		metric.WithDescription("Broadcast events waiting for delivery.")); err != nil {
		return fmt.Errorf("broadcast: metric: %w", err)
	}
	return nil
}
