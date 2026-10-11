package queue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hibiken/asynq"
	"github.com/robfig/cron/v3"
	"github.com/valkey-io/valkey-go"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

type QueueService struct {
	cache    cache.CacheServices
	inflight *utils.Inflight
	id       string   // this instance, for scheduler leadership
	routes   sync.Map // task type -> route
	cron     *cron.Cron
	leader   atomic.Bool
	started  atomic.Bool

	mu        sync.Mutex // serialises Start and Stop
	client    *asynq.Client
	worker    *asynq.Server
	stopLeads context.CancelFunc
}

// route is how one job type is run.
type route struct {
	handler Handler
	limit   int64
	per     time.Duration
	gate    *utils.Gate
}

// settings is what a list of Options adds up to.
type settings struct {
	enqueue []asynq.Option
	limit   int64
	per     time.Duration
	gate    *utils.Gate
}

type retryAfterError struct {
	after time.Duration
	err   error
}

func (e *retryAfterError) Error() string { return e.err.Error() }
func (e *retryAfterError) Unwrap() error { return e.err }

func NewQueueService(cache cache.CacheServices) QueueServices {
	id := make([]byte, 16)
	_, _ = rand.Read(id)
	return &QueueService{
		cache:    cache,
		inflight: utils.NewInflight(),
		id:       hex.EncodeToString(id),
		cron:     cron.New(),
	}
}

// Lifecycle

// Start connects to Valkey with the cache's settings and runs this
// instance's worker and scheduler. Calling it while running does nothing.
func (q *QueueService) Start(ctx context.Context) error {
	if q.cache == nil {
		return ErrNoCache
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.started.Load() {
		return nil
	}
	conn, err := q.conn()
	if err != nil {
		return err
	}
	client := asynq.NewClient(conn)
	worker := asynq.NewServer(conn, asynq.Config{
		Concurrency:              Concurrency,
		Queues:                   map[string]int{Critical: 6, Default: 3, Low: 1},
		RetryDelayFunc:           retryDelay,
		IsFailure:                func(err error) bool { _, later := errors.AsType[*retryAfterError](err); return !later },
		ShutdownTimeout:          ShutdownTimeout,
		TaskCheckInterval:        PollInterval,
		DelayedTaskCheckInterval: PollInterval,
		LogLevel:                 asynq.WarnLevel,
	})
	if err := client.Ping(); err != nil {
		_ = client.Close()
		return fmt.Errorf("queue: starting: %w", err)
	}
	q.client = client // before the worker starts: dispatch may requeue with it
	if err := worker.Start(asynq.HandlerFunc(q.dispatch)); err != nil {
		q.client = nil
		_ = client.Close()
		return fmt.Errorf("queue: starting: %w", err)
	}
	leadCtx, stopLeads := context.WithCancel(context.Background())
	go q.lead(leadCtx)
	q.cron.Start()
	q.worker, q.stopLeads = worker, stopLeads
	q.started.Store(true)
	return nil
}

// Stop refuses new jobs, stops the scheduler, and lets running jobs finish
// for up to ShutdownTimeout; unfinished ones go back for another instance.
// If ctx ends first it returns an error and finishes in the background.
func (q *QueueService) Stop(ctx context.Context) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.started.Swap(false) {
		return nil
	}
	q.cron.Stop()
	q.stopLeads()
	client, worker := q.client, q.worker
	done := make(chan struct{})
	go func() {
		worker.Shutdown()
		_ = q.inflight.Wait(context.Background())
		_ = client.Close()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("queue: stop: jobs still finishing: %w", ctx.Err())
	}
}

func (q *QueueService) Ping(ctx context.Context) error {
	if !q.started.Load() {
		return ErrNotStarted
	}
	if err := q.client.Ping(); err != nil {
		return err
	}
	return q.cache.Ping(ctx)
}

// conn builds the queue's connection from the cache's settings, on DB. A
// cluster has no numbered databases, so there it shares the keyspace.
func (q *QueueService) conn() (asynq.RedisConnOpt, error) {
	cfg := q.cache.Config()
	switch len(cfg.Addresses) {
	case 0:
		return nil, cache.ErrNoAddresses
	case 1:
		return asynq.RedisClientOpt{Addr: cfg.Addresses[0], Password: cfg.Password, DB: DB}, nil
	default:
		return asynq.RedisClusterClientOpt{Addrs: cfg.Addresses, Password: cfg.Password}, nil
	}
}

// Jobs

func (q *QueueService) Handle(taskType string, handler Handler, opts ...Option) {
	set := apply(opts)
	q.routes.Store(taskType, route{handler: handler, limit: set.limit, per: set.per, gate: set.gate})
}

func (q *QueueService) Enqueue(ctx context.Context, taskType string, payload any, opts ...Option) (string, error) {
	if !q.started.Load() {
		return "", ErrNotStarted
	}
	done := q.inflight.Track()
	defer done()
	if !q.started.Load() { // Stop may have begun after the first check
		return "", ErrNotStarted
	}
	data, err := encode(payload)
	if err != nil {
		return "", err
	}
	info, err := q.client.EnqueueContext(ctx, asynq.NewTask(taskType, data), apply(opts).enqueue...)
	if err != nil {
		return "", err
	}
	return info.ID, nil
}

// dispatch runs a job on this instance and honours RetryAfter.
func (q *QueueService) dispatch(ctx context.Context, t *asynq.Task) error {
	err := q.run(ctx, t)
	if later, ok := errors.AsType[*retryAfterError](err); ok {
		return q.later(ctx, t, later)
	}
	return err
}

// run runs a job. A job type with no handler here goes back for an instance
// that has one. A rate-limited type takes a slot in the current window
// first; over the limit, the job waits for the next window.
func (q *QueueService) run(ctx context.Context, t *asynq.Task) error {
	v, ok := q.routes.Load(t.Type())
	if !ok {
		return RetryAfter(UnhandledRetry, ErrNoHandler)
	}
	r := v.(route)
	if r.gate != nil {
		done, ok := r.gate.Enter()
		if !ok {
			return RetryAfter(GuardRetry, ErrClosed)
		}
		defer done()
	}
	if r.limit > 0 {
		now := time.Now()
		window := now.UnixNano() / int64(r.per)
		n, err := q.cache.Incr(ctx, RateLimitKeyPrefix+t.Type()+":"+strconv.FormatInt(window, 10), 2*r.per)
		if err != nil {
			return err
		}
		if n > r.limit {
			return RetryAfter(time.Unix(0, (window+1)*int64(r.per)).Sub(now), ErrRateLimited)
		}
	}
	return r.handler(ctx, t.Payload())
}

// later retries a RetryAfter job after its delay without counting a failure.
// asynq archives a job whose retries are used up even then, so such a job is
// queued again as a fresh copy (same queue and retry limit) instead.
func (q *QueueService) later(ctx context.Context, t *asynq.Task, e *retryAfterError) error {
	retried, _ := asynq.GetRetryCount(ctx)
	maxRetry, _ := asynq.GetMaxRetry(ctx)
	if retried < maxRetry {
		return e
	}
	name, _ := asynq.GetQueueName(ctx)
	_, err := q.client.EnqueueContext(ctx, asynq.NewTask(t.Type(), t.Payload()),
		asynq.Queue(name), asynq.MaxRetry(maxRetry), asynq.ProcessAt(runAt(e.after)))
	if err != nil {
		return e
	}
	return nil
}

// retryDelay honours RetryAfter, otherwise backs off exponentially.
func retryDelay(n int, err error, _ *asynq.Task) time.Duration {
	if later, ok := errors.AsType[*retryAfterError](err); ok {
		// asynq adds this to its own clock and stores whole seconds; the small
		// margin keeps that from rounding down to the second before runAt.
		return time.Until(runAt(later.after)) + 50*time.Millisecond
	}
	return min(RetryBaseDelay<<min(n, 20), RetryMaxDelay)
}

// runAt is the first whole second at least d from now. asynq stores run
// times in whole seconds, so this keeps a RetryAfter job from running early;
// it runs at most a second late.
func runAt(d time.Duration) time.Time {
	at := time.Now().Add(d)
	if whole := at.Truncate(time.Second); !whole.Equal(at) {
		at = whole.Add(time.Second)
	}
	return at
}

// Scheduling

func (q *QueueService) Schedule(spec, taskType string, payload any, opts ...Option) (string, error) {
	if q.cron == nil { // not built with NewQueueService
		return "", ErrNoCache
	}
	data, err := encode(payload)
	if err != nil {
		return "", err
	}
	id, err := q.cron.AddFunc(spec, func() {
		if !q.leader.Load() {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), EnqueueOnTick)
		defer cancel()
		_, _ = q.Enqueue(ctx, taskType, data, opts...)
	})
	if err != nil {
		return "", fmt.Errorf("queue: schedule %q: %w", spec, err)
	}
	return strconv.Itoa(int(id)), nil
}

func (q *QueueService) Unschedule(id string) error {
	n, err := strconv.Atoi(id)
	if err != nil || q.cron == nil || q.cron.Entry(cron.EntryID(n)).ID == 0 {
		return ErrNoSchedule
	}
	q.cron.Remove(cron.EntryID(n))
	return nil
}

var (
	// leadScript takes the leader lock if free or renews it if this
	// instance holds it; it returns 1 when this instance leads.
	leadScript = valkey.NewLuaScript(`
local cur = redis.call('GET', KEYS[1])
if cur == ARGV[1] then
	redis.call('PEXPIRE', KEYS[1], ARGV[2])
	return 1
end
if not cur then
	redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
	return 1
end
return 0`)

	// resignScript releases the leader lock only if this instance holds it.
	resignScript = valkey.NewLuaScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
	return redis.call('DEL', KEYS[1])
end
return 0`)
)

// lead keeps this instance's claim on scheduler leadership current until
// ctx ends, then resigns so another instance can take over at once.
func (q *QueueService) lead(ctx context.Context) {
	ttl := strconv.FormatInt(LeaderTTL.Milliseconds(), 10)
	tick := time.NewTicker(LeaderRenew)
	defer tick.Stop()
	for {
		client := q.cache.Client()
		leads := false
		if client != nil {
			n, err := leadScript.Exec(ctx, client, []string{LeaderKey}, []string{q.id, ttl}).AsInt64()
			leads = err == nil && n == 1
		}
		q.leader.Store(leads)
		select {
		case <-tick.C:
		case <-ctx.Done():
			q.leader.Store(false)
			if client := q.cache.Client(); client != nil {
				resign, cancel := context.WithTimeout(context.Background(), time.Second)
				_ = resignScript.Exec(resign, client, []string{LeaderKey}, []string{q.id}).Error()
				cancel()
			}
			return
		}
	}
}

// Options

func Priority(queue string) Option    { return enqueueOption(asynq.Queue(queue)) }
func MaxRetry(n int) Option           { return enqueueOption(asynq.MaxRetry(n)) }
func Timeout(d time.Duration) Option  { return enqueueOption(asynq.Timeout(d)) }
func Delay(d time.Duration) Option    { return enqueueOption(asynq.ProcessIn(d)) }
func Unique(ttl time.Duration) Option { return enqueueOption(asynq.Unique(ttl)) }

// RateLimit runs at most n jobs of the type per window, across every
// instance. Jobs over the limit wait for the next window. A count or window
// of zero or less means no limit.
func RateLimit(n int, per time.Duration) Option {
	return func(s *settings) {
		if n > 0 && per > 0 {
			s.limit, s.per = int64(n), per
		}
	}
}

// Guard runs the job type only while gate is open, counting each run in
// the gate so the service's Stop waits for it. While the gate is closed on
// this instance, jobs go back for another instance.
func Guard(gate *utils.Gate) Option {
	return func(s *settings) { s.gate = gate }
}

func enqueueOption(o asynq.Option) Option {
	return func(s *settings) { s.enqueue = append(s.enqueue, o) }
}

func apply(opts []Option) settings {
	var set settings
	for _, opt := range opts {
		if opt != nil {
			opt(&set)
		}
	}
	return set
}

// Helpers

// RetryAfter makes the queue run the job again after d without counting a
// failed attempt; err is kept for errors.Is.
func RetryAfter(d time.Duration, err error) error {
	if err == nil {
		err = ErrRetryLater
	}
	return &retryAfterError{after: d, err: err}
}

// JSON adapts a typed function to a Handler. A payload that does not decode
// into T is not retried.
func JSON[T any](fn func(context.Context, T) error) Handler {
	return func(ctx context.Context, payload []byte) error {
		var v T
		if err := json.Unmarshal(payload, &v); err != nil {
			return errors.Join(fmt.Errorf("queue: decoding payload: %w", err), ErrSkipRetry)
		}
		return fn(ctx, v)
	}
}

// NewJob registers fn for taskType and returns a Job that queues payloads
// for it with the same options.
func NewJob[T any](q QueueServices, taskType string, fn func(context.Context, T) error, opts ...Option) *Job[T] {
	q.Handle(taskType, JSON(fn), opts...)
	return &Job[T]{queue: q, taskType: taskType, opts: opts}
}

// Deliver queues payload to be run by the job's function on some instance.
func (j *Job[T]) Deliver(ctx context.Context, payload T) error {
	_, err := j.queue.Enqueue(ctx, j.taskType, payload, j.opts...)
	return err
}

// encode turns a payload into job bytes: []byte as is, anything else JSON.
func encode(payload any) ([]byte, error) {
	if b, ok := payload.([]byte); ok {
		return b, nil
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("queue: encoding payload: %w", err)
	}
	return b, nil
}
