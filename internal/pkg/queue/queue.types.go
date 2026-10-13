package queue

import (
	"context"
	"errors"
	"time"

	"github.com/hibiken/asynq"
)

// Queues, served by priority weight by every instance's worker.
const (
	Critical = "critical" // OTPs, password resets: users are waiting
	Default  = "default"  // notifications, emails
	Low      = "low"      // reports, cleanups, cron housekeeping
)

const (
	DB              = 1                      // Valkey database for jobs, apart from cached data (DB 0)
	Concurrency     = 10                     // jobs run at once per instance
	RetryBaseDelay  = time.Second            // first retry delay; doubles each attempt
	RetryMaxDelay   = 10 * time.Minute       // longest retry delay
	ShutdownTimeout = 10 * time.Second       // Stop waits this long for running jobs
	PollInterval    = 250 * time.Millisecond // how often the worker checks for new and retried jobs
	UnhandledRetry  = 5 * time.Second        // retry delay for a job type this instance has no handler for
)

// Scheduling. Every instance keeps the schedules, but only the leader
// enqueues cron ticks, so each tick runs once however many instances run.
// Leadership is a lock in the cache that the leader keeps renewing; if the
// leader dies, another instance takes over within LeaderTTL.
const (
	LeaderKey     = "queue:scheduler:leader"
	LeaderTTL     = 15 * time.Second
	LeaderRenew   = 5 * time.Second
	EnqueueOnTick = 10 * time.Second // deadline for enqueueing one cron tick
	GuardRetry    = time.Second      // retry delay for a job whose Guard gate is closed here
)

// RateLimitKeyPrefix names the cache counters behind RateLimit; one counter
// per job type per window, shared by every instance.
const RateLimitKeyPrefix = "queue:rate:"

var (
	ErrNotStarted  = errors.New("queue: service not started")
	ErrNoCache     = errors.New("queue: needs a cache service")
	ErrNoHandler   = errors.New("queue: no handler for this job type on this instance")
	ErrNoSchedule  = errors.New("queue: no schedule with this id")
	ErrRateLimited = errors.New("queue: rate limit reached")
	ErrRetryLater  = errors.New("queue: retry later")
	ErrClosed      = errors.New("queue: the job's service is stopped on this instance")
	// ErrSkipRetry, wrapped in a handler's error, stops further attempts.
	ErrSkipRetry = asynq.SkipRetry
)

type (
	// Handler runs one job with its payload. Return nil when done, an error
	// to retry with backoff, RetryAfter to retry later without counting a
	// failure, or an error wrapping ErrSkipRetry to give up. JSON wraps a
	// typed function as a Handler.
	Handler func(ctx context.Context, payload []byte) error

	// Option configures jobs: how each one is queued (Priority, MaxRetry,
	// Timeout, Delay, Unique) and how a job type is run (RateLimit). Every
	// call takes all of them and uses the ones that apply to it.
	Option func(*settings)

	// ServiceOption configures a QueueService: Namespace.
	ServiceOption func(*QueueService)

	// Job is a typed job: its type, handler and options set up once with
	// NewJob, then queued with Deliver.
	Job[T any] struct {
		queue    IQueueService
		taskType string
		opts     []Option
	}

	// IQueueService is a job queue stored in Valkey, so it works behind a
	// load balancer: any instance can enqueue, any instance's worker runs
	// the next job, and a job interrupted by a shutdown goes back for
	// another instance.
	IQueueService interface {
		// Start runs this instance's worker and scheduler; Stop shuts them
		// down, letting running jobs finish for up to ShutdownTimeout.
		Start(ctx context.Context) error
		Stop(ctx context.Context) error
		Ping(ctx context.Context) error

		// Handle sets what this instance does with jobs of taskType. Call it
		// before or after Start; a later call replaces the earlier handler.
		Handle(taskType string, handler Handler, opts ...Option)

		// Enqueue queues a job and returns its ID. payload is JSON-encoded
		// ([]byte is sent as is).
		Enqueue(ctx context.Context, taskType string, payload any, opts ...Option) (id string, err error)

		// Schedule enqueues the job on a cron spec ("*/5 * * * *",
		// "@every 1h") and returns an ID for Unschedule. Only the leader
		// instance enqueues ticks, so each tick runs once.
		Schedule(spec, taskType string, payload any, opts ...Option) (id string, err error)
		Unschedule(id string) error
	}
)
