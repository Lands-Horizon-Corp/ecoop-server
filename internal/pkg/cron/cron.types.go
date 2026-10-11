package cron

import (
	"context"
	"errors"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/queue"
)

var (
	ErrNotStarted      = errors.New("cron: service not started")
	ErrNoTracker       = errors.New("cron: service has no in-flight tracker; create it with NewCronService")
	ErrNoQueue         = errors.New("cron: needs a queue service")
	ErrQueueDown       = errors.New("cron: queue unavailable")
	ErrNoJobID         = errors.New("cron: job id is empty")
	ErrNoTaskName      = errors.New("cron: task name is empty")
	ErrNoHandler       = errors.New("cron: job has no handler")
	ErrInvalidSchedule = errors.New("cron: invalid schedule")
	ErrJobExists       = errors.New("cron: a job with this id already exists")
	ErrTaskTaken       = errors.New("cron: another job already uses this task name")
	ErrJobNotFound     = errors.New("cron: no job with this id")
	ErrUnknownTask     = errors.New("cron: no job runs this task name")
	ErrNilLocation     = errors.New("cron: location is nil")
)

type (
	// CronJob runs Handler with Payload on Schedule: a standard cron spec
	// ("*/5 * * * *") or a descriptor ("@hourly", "@every 30s"), in Loc
	// (the service's location when nil). Every instance keeps the schedule;
	// only the queue's leader enqueues each tick, so it runs once per tick
	// across all instances, on whichever worker picks it up.
	CronJob struct {
		JobID    string
		Schedule string
		TaskName string
		Payload  []byte
		Handler  queue.Handler
		Loc      *time.Location
	}

	CronServices interface {
		// Start checks the queue and starts running jobs on this instance;
		// Run does the same. Stop refuses new runs and waits for running
		// handlers, or until ctx ends. Jobs stay defined across Stop and
		// Start; a tick delivered to a stopped instance goes back to the
		// queue for another one.
		Start(ctx context.Context) error
		Stop(ctx context.Context) error
		Run(ctx context.Context) error
		Ping(ctx context.Context) error

		// CreateJob defines a job (before or after Start); RemoveJob stops
		// scheduling it.
		CreateJob(data CronJob) error
		RemoveJob(id string) error

		// ExecuteJob queues one run now, outside the schedule. A nil
		// payload uses the job's own.
		ExecuteJob(ctx context.Context, taskName string, payload []byte) error
		ExecuteJobByID(ctx context.Context, id string, payload []byte) error

		// SetLocation sets the time zone for jobs without their own Loc,
		// rescheduling the ones already defined.
		SetLocation(loc *time.Location) error
	}
)
