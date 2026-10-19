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
	ErrJobPanicked     = errors.New("cron: job panicked")
)

type (
	// Option configures a CronService: WithObserver.
	Option func(*CronService)

	CronJob struct {
		JobID    string
		Schedule string
		TaskName string
		Payload  []byte
		Handler  queue.Handler
		Loc      *time.Location
	}

	ICronServices interface {
		Start(ctx context.Context) error
		Stop(ctx context.Context) error
		Run(ctx context.Context) error
		Ping(ctx context.Context) error

		CreateJob(data CronJob) error
		RemoveJob(id string) error

		ExecuteJob(ctx context.Context, taskName string, payload []byte) error
		ExecuteJobByID(ctx context.Context, id string, payload []byte) error

		SetLocation(loc *time.Location) error
	}
)
