package cron

import (
	"context"
	"time"

	"github.com/hibiken/asynq"
)

type (
	CronJob struct {
		JobID    string
		Schedule string
		TaskName string
		Payload  []byte
		Handler  func(context.Context, *asynq.Task) error
		Loc      *time.Location
	}

	CronServices interface {
		Start(ctx context.Context) error
		Stop(ctx context.Context) error
		Run(ctx context.Context) error
		Ping(ctx context.Context) error

		CreateJob(data CronJob) error
		RemoveJob(id string) error

		ExecuteJob(taskName string, payload []byte) error
		ExecuteJobByID(id string, payload []byte) error
		SetLocation(loc *time.Location) error
	}
)
