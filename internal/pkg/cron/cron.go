package cron

import (
	"context"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/queue"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

type CronService struct {
	gate  *utils.Gate
	queue queue.QueueServices
}

func NewCronService(
	queue queue.QueueServices,
	inflight *utils.Inflight,
) CronServices {
	return &CronService{
		queue: queue,
		gate:  utils.NewGate(inflight),
	}
}

// CreateJob implements [CronServices].
func (c *CronService) CreateJob(data CronJob) error {
	panic("unimplemented")
}

// ExecuteJob implements [CronServices].
func (c *CronService) ExecuteJob(taskName string, payload []byte) error {
	panic("unimplemented")
}

// ExecuteJobByID implements [CronServices].
func (c *CronService) ExecuteJobByID(id string, payload []byte) error {
	panic("unimplemented")
}

// Ping implements [CronServices].
func (c *CronService) Ping(ctx context.Context) error {
	panic("unimplemented")
}

// RemoveJob implements [CronServices].
func (c *CronService) RemoveJob(id string) error {
	panic("unimplemented")
}

// Run implements [CronServices].
func (c *CronService) Run(ctx context.Context) error {
	panic("unimplemented")
}

// SetLocation implements [CronServices].
func (c *CronService) SetLocation(loc *time.Location) error {
	panic("unimplemented")
}

// Start implements [CronServices].
func (c *CronService) Start(ctx context.Context) error {
	panic("unimplemented")
}

// Stop implements [CronServices].
func (c *CronService) Stop(ctx context.Context) error {
	panic("unimplemented")
}
