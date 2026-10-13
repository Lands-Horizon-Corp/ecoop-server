package cdc

import (
	"context"
	"sync"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
	"github.com/twmb/franz-go/pkg/kgo"
)

type CDCService struct {
	brokers []string
	groupID string
	gate    *utils.Gate

	dlqSuffix string
	attempts  int
	backoff   time.Duration

	mu       sync.Mutex
	producer *kgo.Client
}

func NewCDCService(
	brokers []string, groupID string, inflight *utils.Inflight,
) CDCServices {
	return &CDCService{
		brokers: brokers,
		groupID: groupID,
		gate:    utils.NewGate(inflight),
	}
}

// Run implements [CDCServices].
func (c *CDCService) Run(ctx context.Context) error {
	panic("unimplemented")
}

// Stop implements [CDCServices].
func (c *CDCService) Stop(ctx context.Context) error {
	panic("unimplemented")
}

// Subscribe implements [CDCServices].
func (c *CDCService) Subscribe(ctx context.Context, topic string, handler func(ctx context.Context, key ChangeEvent, value ChangeEvent) error) error {
	panic("unimplemented")
}
