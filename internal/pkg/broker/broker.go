package broker

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

type BrokerService struct {
	producer *kgo.Client
}

func NewBrokerService(
	brokers []string, groupID string,
) BrokerServices {
	return &BrokerService{}
}

func (b *BrokerService) Publish(ctx context.Context, topic string, key []byte, value []byte) error {
	panic("unimplemented")
}

func (b *BrokerService) Run(ctx context.Context) error {
	panic("unimplemented")
}

func (b *BrokerService) Stop(ctx context.Context) error {
	panic("unimplemented")
}

func (b *BrokerService) Subscribe(ctx context.Context, topic string, handler func(ctx context.Context, key []byte, value []byte) error) error {
	panic("unimplemented")
}
