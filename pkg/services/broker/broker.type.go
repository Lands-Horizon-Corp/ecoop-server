package broker

import "context"

type (
	MessageBrokerServices interface {
		Run(ctx context.Context) error
		Stop(ctx context.Context) error
		Publish(ctx context.Context, topic string, key, value []byte) error
		Subscribe(ctx context.Context, topic string, handler func(key, value []byte) error) error
	}
)
