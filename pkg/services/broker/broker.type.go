package broker

import "context"

type (
	MessageBrokerServices interface {
		Run(ctx context.Context) error
		Stop(ctx context.Context) error
		Publish(ctx context.Context, topic string, key, value []byte) error
		Subscribe(ctx context.Context, topic string, handler func(key, value []byte) error) error
	}

	Message struct {
		Key, Value []byte
		Topic      string
		Partition  int32
		Offset     int64
	}

	BatchBrokerServices interface {
		MessageBrokerServices
		PublishBatch(ctx context.Context, topic string, msgs []Message) error
		Enqueue(ctx context.Context, topic string, key, value []byte) error
		Flush(ctx context.Context) error
		SubscribeBatch(ctx context.Context, topic string, handler func(batch []Message) error) error
	}
)
