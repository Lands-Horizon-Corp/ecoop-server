package broadcast

import "context"

type (
	Channel string
	Events  []string

	// BroadcastService is the part of the broadcaster that event producers such as CQRS need.
	BroadcastService interface {
		Broadcast(channels []Channel, events Events, payload any) error
	}

	BroadcasterServices interface {
		BroadcastService
		Run(ctx context.Context) error
		Publish(ctx context.Context, channel, event string, payload any) error
		Dispatch(ctx context.Context, channels []string, event string, payload any) error
		Runner(ctx context.Context)
	}
)
