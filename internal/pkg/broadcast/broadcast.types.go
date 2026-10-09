package broadcast

import "context"

type (
	Channel string
	Events  []string

	BroadcasterServices interface {
		Run(ctx context.Context) error
		Publish(ctx context.Context, channel, event string, payload any) error
		Dispatch(ctx context.Context, channels []string, event string, payload any) error
		Broadcast(channels []Channel, events Events, payload any) error
		Runner(ctx context.Context)
	}
)
