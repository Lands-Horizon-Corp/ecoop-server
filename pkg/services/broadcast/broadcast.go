package broadcast

type (
	Channel string
	Events  []string

	BroadcastService interface {
		Broadcast(channels []Channel, events Events, payload any) error
	}
)
