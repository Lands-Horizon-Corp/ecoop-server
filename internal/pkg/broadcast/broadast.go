package broadcast

// import (
// 	"context"
// 	"fmt"

// 	"github.com/pusher/pusher-http-go/v5"
// )

// type BroadcasterService struct {
// 	client pusher.Client
// }

// // NewBroadcastService builds a Pusher-backed broadcaster. log is optional (it must already be started);
// // when set, every publish is traced and failures are logged with their channels and event.
// func NewBroadcastService(
// 	appID, key, secret, host, port string, log logger.LogContextService) BroadcasterServices {
// 	return &BroadcasterService{
// 		client: pusher.Client{
// 			AppID:  appID,
// 			Key:    key,
// 			Secret: secret,
// 			Host:   fmt.Sprintf("%s:%s", host, port),
// 		},
// 		log: log,
// 	}
// }

// // Broadcast implements [BroadcasterServices].
// func (b *BroadcasterService) Broadcast(channels []Channel, events Events, payload any) error {
// 	panic("unimplemented")
// }

// // Dispatch implements [BroadcasterServices].
// func (b *BroadcasterService) Dispatch(ctx context.Context, channels []string, event string, payload any) error {
// 	panic("unimplemented")
// }

// // Publish implements [BroadcasterServices].
// func (b *BroadcasterService) Publish(ctx context.Context, channel string, event string, payload any) error {
// 	panic("unimplemented")
// }

// // Run implements [BroadcasterServices].
// func (b *BroadcasterService) Run(ctx context.Context) error {
// 	panic("unimplemented")
// }

// // Runner implements [BroadcasterServices].
// func (b *BroadcasterService) Runner(ctx context.Context) {
// 	panic("unimplemented")
// }
