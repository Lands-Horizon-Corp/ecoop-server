//go:build integration

package broker_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/broker"
)

// Messages with one key arrive in the order they were sent.
func TestHappy_OrderPerKey(t *testing.T) {
	t.Parallel()
	topic, group := names()
	b := service(t, group)
	var in inbox
	_ = b.Subscribe(ctxT(t), topic, in.handle)
	publish(t, b, topic, 20, "account-7")
	for i, m := range in.wait(t, 20) {
		if m != fmt.Sprintf("account-7=%d", i) {
			t.Fatalf("message %d = %q: out of order", i, m)
		}
	}
}

// Subscribed before Run (as an app does at boot), the subscription starts
// with Run and also gets what was published before its group first joined.
func TestHappy_SubscribeBeforeRun(t *testing.T) {
	t.Parallel()
	topic, group := names()
	sender := service(t, group+"-sender")
	publish(t, sender, topic, 3, "early")
	b := broker.NewBrokerService([]string{addr}, group, nil)
	var in inbox
	if err := b.Subscribe(ctxT(t), topic, in.handle); err != nil {
		t.Fatal(err)
	}
	if err := b.Run(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(context.Background())
	publish(t, sender, topic, 2, "late")
	in.wait(t, 5)
}
