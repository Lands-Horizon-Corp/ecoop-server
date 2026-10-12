//go:build integration

package broker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/broker"
)

func TestSad_Refusals(t *testing.T) {
	t.Parallel()
	topic, group := names()
	ctx := ctxT(t)
	if err := broker.NewBrokerService(nil, group, nil).Run(ctx); !errors.Is(err, broker.ErrNoBrokers) {
		t.Errorf("no brokers: %v", err)
	}
	short, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := broker.NewBrokerService([]string{"127.0.0.1:1"}, group, nil).Run(short); !errors.Is(err, broker.ErrConnect) {
		t.Errorf("unreachable: %v", err)
	}
	idle := broker.NewBrokerService([]string{addr}, "", nil)
	if err := idle.Publish(ctx, topic, nil, []byte("x")); !errors.Is(err, broker.ErrNotRunning) {
		t.Errorf("publish before Run: %v", err)
	}
	err := idle.Subscribe(ctx, "", nil)
	for _, want := range []error{broker.ErrNoTopic, broker.ErrNoHandler, broker.ErrNoGroup} {
		if !errors.Is(err, want) {
			t.Errorf("Subscribe(empty) = %v, missing %v", err, want)
		}
	}
	b := service(t, group)
	var in inbox
	_ = b.Subscribe(ctx, topic, in.handle)
	if err := b.Subscribe(ctx, topic, in.handle); !errors.Is(err, broker.ErrDuplicateSubscription) {
		t.Errorf("subscribe twice: %v", err)
	}
	if err := b.Publish(ctx, "  ", nil, nil); !errors.Is(err, broker.ErrNoTopic) {
		t.Errorf("empty topic: %v", err)
	}
}
