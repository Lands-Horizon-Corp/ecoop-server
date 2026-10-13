//go:build integration

package broker_test

import (
	"context"
	"errors"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/broker"
	"testing"
)

// Smoke: run, subscribe, publish, receive, stop.
func TestSmoke(t *testing.T) {
	t.Parallel()
	topic, group := names()
	b := service(t, group)
	var in inbox
	if err := b.Subscribe(ctxT(t), topic, in.handle); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(ctxT(t), topic, []byte("loan-42"), []byte(`{"status":"approved"}`)); err != nil {
		t.Fatal(err)
	}
	if got := in.wait(t, 1)[0]; got != `loan-42={"status":"approved"}` {
		t.Fatalf("received %q", got)
	}
	if err := b.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// Smoke: a dead letter's topic is the original plus the default suffix.
func TestSmoke_DLQ(t *testing.T) {
	t.Parallel()
	topic, group := names()
	b := service(t, group)
	_ = b.Subscribe(ctxT(t), topic, func(context.Context, []byte, []byte) error { return errors.New("no") })
	_ = b.Publish(ctxT(t), topic, []byte("k"), []byte("v"))
	if dead := readDLQ(t, topic+broker.DLQSuffix); string(dead.Value) != "v" {
		t.Fatalf("dead letter = %q", dead.Value)
	}
}
