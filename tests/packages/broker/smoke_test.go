//go:build integration

package broker_test

import (
	"context"
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
