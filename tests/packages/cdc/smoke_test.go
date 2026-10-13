//go:build integration

package cdc_test

import (
	"context"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
)

// Smoke: a Debezium-shaped message goes in, a decoded change comes out.
func TestSmoke(t *testing.T) {
	t.Parallel()
	topic := names()
	s := service(t)
	var in inbox
	if err := s.Subscribe(ctxT(t), topic, in.handle); err != nil {
		t.Fatal(err)
	}
	if err := producer(t).Publish(ctxT(t), topic, key(7), event(cdc.OpCreate, 7, "Juan")); err != nil {
		t.Fatal(err)
	}
	m := in.wait(t, 1)[0]
	if m.Key != `{"id":7}` || m.Op != cdc.OpCreate || m.Name != "Juan" {
		t.Fatalf("received %+v", m)
	}
	if err := s.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// Topic follows the connector's naming, from CDC_TOPIC_PREFIX.
func TestSmoke_Topic(t *testing.T) {
	t.Parallel()
	if got := cdc.NewCDCService(nil, "", "acme").Topic("public", "users"); got != "acme.public.users" {
		t.Fatalf("Topic = %q", got)
	}
	if got := cdc.NewCDCService(nil, "", " ").Topic("public", "users"); got != "ecoop.public.users" {
		t.Fatalf("default Topic = %q", got)
	}
}
