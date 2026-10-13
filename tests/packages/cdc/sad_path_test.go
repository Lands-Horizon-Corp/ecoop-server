//go:build integration

package cdc_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
)

func TestSad_Refusals(t *testing.T) {
	t.Parallel()
	skipUnlessKafka(t)
	topic := names()
	ctx := ctxT(t)
	idle := newService()
	var in inbox
	if err := idle.Subscribe(ctx, "  ", nil); !errors.Is(err, cdc.ErrNoTopic) || !errors.Is(err, cdc.ErrNoHandler) {
		t.Errorf("Subscribe(empty) = %v", err)
	}
	s := service(t)
	_ = s.Subscribe(ctx, topic, in.handle)
	if err := s.Subscribe(ctx, topic, in.handle); err == nil {
		t.Error("subscribed to the same topic twice")
	}
}

func TestSad_Decode(t *testing.T) {
	t.Parallel()
	for _, v := range []string{`{}`, `nope`, `[1]`, `{"payload":{"x":1}}`} {
		if _, err := cdc.Decode([]byte(v)); !errors.Is(err, cdc.ErrNotChangeEvent) {
			t.Errorf("Decode(%s) = %v", v, err)
		}
	}
}

// Missing or unreachable settings are reported by Run and Subscribe.
func TestSad_Settings(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := cdc.NewCDCService(nil, "g", "").Run(ctx); !errors.Is(err, cdc.ErrNoBrokers) {
		t.Errorf("no brokers: %v", err)
	}
	if err := cdc.NewCDCService([]string{"127.0.0.1:1"}, "g", "").Run(ctx); !errors.Is(err, cdc.ErrConnect) {
		t.Errorf("unreachable: %v", err)
	}
	var in inbox
	if err := cdc.NewCDCService(nil, "", "").Subscribe(ctx, "t", in.handle); !errors.Is(err, cdc.ErrNoGroup) {
		t.Errorf("no group: %v", err)
	}
}
