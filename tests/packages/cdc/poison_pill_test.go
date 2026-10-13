//go:build integration

package cdc_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
)

// Garbage on a CDC topic and a handler that rejects a row are parked on the
// topic's DLQ; the rows around them are still delivered.
func TestPoison_GarbageAndRejectedRowsAreParked(t *testing.T) {
	t.Parallel()
	topic := names()
	s := service(t)
	var in inbox
	_ = s.Subscribe(ctxT(t), topic, func(ctx context.Context, k json.RawMessage, ev cdc.ChangeEvent) error {
		if string(ev.After) == `{"id":2,"name":"bad"}` {
			return errors.New("rejected by ledger")
		}
		return in.handle(ctx, k, ev)
	})
	p := producer(t)
	_ = p.Publish(ctxT(t), topic, key(1), event(cdc.OpCreate, 1, "ok"))
	_ = p.Publish(ctxT(t), topic, key(9), []byte("<<not json>>"))
	_ = p.Publish(ctxT(t), topic, key(2), event(cdc.OpCreate, 2, "bad"))
	_ = p.Publish(ctxT(t), topic, key(3), event(cdc.OpCreate, 3, "ok"))
	if got := in.wait(t, 2); got[0].ID != 1 || got[1].ID != 3 {
		t.Fatalf("received %+v", got)
	}
	dead := readAll(t, topic+".dlq", 2)
	if string(dead[0].Value) != "<<not json>>" || string(dead[1].Key) != `{"id":2}` {
		t.Fatalf("dead letters = %s / %s", dead[0].Value, dead[1].Key)
	}
}

// A parked change carries its origin, reason and attempts.
func TestPoison_DLQHeaders(t *testing.T) {
	t.Parallel()
	topic := names()
	s := service(t)
	_ = s.Subscribe(ctxT(t), topic, func(context.Context, json.RawMessage, cdc.ChangeEvent) error { panic("boom") })
	_ = producer(t).Publish(ctxT(t), topic, key(5), event(cdc.OpCreate, 5, "x"))
	dead := readAll(t, topic+".dlq", 1)[0]
	if header(dead, cdc.HeaderDLQTopic) != topic || header(dead, cdc.HeaderDLQAttempts) != "3" ||
		!strings.Contains(header(dead, cdc.HeaderDLQError), "panicked") ||
		!strings.HasSuffix(header(dead, cdc.HeaderDLQGroup), "."+topic) || string(dead.Key) != `{"id":5}` {
		t.Fatalf("headers = %+v", dead.Headers)
	}
}
