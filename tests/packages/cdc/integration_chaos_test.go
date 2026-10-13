//go:build integration

package cdc_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
)

// The consumer stops while rows keep changing in Postgres; a new instance in
// the same group picks up where it left off and no change is lost.
func TestIntegrationChaos_ConsumerRestartDuringLiveChanges(t *testing.T) {
	skipUnlessDebezium(t)
	prefix := fmt.Sprintf("chaos-%d-", runID)
	var in inbox
	a := newService()
	topic := a.Topic("public", "users")
	_ = a.Subscribe(ctxT(t), topic, func(ctx context.Context, k json.RawMessage, ev cdc.ChangeEvent) error {
		time.Sleep(30 * time.Millisecond)
		return in.handle(ctx, k, ev)
	})
	if err := a.Run(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	const n = 30
	for i := range n {
		psql(t, fmt.Sprintf(`INSERT INTO users(name) VALUES ('%s%d')`, prefix, i))
		if i == 5 {
			_ = a.Stop(context.Background()) // dies mid-stream
		}
	}
	b := service(t)
	_ = b.Subscribe(ctxT(t), topic, in.handle)
	in.waitUntil(t, "every insert", func(all []got) bool {
		seen := map[string]bool{}
		for _, m := range all {
			seen[m.Name] = true
		}
		for i := range n {
			if !seen[fmt.Sprintf("%s%d", prefix, i)] {
				return false
			}
		}
		return true
	}) // duplicates are allowed, gaps are not
	psql(t, fmt.Sprintf(`DELETE FROM users WHERE name LIKE '%s%%'`, prefix))
}

// The instance that dead-letters a poison change stops; the next one takes
// over and the poison change is on the DLQ exactly once (it was committed
// after being parked, so it is not replayed).
func TestIntegrationChaos_PoisonThenFailover(t *testing.T) {
	skipUnlessKafka(t)
	topic := names()
	var in inbox
	handler := func(ctx context.Context, k json.RawMessage, ev cdc.ChangeEvent) error {
		if string(ev.After) == `{"id":0,"name":"poison"}` {
			return errors.New("bad row")
		}
		return in.handle(ctx, k, ev)
	}
	a := newService()
	_ = a.Subscribe(ctxT(t), topic, handler)
	if err := a.Run(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	p := producer(t)
	_ = p.Publish(ctxT(t), topic, key(0), event(cdc.OpCreate, 0, "poison"))
	readAll(t, topic+".dlq", 1)
	_ = a.Stop(context.Background())
	b := service(t)
	_ = b.Subscribe(ctxT(t), topic, handler)
	for i := 1; i <= 5; i++ {
		_ = p.Publish(ctxT(t), topic, key(i), event(cdc.OpCreate, i, "ok"))
	}
	in.wait(t, 5)
	if n := countFor(t, topic+".dlq", 3*time.Second); n != 1 {
		t.Fatalf("poison change dead-lettered %d times, want 1", n)
	}
}
