//go:build integration

package cdc_test

import (
	"fmt"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
)

// Changes to one row arrive in the order they happened, and the schema
// wrapper Debezium can add is accepted too.
func TestHappy_OrderPerRowAndWrapper(t *testing.T) {
	t.Parallel()
	topic := names()
	s := service(t)
	var in inbox
	_ = s.Subscribe(ctxT(t), topic, in.handle)
	p := producer(t)
	_ = p.Publish(ctxT(t), topic, key(1), event(cdc.OpCreate, 1, "a"))
	_ = p.Publish(ctxT(t), topic, key(1), event(cdc.OpUpdate, 1, "b"))
	_ = p.Publish(ctxT(t), topic, key(1), []byte(fmt.Sprintf(`{"schema":{},"payload":%s}`, event(cdc.OpDelete, 1, "b"))))
	got := in.wait(t, 3)
	for i, op := range []string{cdc.OpCreate, cdc.OpUpdate, cdc.OpDelete} {
		if got[i].Op != op {
			t.Fatalf("change %d = %q, want %q", i, got[i].Op, op)
		}
	}
}

// A delete's tombstone is skipped, not delivered as an event.
func TestHappy_TombstoneSkipped(t *testing.T) {
	t.Parallel()
	topic := names()
	s := service(t)
	var in inbox
	_ = s.Subscribe(ctxT(t), topic, in.handle)
	p := producer(t)
	_ = p.Publish(ctxT(t), topic, key(1), event(cdc.OpDelete, 1, ""))
	_ = p.Publish(ctxT(t), topic, key(1), nil) // tombstone
	_ = p.Publish(ctxT(t), topic, key(2), event(cdc.OpCreate, 2, "after"))
	if got := in.wait(t, 2); got[1].ID != 2 || len(got) != 2 {
		t.Fatalf("received %+v", got)
	}
}

// Realtime: rows written to Postgres reach the handler as create, update
// and delete while the service runs.
func TestHappy_Realtime(t *testing.T) {
	skipUnlessDebezium(t)
	s := service(t)
	var in inbox
	if err := s.Subscribe(ctxT(t), s.Topic("public", "users"), in.handle); err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("rt-%d", runID)
	psql(t, fmt.Sprintf(`INSERT INTO users(name) VALUES ('%[1]s'); UPDATE users SET name='%[1]s-2' WHERE name='%[1]s'; DELETE FROM users WHERE name='%[1]s-2'`, name))
	var id int64
	in.waitUntil(t, "create, update, delete", func(all []got) bool {
		seen := map[string]bool{}
		for _, m := range all {
			switch {
			case m.Op == cdc.OpCreate && m.Name == name:
				id, seen[m.Op] = m.ID, true
			case m.Op == cdc.OpUpdate && m.Name == name+"-2":
				seen[m.Op] = true
			case m.Op == cdc.OpDelete && id != 0 && m.ID == id:
				seen[m.Op] = true
			}
		}
		return len(seen) == 3
	})
}
