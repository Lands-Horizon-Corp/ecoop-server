//go:build integration

package broker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/broker"
)

// Needs the debezium and debezium-setup services (docker compose up -d
// --wait debezium && docker compose up debezium-setup). Names come from the
// same env as the compose file.
func TestDebezium_RowChangesArriveAsEvents(t *testing.T) {
	if skipReason != "" {
		t.Skip(skipReason)
	}
	prefix := envOr("DEBEZIUM_TOPIC_PREFIX", "ecoop")
	user, db := envOr("POSTGRES_USER", "ecoop"), envOr("POSTGRES_DB", "ecoop")
	topic := prefix + ".public.users"
	if out, err := exec.Command("docker", "compose", "ps", "--status", "running", "-q", "debezium").Output(); err != nil || len(out) == 0 {
		t.Skip("debezium is not running")
	}
	b := service(t, fmt.Sprintf("debezium-group-%d", runID))
	events := make(chan *broker.ChangeEvent, 16)
	if err := b.Subscribe(ctxT(t), topic, func(_ context.Context, _, v []byte) error {
		ev, err := broker.DecodeChangeEvent(v)
		if err != nil {
			return err
		}
		if ev != nil {
			events <- ev
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("cdc-%d", runID)
	sql := fmt.Sprintf(`INSERT INTO users(name) VALUES ('%[1]s'); UPDATE users SET name='%[1]s-2' WHERE name='%[1]s'; DELETE FROM users WHERE name='%[1]s-2';`, name)
	if out, err := exec.Command("docker", "compose", "exec", "-T", "postgres", "psql", "-U", user, "-d", db, "-c", sql).CombinedOutput(); err != nil {
		t.Fatalf("psql: %v\n%s", err, out)
	}
	want := map[string]bool{}
	var id int64
	deadline := time.After(60 * time.Second)
	for len(want) < 3 {
		select {
		case ev := <-events:
			// A delete's "before" holds only the primary key (default
			// replica identity), so match it by the id seen on create.
			var row struct {
				ID   int64
				Name string
			}
			raw := ev.After
			if ev.Op == broker.OpDelete {
				raw = ev.Before
			}
			_ = json.Unmarshal(raw, &row)
			switch {
			case ev.Op == broker.OpCreate && row.Name == name:
				id, want[ev.Op] = row.ID, true
			case ev.Op == broker.OpUpdate && row.Name == name+"-2",
				ev.Op == broker.OpDelete && id != 0 && row.ID == id:
				want[ev.Op] = true
			}
		case <-deadline:
			t.Fatalf("only saw ops %v", want)
		}
	}
	for _, op := range []string{broker.OpCreate, broker.OpUpdate, broker.OpDelete} {
		if !want[op] {
			t.Fatalf("missing op %q (saw %v)", op, want)
		}
	}
}

func TestDecodeChangeEvent(t *testing.T) {
	t.Parallel()
	if ev, err := broker.DecodeChangeEvent(nil); ev != nil || err != nil {
		t.Fatalf("tombstone = %v, %v", ev, err)
	}
	for _, v := range []string{`{"payload":{"op":"c","after":{"id":1}}}`, `{"op":"c","after":{"id":1}}`} {
		if ev, err := broker.DecodeChangeEvent([]byte(v)); err != nil || ev.Op != "c" {
			t.Fatalf("%s: %v %v", v, ev, err)
		}
	}
	for _, v := range []string{`{}`, `nope`} {
		if _, err := broker.DecodeChangeEvent([]byte(v)); err == nil {
			t.Fatalf("%s accepted", v)
		}
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
