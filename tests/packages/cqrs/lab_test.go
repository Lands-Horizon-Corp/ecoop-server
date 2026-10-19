//go:build integration

package cqrs_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cqrs"
)

// A lab is one pair of databases and one running service shared by many
// subtests. Creating a database pair costs a second or two; resetting one costs
// a few milliseconds, which is what makes thousands of scenarios affordable.
// Subtests of a lab run one after another (they share the databases); labs run
// in parallel with each other, at most labSlots at a time so that the Postgres
// servers never see more connections than they allow.

var labSlots = make(chan struct{}, 5)

type lab struct {
	t     *testing.T
	p     *pair
	parts int
	group string
	f     *fakeCDC
	svc   svcT
}

func newLab(t *testing.T, parts int, mods ...func(*cqrsSvc)) *lab {
	t.Helper()
	skipUnlessPostgres(t)
	labSlots <- struct{}{}
	t.Cleanup(func() { <-labSlots })
	p := newPair(t, 6)
	l := &lab{t: t, p: p, parts: parts, group: fmt.Sprintf("lab-%d-%d", runID%1_000_000, seqNo.Add(1))}
	l.f = newFakeCDC(l.group)
	l.svc = newService(t, p, l.f, mods...)
	fakes.Store(l.svc, l.f)
	start(t, l.svc)
	return l
}

// reset puts both databases, the checkpoints and the fake Kafka back to empty.
func (l *lab) reset(t testing.TB) {
	t.Helper()
	// A subtest that failed part way may have left a fault behind. Heal both
	// databases first, so the next subtest starts from a healthy pair.
	setReadOnly(t, l.p, false)
	setWriteReadOnly(t, l.p, false)
	_, _ = readAdmin.Exec(fmt.Sprintf(`ALTER DATABASE %q WITH ALLOW_CONNECTIONS true`, l.p.readDB))
	_, _ = writeAdmin.Exec(fmt.Sprintf(`ALTER DATABASE %q WITH ALLOW_CONNECTIONS true`, l.p.writeDB))
	killReadBackends(t, l.p, 0)
	settle(t, l.p)
	settleWrite(t, l.p)
	l.p.exec(l.p.read, `ALTER TABLE accounts DROP CONSTRAINT IF EXISTS balance_ok`)
	l.p.exec(l.p.write, `TRUNCATE accounts`)
	l.p.exec(l.p.read, `TRUNCATE accounts, cqrs_checkpoints, cqrs_row_state`)
	l.f.reset()
}

// run is a subtest with a fresh world over the reset databases.
func (l *lab) run(name string, fn func(t *testing.T, r *rig)) {
	l.t.Helper()
	l.t.Run(name, func(t *testing.T) {
		l.reset(t)
		r := &rig{t: t, p: l.p, w: newWorld(t, l.p, l.parts), f: l.f, svc: l.svc, group: l.group, base: l.svc.Stats()}
		fn(t, r)
	})
}

// stats is what the service did since this subtest began.
func (r *rig) stats() cqrs.Stats { return statsSince(r.svc.Stats(), r.base) }

func statsSince(now, then cqrs.Stats) cqrs.Stats {
	return cqrs.Stats{
		Batches: now.Batches - then.Batches, Records: now.Records - then.Records, Applied: now.Applied - then.Applied,
		Duplicates: now.Duplicates - then.Duplicates, Tombstones: now.Tombstones - then.Tombstones,
		Skipped: now.Skipped - then.Skipped, DeadLettered: now.DeadLettered - then.DeadLettered,
		Retries: now.Retries - then.Retries, Stale: now.Stale - then.Stale, CommitFails: now.CommitFails - then.CommitFails,
		Hooks: now.Hooks - then.Hooks, HooksDropped: now.HooksDropped - then.HooksDropped, LastError: now.LastError,
	}
}

// reset forgets what Kafka had committed and what was parked.
func (f *fakeCDC) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.committed = map[int32]int64{}
	f.dead, f.causes, f.deadErr = nil, nil, nil
}

// ---- more world operations --------------------------------------------------

// mutate changes every row of [lo, hi] with a SET clause and emits the updates.
func (w *world) mutate(lo, hi int64, set string, args ...any) []cdc.Record {
	w.t.Helper()
	w.exec(`UPDATE accounts SET `+set+`, version = version + 1, updated_at = clock_timestamp() WHERE id BETWEEN ? AND ?`,
		append(args, lo, hi)...)
	return w.emitRange(cdc.OpUpdate, lo, hi)
}

// insertMissing creates the rows of [lo, hi] that do not exist yet and emits
// their creates; rows that exist are left alone.
func (w *world) insertMissing(lo, hi int64, owner string) []cdc.Record {
	w.t.Helper()
	var ids []int64
	err := w.p.write.Client().NewRaw(`INSERT INTO accounts (id, owner, balance, version, updated_at)
		SELECT g, CAST(? AS text) || g, g, 1, clock_timestamp() FROM generate_series(CAST(? AS bigint), CAST(? AS bigint)) g
		WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.id = g) RETURNING id`, owner, lo, hi).Scan(context.Background(), &ids)
	if err != nil {
		w.t.Fatalf("insertMissing: %v", err)
	}
	made := make(map[int64]bool, len(ids))
	for _, id := range ids {
		made[id] = true
	}
	var out []cdc.Record
	for _, r := range w.jsonRows(`id BETWEEN ? AND ?`, lo, hi) {
		if made[r.ID] {
			out = append(out, w.emit(r.ID, cdc.OpCreate, "null", r.J))
		}
	}
	return out
}

// bulkRemoveAll deletes every row, emitting the deletes.
func (w *world) bulkRemoveAll() []cdc.Record {
	w.t.Helper()
	var ids []int64
	if err := w.p.write.Client().NewRaw(`SELECT min(id) AS lo FROM accounts`).Scan(context.Background(), &ids); err != nil || len(ids) == 0 {
		return nil
	}
	var hi []int64
	_ = w.p.write.Client().NewRaw(`SELECT max(id) AS hi FROM accounts`).Scan(context.Background(), &hi)
	if len(hi) == 0 {
		return nil
	}
	return w.bulkRemove(ids[0], hi[0])
}

// inject adds a record that is not a change event to a partition's log.
func (w *world) inject(part int32, rec cdc.Record) {
	w.mu.Lock()
	defer w.mu.Unlock()
	rec.Partition = part
	rec.Offset = int64(len(w.log[part]))
	w.log[part] = append(w.log[part], rec)
}

// garbage injects a record that cannot be decoded.
func (w *world) garbage(part int32) {
	w.inject(part, poison(part, 0))
}

// replaySuffix has the connector re-emit everything from offset from to the end
// of each partition: what a restart from an earlier flushed position does.
func (w *world) replaySuffix(fromFraction float64) {
	for part := range w.parts {
		n := w.size(part)
		from := int64(float64(n) * fromFraction)
		if from < n {
			w.duplicate(part, from, n)
		}
	}
}

// logRow is a row as the event log carries it (row_to_json of the write table).
type logRow struct {
	ID        int64     `json:"id"`
	Owner     string    `json:"owner"`
	Balance   int64     `json:"balance"`
	Note      *string   `json:"note"`
	Active    bool      `json:"active"`
	UpdatedAt time.Time `json:"updated_at"`
	Version   int64     `json:"version"`
}

// finalFromLog is the state the event log implies once every record has been
// applied in order: what the read database must hold, computable even while the
// write database cannot be reached.
func (w *world) finalFromLog() map[int64]Account {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := map[int64]Account{}
	for _, l := range w.log {
		for _, r := range l {
			if r.Event == nil {
				continue
			}
			switch r.Event.Op {
			case cdc.OpDelete:
				var k struct {
					ID int64 `json:"id"`
				}
				_ = json.Unmarshal(r.Key, &k)
				delete(out, k.ID)
			case cdc.OpCreate, cdc.OpUpdate, cdc.OpRead:
				var lr logRow
				if err := json.Unmarshal(r.Event.After, &lr); err != nil {
					continue
				}
				out[lr.ID] = Account{ID: lr.ID, Owner: lr.Owner, Balance: lr.Balance, Note: lr.Note, Active: lr.Active, UpdatedAt: lr.UpdatedAt, Version: lr.Version}
			}
		}
	}
	return out
}

// ---- scripts ----------------------------------------------------------------

type script struct {
	name string
	run  func(w *world)
}

func scripts() []script {
	return []script{
		{"empty", func(w *world) {}},
		{"insert-only", func(w *world) { w.bulkInsert(1, 40, "a") }},
		{"insert-update", func(w *world) { w.bulkInsert(1, 40, "a"); w.bulkUpdate(1, 40, 3) }},
		{"insert-delete", func(w *world) { w.bulkInsert(1, 40, "a"); w.bulkRemove(10, 25) }},
		{"full-cycle", func(w *world) {
			w.bulkInsert(1, 40, "a")
			w.bulkUpdate(1, 40, 2)
			w.bulkRemove(30, 40)
			w.bulkInsert(100, 10, "b")
		}},
		{"recreate", func(w *world) {
			w.bulkInsert(1, 20, "a")
			w.bulkRemove(1, 20)
			w.bulkInsert(1, 20, "again")
			w.bulkUpdate(1, 20, 5)
		}},
		{"hot-keys", func(w *world) {
			w.bulkInsert(1, 4, "hot")
			for range 25 {
				w.bulkUpdate(1, 4, 1)
			}
		}},
		{"churn", func(w *world) {
			for round := range 4 {
				base := int64(round*20 + 1)
				w.bulkInsert(base, 15, "c")
				w.bulkUpdate(base, base+14, 1)
				w.bulkRemove(base, base+6)
			}
		}},
		{"nulls-and-empty", func(w *world) {
			w.bulkInsert(1, 20, "n")
			w.mutate(1, 10, "note = NULL")
			w.mutate(5, 15, "note = ''")
			w.mutate(10, 20, "note = 'x', active = true")
			w.mutate(1, 20, "owner = ''")
		}},
		{"unicode-and-big", func(w *world) {
			w.bulkInsert(1, 12, "u")
			w.mutate(1, 4, "owner = ?", "Zoë 日本語 🎉 ñ")
			w.mutate(5, 8, "owner = ?", strings.Repeat("长", 3000))
			w.mutate(9, 12, "owner = ?", `quote's "double" \back\slash ; -- ? % _ {} [] <>`)
		}},
		{"single-rows", func(w *world) {
			for i := int64(1); i <= 30; i++ {
				w.insert(i, "s", i)
				if i%2 == 0 {
					w.set(i, "s2", i*2)
				}
				if i%5 == 0 {
					w.remove(i)
				}
			}
		}},
		{"delete-all", func(w *world) { w.bulkInsert(1, 30, "d"); w.bulkRemove(1, 30) }},
		{"reinsert-elsewhere", func(w *world) {
			w.bulkInsert(1, 30, "a")
			w.bulkRemove(1, 30)
			w.bulkInsert(31, 30, "b")
			w.bulkUpdate(31, 60, 1)
		}},
		{"sparse-big-ids", func(w *world) {
			for _, id := range []int64{7, 1000003, 9999999999, 42, 8589934592} {
				w.insert(id, "big", id%1000)
			}
			w.set(1000003, "changed", 5)
			w.remove(7)
		}},
		{"extreme-balances", func(w *world) {
			w.bulkInsert(1, 10, "x")
			w.mutate(1, 5, "balance = 9223372036854775807")
			w.mutate(6, 10, "balance = -9223372036854775808")
			w.mutate(3, 8, "balance = 0")
		}},
	}
}

// ---- a lab-wide semaphore-free helper for subtests that need a pump ----------

// parallelLabs runs fns as parallel parent tests, one lab each.
func labTest(t *testing.T, parts int, mods []func(*cqrsSvc), body func(l *lab)) {
	t.Parallel()
	l := newLab(t, parts, mods...)
	body(l)
}

var _ = sync.Mutex{}
var _ = time.Second
