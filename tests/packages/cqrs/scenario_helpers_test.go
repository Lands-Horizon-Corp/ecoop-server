//go:build integration

package cqrs_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cqrs"
)

// The scenario suites check one thing above all: after the worst that can
// happen, the write database and the read database hold the same rows. To make
// that meaningful, nothing here is a hand-made expectation. A world changes the
// write database for real and builds every event from the row Postgres then
// holds (what a connector would emit), so "in sync" means the read database
// really equals the write database, column for column.

type jrow struct {
	ID int64  `bun:"id"`
	J  string `bun:"j"`
}

// world is the write side: it changes the write database and keeps the log of
// events that a connector and Kafka would hold for it, per partition.
type world struct {
	t     testing.TB
	p     *pair
	parts int32

	mu    sync.Mutex
	log   map[int32][]cdc.Record
	clock int64
}

func newWorld(t testing.TB, p *pair, parts int) *world {
	return &world{t: t, p: p, parts: int32(parts), log: map[int32][]cdc.Record{}}
}

func (w *world) partOf(id int64) int32 { return int32(id % int64(w.parts)) }

func (w *world) exec(query string, args ...any) {
	w.t.Helper()
	if _, err := w.p.write.Client().ExecContext(context.Background(), query, args...); err != nil {
		w.t.Fatalf("write db %q: %v", query, err)
	}
}

// tryExec is exec that reports the error instead of failing the test: for
// writes made while the write database is being failed over.
func (w *world) tryExec(query string, args ...any) error {
	_, err := w.p.write.Client().ExecContext(context.Background(), query, args...)
	return err
}

// retry runs fn until it succeeds, as an application retries a write across a
// failover.
func (w *world) retry(fn func() error) {
	w.t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for {
		err := fn()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			w.t.Fatalf("the write database never accepted the write: %v", err)
		}
		time.Sleep(40 * time.Millisecond)
	}
}

// insertEventually inserts a row, retrying across outages (a retry after an
// unknown commit outcome must not fail on the row that did commit), then emits
// the change.
func (w *world) insertEventually(id int64, owner string, balance int64) cdc.Record {
	w.t.Helper()
	w.retry(func() error {
		return w.tryExec(`INSERT INTO accounts (id, owner, balance, version, updated_at) VALUES (?, ?, ?, 1, clock_timestamp()) ON CONFLICT (id) DO NOTHING`, id, owner, balance)
	})
	var rows []jrow
	w.retry(func() error {
		return w.p.write.Client().NewRaw(`SELECT id, row_to_json(a)::text AS j FROM accounts a WHERE id = ?`, id).Scan(context.Background(), &rows)
	})
	return w.emit(id, cdc.OpCreate, "null", rows[0].J)
}

func (w *world) jsonRows(where string, args ...any) []jrow {
	w.t.Helper()
	var rows []jrow
	q := `SELECT id, row_to_json(a)::text AS j FROM accounts a WHERE ` + where + ` ORDER BY id`
	if err := w.p.write.Client().NewRaw(q, args...).Scan(context.Background(), &rows); err != nil {
		w.t.Fatalf("write db %q: %v", q, err)
	}
	return rows
}

func (w *world) emit(id int64, op cdc.ChangeType, before, after string) cdc.Record {
	w.mu.Lock()
	defer w.mu.Unlock()
	part := w.partOf(id)
	w.clock++
	// The WAL position only ever grows, whichever partition a change lands in.
	r := event(part, int64(len(w.log[part])), op, id, before, after,
		at(baseTime.Add(time.Duration(w.clock)*time.Millisecond)), lsn(1_000+w.clock*16))
	w.log[part] = append(w.log[part], r)
	return r
}

func (w *world) tomb(id int64) cdc.Record {
	w.mu.Lock()
	defer w.mu.Unlock()
	part := w.partOf(id)
	r := cdc.Record{Topic: testTopic, Partition: part, Offset: int64(len(w.log[part])), Key: json.RawMessage(`{"id":` + itoa(id) + `}`)}
	w.log[part] = append(w.log[part], r)
	return r
}

func (w *world) emitRange(op cdc.ChangeType, lo, hi int64) []cdc.Record {
	var out []cdc.Record
	for _, r := range w.jsonRows(`id BETWEEN ? AND ?`, lo, hi) {
		out = append(out, w.emit(r.ID, op, "null", r.J))
	}
	return out
}

// insert, set and remove change one row.
func (w *world) insert(id int64, owner string, balance int64) cdc.Record {
	w.t.Helper()
	w.exec(`INSERT INTO accounts (id, owner, balance, version, updated_at) VALUES (?, ?, ?, 1, clock_timestamp())`, id, owner, balance)
	return w.emit(id, cdc.OpCreate, "null", w.jsonRows(`id = ?`, id)[0].J)
}

func (w *world) set(id int64, owner string, balance int64) cdc.Record {
	w.t.Helper()
	w.exec(`UPDATE accounts SET owner = ?, balance = ?, version = version + 1, updated_at = clock_timestamp() WHERE id = ?`, owner, balance, id)
	return w.emit(id, cdc.OpUpdate, "null", w.jsonRows(`id = ?`, id)[0].J)
}

func (w *world) remove(id int64) []cdc.Record {
	w.t.Helper()
	before := w.jsonRows(`id = ?`, id)[0].J
	w.exec(`DELETE FROM accounts WHERE id = ?`, id)
	return []cdc.Record{w.emit(id, cdc.OpDelete, before, "null"), w.tomb(id)}
}

// bulkInsert creates n rows from id `from` in one statement.
func (w *world) bulkInsert(from int64, n int, owner string) []cdc.Record {
	w.t.Helper()
	w.exec(`INSERT INTO accounts (id, owner, balance, version, updated_at)
		SELECT g, CAST(? AS text) || g, g, 1, clock_timestamp() FROM generate_series(CAST(? AS bigint), CAST(? AS bigint)) g`,
		owner, from, from+int64(n)-1)
	return w.emitRange(cdc.OpCreate, from, from+int64(n)-1)
}

// bulkUpdate changes every row in [lo, hi] once.
func (w *world) bulkUpdate(lo, hi, delta int64) []cdc.Record {
	w.t.Helper()
	w.exec(`UPDATE accounts SET balance = balance + ?, version = version + 1, updated_at = clock_timestamp() WHERE id BETWEEN ? AND ?`, delta, lo, hi)
	return w.emitRange(cdc.OpUpdate, lo, hi)
}

// bulkRemove deletes every row in [lo, hi].
func (w *world) bulkRemove(lo, hi int64) []cdc.Record {
	w.t.Helper()
	var out []cdc.Record
	for _, r := range w.jsonRows(`id BETWEEN ? AND ?`, lo, hi) {
		out = append(out, w.emit(r.ID, cdc.OpDelete, r.J, "null"), w.tomb(r.ID))
	}
	w.exec(`DELETE FROM accounts WHERE id BETWEEN ? AND ?`, lo, hi)
	return out
}

func (w *world) slice(part int32, from int64, max int) []cdc.Record {
	w.mu.Lock()
	defer w.mu.Unlock()
	l := w.log[part]
	if from >= int64(len(l)) {
		return nil
	}
	end := min(int(from)+max, len(l))
	return append([]cdc.Record(nil), l[from:end]...)
}

func (w *world) size(part int32) int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return int64(len(w.log[part]))
}

func (w *world) total() int64 {
	var n int64
	for part := range w.parts {
		n += w.size(part)
	}
	return n
}

// changes is how many records carry a change (tombstones do not).
func (w *world) changes() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, l := range w.log {
		for _, r := range l {
			if r.Event != nil {
				n++
			}
		}
	}
	return n
}

// deliverNew hands f every record it has not been handed, from its committed
// offsets on, in batches of at most size; f commits what its handler accepted.
func (w *world) deliverNew(ctx context.Context, f *fakeCDC, size int) error {
	for part := range w.parts {
		for {
			batch := w.slice(part, f.committedOffset(part), size)
			if len(batch) == 0 {
				break
			}
			if err := f.deliver(ctx, batch); err != nil {
				return err
			}
		}
	}
	return nil
}

// deliverRange hands f records [from, to) of a partition, whatever it has
// committed: Kafka replaying what a consumer had already seen.
func (w *world) deliverRange(ctx context.Context, f *fakeCDC, part int32, from, to int64, size int) error {
	for from < to {
		batch := w.slice(part, from, int(min(int64(size), to-from)))
		if len(batch) == 0 {
			return nil
		}
		if err := f.call(ctx, batch); err != nil {
			return err
		}
		from += int64(len(batch))
	}
	return nil
}

// ---- the fake Kafka consumer -----------------------------------------------

// pump is a consumer loop over a world's log: it fetches from the committed
// offsets, hands batches to the service, and commits what was handled. The broker
// can be taken down (nothing is fetched or committed) and commits can be made to
// fail (what was handled is delivered again).
type pump struct {
	w          *world
	f          *fakeCDC
	max        int
	down       atomic.Bool
	failCommit atomic.Bool
	handled    atomic.Int64
	cancel     context.CancelFunc
	done       chan struct{}

	// A consumer reads ahead of what it has committed: its position is in memory.
	// A failed commit does not move it back; only a restart or rebalance does.
	// Both maps belong to the run goroutine.
	pos map[int32]int64
	own map[int32]int64 // the committed offset this pump last set itself
}

func (w *world) startPump(t testing.TB, f *fakeCDC, max int) *pump {
	ctx, cancel := context.WithCancel(context.Background())
	pm := &pump{w: w, f: f, max: max, cancel: cancel, done: make(chan struct{}), pos: map[int32]int64{}, own: map[int32]int64{}}
	go pm.run(ctx)
	t.Cleanup(func() { cancel(); <-pm.done })
	return pm
}

func (pm *pump) run(ctx context.Context) {
	defer close(pm.done)
	for ctx.Err() == nil {
		progressed := false
		for part := range pm.w.parts {
			if pm.down.Load() {
				break
			}
			// Someone else moved this partition's committed offset (a restore, a
			// test rewinding Kafka): that is a rebalance, and the position follows it.
			committed := pm.f.committedOffset(part)
			if committed != pm.own[part] {
				pm.pos[part], pm.own[part] = committed, committed
			}
			if pm.pos[part] < committed {
				pm.pos[part] = committed
			}
			batch := pm.w.slice(part, pm.pos[part], pm.max)
			if len(batch) == 0 {
				// Read to the end with commits still owed (they failed): the consumer is
				// restarted or rebalanced, and Kafka hands back what it never committed.
				if pm.pos[part] > committed {
					pm.pos[part] = committed
					progressed = true
					time.Sleep(15 * time.Millisecond)
				}
				continue
			}
			if err := pm.f.call(ctx, batch); err != nil {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			pm.handled.Add(int64(len(batch)))
			progressed = true
			last := batch[len(batch)-1]
			pm.pos[part] = last.Offset + 1
			if pm.down.Load() || pm.failCommit.Load() {
				pm.f.commitError(part, last.Offset, errors.New("kafka: commit failed"))
				continue
			}
			pm.f.setCommitted(part, last.Offset+1)
			pm.own[part] = last.Offset + 1
		}
		if !progressed {
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// caughtUp is whether every partition is committed to the end of the log.
func (pm *pump) caughtUp() bool {
	for part := range pm.w.parts {
		if pm.f.committedOffset(part) < pm.w.size(part) {
			return false
		}
	}
	return true
}

// ---- the rig ---------------------------------------------------------------

type svcT = cqrs.ICQRSService[Account, AccountResponse, struct{}, int64]

// rig is a write world, a read database and one running replica.
type rig struct {
	t     testing.TB
	p     *pair
	w     *world
	f     *fakeCDC
	svc   svcT
	group string
	base  cqrs.Stats // what the service had already done when this rig began

	// State of the fault a test has started and must stop.
	healRead func()
	release  func()
	killer   *killer
}

func newRig(t testing.TB, parts int, mods ...func(*cqrsSvc)) *rig {
	t.Helper()
	p := newPair(t, 8)
	r := &rig{t: t, p: p, w: newWorld(t, p, parts), group: fmt.Sprintf("g-%d-%d", runID%1_000_000, seqNo.Add(1))}
	r.f = newFakeCDC(r.group)
	r.svc = newService(t, p, r.f, mods...)
	fakes.Store(r.svc, r.f)
	start(t, r.svc)
	return r
}

// replica starts another service on the same databases and group.
func (r *rig) replica(mods ...func(*cqrsSvc)) (*fakeCDC, svcT) {
	r.t.Helper()
	f := newFakeCDC(r.group)
	svc := newService(r.t, r.p, f, mods...)
	fakes.Store(svc, f)
	start(r.t, svc)
	return f, svc
}

func withHooks(rec *recorder) func(*cqrsSvc) {
	return func(c *cqrsSvc) {
		c.ToResource = func(a *Account) *AccountResponse { return &AccountResponse{ID: a.ID, Owner: a.Owner} }
		c.Created = func(*Account) []string { return []string{"c"} }
		c.Updated = func(*Account) []string { return []string{"u"} }
		c.Deleted = func(*Account) []string { return []string{"d"} }
		c.Dispatch = rec.dispatch
		c.HookWorkers = 64
	}
}

// ---- comparing the two databases -------------------------------------------

func accountsOf(db *bun.DB) (map[int64]Account, error) {
	var rows []Account
	if err := db.NewSelect().Model(&rows).Scan(context.Background()); err != nil {
		return nil, err
	}
	out := make(map[int64]Account, len(rows))
	for _, r := range rows {
		out[r.ID] = r
	}
	return out, nil
}

func equalAccount(a, b Account) bool {
	if a.ID != b.ID || a.Owner != b.Owner || a.Balance != b.Balance || a.Active != b.Active ||
		a.Version != b.Version || !a.UpdatedAt.Equal(b.UpdatedAt) {
		return false
	}
	if (a.Note == nil) != (b.Note == nil) {
		return false
	}
	return a.Note == nil || *a.Note == *b.Note
}

func brief(a Account) string {
	return fmt.Sprintf("{owner=%q balance=%d version=%d at=%s}", a.Owner, a.Balance, a.Version, a.UpdatedAt.UTC().Format(time.RFC3339Nano))
}

// diffAccounts describes how read differs from write ("" when it does not).
// Rows in except are left out of the comparison on both sides: they are rows
// the read database rejected for good, whether it holds an older copy or none.
func diffAccounts(write, read map[int64]Account, except map[int64]bool) string {
	var out []string
	for id, a := range write {
		if len(out) >= 6 {
			break
		}
		b, ok := read[id]
		switch {
		case except[id]:
			continue
		case !ok:
			out = append(out, fmt.Sprintf("row %d is missing from the read db", id))
		case !equalAccount(a, b):
			out = append(out, fmt.Sprintf("row %d differs: write=%s read=%s", id, brief(a), brief(b)))
		}
	}
	for id := range read {
		if _, ok := write[id]; !ok && len(out) < 8 && !except[id] {
			out = append(out, fmt.Sprintf("row %d exists only in the read db", id))
		}
	}
	sort.Strings(out)
	return strings.Join(out, "; ")
}

func exceptSet(ids []int64) map[int64]bool {
	if len(ids) == 0 {
		return nil
	}
	m := make(map[int64]bool, len(ids))
	for _, id := range ids {
		m[id] = true
	}
	return m
}

// syncDiff compares the two databases; the error is a database that cannot be read.
func (p *pair) syncDiff(except ...int64) (string, error) {
	w, err := accountsOf(p.write.Client())
	if err != nil {
		return "", fmt.Errorf("reading the write db: %w", err)
	}
	r, err := accountsOf(p.read.Client())
	if err != nil {
		return "", fmt.Errorf("reading the read db: %w", err)
	}
	return diffAccounts(w, r, exceptSet(except)), nil
}

func (p *pair) assertInSync(t testing.TB, except ...int64) {
	t.Helper()
	d, err := p.syncDiff(except...)
	if err != nil {
		t.Fatal(err)
	}
	if d != "" {
		t.Fatalf("the databases differ: %s", d)
	}
}

// waitInSync waits until the read database equals the write database.
func (r *rig) waitInSync(except ...int64) {
	r.t.Helper()
	r.waitInSyncFor(90*time.Second, except...)
}

func (r *rig) waitInSyncFor(d time.Duration, except ...int64) {
	r.t.Helper()
	deadline := time.Now().Add(d)
	var last string
	for {
		diff, err := r.p.syncDiff(except...)
		switch {
		case err != nil:
			last = err.Error()
		case diff == "":
			return
		default:
			last = diff
		}
		if time.Now().After(deadline) {
			r.t.Fatalf("the databases never converged: %s (service: %+v)", last, r.svc.Stats())
		}
		time.Sleep(40 * time.Millisecond)
	}
}

// ---- infrastructure faults -------------------------------------------------

// readOutage makes the read database refuse every connection and drops the ones
// that exist: from the service's side the server is gone. The returned function
// brings it back (it also runs when the test ends).
func readOutage(t testing.TB, p *pair) (restore func()) {
	t.Helper()
	if _, err := readAdmin.Exec(fmt.Sprintf(`ALTER DATABASE %q WITH ALLOW_CONNECTIONS false`, p.readDB)); err != nil {
		t.Fatal(err)
	}
	killReadBackends(t, p, 0)
	var once sync.Once
	restore = func() {
		once.Do(func() {
			_, _ = readAdmin.Exec(fmt.Sprintf(`ALTER DATABASE %q WITH ALLOW_CONNECTIONS true`, p.readDB))
		})
	}
	t.Cleanup(restore)
	return restore
}

// writeOutage is readOutage for the write database.
func writeOutage(t testing.TB, p *pair) (restore func()) {
	t.Helper()
	if _, err := writeAdmin.Exec(fmt.Sprintf(`ALTER DATABASE %q WITH ALLOW_CONNECTIONS false`, p.writeDB)); err != nil {
		t.Fatal(err)
	}
	killWriteBackends(t, p)
	var once sync.Once
	restore = func() {
		once.Do(func() {
			_, _ = writeAdmin.Exec(fmt.Sprintf(`ALTER DATABASE %q WITH ALLOW_CONNECTIONS true`, p.writeDB))
		})
	}
	t.Cleanup(restore)
	return restore
}

func killWriteBackends(t testing.TB, p *pair) {
	t.Helper()
	if _, err := writeAdmin.Exec(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		WHERE datname = $1 AND pid <> pg_backend_pid()`, p.writeDB); err != nil {
		t.Fatal(err)
	}
}

func setWriteReadOnly(t testing.TB, p *pair, on bool) {
	t.Helper()
	q := fmt.Sprintf(`ALTER DATABASE %q RESET default_transaction_read_only`, p.writeDB)
	if on {
		q = fmt.Sprintf(`ALTER DATABASE %q SET default_transaction_read_only = on`, p.writeDB)
	}
	if _, err := writeAdmin.Exec(q); err != nil {
		t.Fatal(err)
	}
}

// settleWrite returns once the write database answers again after faults.
func settleWrite(t testing.TB, p *pair) {
	t.Helper()
	setWriteReadOnly(t, p, false)
	deadline := time.Now().Add(15 * time.Second)
	for {
		var one int
		err := p.write.Client().NewRaw("SELECT 1").Scan(context.Background(), &one)
		if err == nil {
			ok := true
			for range 8 {
				ok = ok && p.write.Client().NewRaw("SELECT 1").Scan(context.Background(), &one) == nil
			}
			if ok {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("the write database never recovered: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// eventuallyN polls cond for up to d.
func eventuallyN(t testing.TB, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("never saw %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// failing returns a context for the test's own long operations.
func longCtx(t testing.TB) context.Context { return ctxTimeout(t, 4*time.Minute) }
