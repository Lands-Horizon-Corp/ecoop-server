package cqrs

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Automatic repair.
//
// Replication parks a change the read database rejects for good (a constraint
// that is stricter on the read side, a column that is too small) and moves on, so
// one bad row never stops the stream. The row it describes then holds its older
// state. Nobody should have to remember to fix that by hand, so the service does:
// it remembers the key of every row it parks, and a background loop, started by
// Run and ended by Stop, keeps looking at those rows. Once the read database
// accepts the row's current state (the cause was fixed: the constraint relaxed,
// the column widened) the row is written, and the service forgets it.
//
// The loop works from the write database's current row, never from the parked
// event. An old event cannot be trusted any more: a row that was deleted and
// created again has changed under it, and replaying history out of order is what
// the ordering guards exist to prevent. The current row is where the stream is
// heading anyway, so writing it is always safe, and whatever the stream still has
// to deliver lands on top of it, in order.
//
// A service that starts finds the rows parked before it started (or by other
// replicas) on the dead-letter topic, so nothing depends on this process having
// been alive when the change was parked.

const (
	// defaultRepairInterval is how often the repair loop looks at parked rows.
	defaultRepairInterval = 30 * time.Second
	// maxRepairBackoff caps the wait before a row that is still rejected is tried
	// again; it doubles from the repair interval up to this.
	maxRepairBackoff = time.Hour
	// repairChunk is how many rows are repaired in one read-database transaction,
	// and repairPerTick how many one pass of the loop looks at.
	repairChunk   = upsertChunk
	repairPerTick = 4 * repairChunk

	// defaultRowStateRetention is how long OrderByLSN keeps the tombstone of a
	// deleted row. It must outlast anything that can still be replayed: Kafka's
	// default retention is 7 days.
	defaultRowStateRetention = 14 * 24 * time.Hour
	maxPruneInterval         = time.Hour
)

// repairState is the rows waiting to be repaired and the loop that repairs them.
type repairState struct {
	mu        sync.Mutex
	pending   map[string]*repairEntry
	seeded    bool // the dead-letter topic has been read
	lastPrune time.Time

	cancel context.CancelFunc // ends the loop
	done   chan struct{}      // closed when the loop has ended
}

// repairEntry is one parked row.
type repairEntry struct {
	id       any
	due      time.Time // when to look at it next
	attempts int       // how many times the read database has refused it since
}

func newRepairState() *repairState {
	return &repairState{pending: map[string]*repairEntry{}}
}

func (rs *repairState) add(key string, id any, due time.Time) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if _, ok := rs.pending[key]; ok {
		return // already waiting; it is looked at on its own schedule
	}
	rs.pending[key] = &repairEntry{id: id, due: due}
}

func (rs *repairState) count() int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return len(rs.pending)
}

// due is up to limit rows that are ready to be looked at.
type dueRow struct {
	key string
	id  any
}

func (rs *repairState) due(now time.Time, limit int) []dueRow {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	var out []dueRow
	for key, e := range rs.pending {
		if len(out) >= limit {
			break
		}
		if !e.due.After(now) {
			out = append(out, dueRow{key: key, id: e.id})
		}
	}
	return out
}

// resolve forgets the rows that were repaired and schedules the ones the read
// database still refuses.
func (rs *repairState) resolve(rows []dueRow, failed map[string]struct{}, now time.Time, interval time.Duration) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	for _, r := range rows {
		e, ok := rs.pending[r.key]
		if !ok {
			continue
		}
		if _, refused := failed[r.key]; !refused {
			delete(rs.pending, r.key)
			continue
		}
		e.attempts++
		e.due = now.Add(repairBackoff(interval, e.attempts))
	}
}

// repairBackoff is the wait after attempts refusals: the interval, doubled each
// time, up to maxRepairBackoff.
func repairBackoff(interval time.Duration, attempts int) time.Duration {
	d := interval
	for i := 0; i < attempts && d < maxRepairBackoff; i++ {
		d *= 2
	}
	return min(d, maxRepairBackoff)
}

// repairOn reports whether the service repairs parked rows by itself.
func (c *CQRSService[TData, TResponse, TRequest, TID]) repairOn() bool {
	return c.RepairInterval > 0 && c.st != nil && c.st.repair != nil
}

// noteParked remembers the row a parked record is about. A record whose key
// cannot be read (poison that is not even JSON) has no row to repair.
func (c *CQRSService[TData, TResponse, TRequest, TID]) noteParked(rec cdc.Record) {
	if !c.repairOn() {
		return
	}
	row, err := c.model.decodeRow(rec.Key)
	if err != nil {
		return
	}
	key, id, err := c.model.identify(&row)
	if err != nil {
		return
	}
	c.st.repair.add(key, id, time.Now().Add(c.RepairInterval))
}

// startRepair starts the loop. The caller holds the lifecycle lock.
func (c *CQRSService[TData, TResponse, TRequest, TID]) startRepair(ctx context.Context) {
	if !c.repairOn() {
		return
	}
	rs := c.st.repair
	if rs.done != nil {
		return
	}
	loopCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	rs.cancel, rs.done = cancel, make(chan struct{})
	go c.repairLoop(loopCtx, rs.done)
}

// stopRepair ends the loop and waits for the pass it is in, or for ctx. The
// caller holds the lifecycle lock.
func (c *CQRSService[TData, TResponse, TRequest, TID]) stopRepair(ctx context.Context) error {
	if c.st == nil || c.st.repair == nil || c.st.repair.done == nil {
		return nil
	}
	rs := c.st.repair
	rs.cancel()
	done := rs.done
	rs.cancel, rs.done = nil, nil
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) repairLoop(ctx context.Context, done chan struct{}) {
	defer close(done)
	t := time.NewTicker(c.RepairInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.repairTick(ctx)
		}
	}
}

// repairTick is one pass: read the dead-letter topic once, repair the rows that
// are due, and prune old tombstones. It never panics out of the loop.
func (c *CQRSService[TData, TResponse, TRequest, TID]) repairTick(ctx context.Context) {
	defer func() {
		if p := recover(); p != nil {
			c.log.Error("repair pass panicked", "panic", fmt.Sprint(p))
		}
	}()
	exit, ok := c.st.gate.Enter()
	if !ok {
		return
	}
	defer exit()
	rs := c.st.repair
	c.seedRepair(ctx)
	for range repairPerTick / repairChunk {
		if ctx.Err() != nil {
			return
		}
		rows := rs.due(time.Now(), repairChunk)
		if len(rows) == 0 {
			break
		}
		ids := make([]any, len(rows))
		for i, r := range rows {
			ids[i] = r.id
		}
		failed := map[string]struct{}{}
		if err := c.repairChunk(ctx, ids, failed); err != nil {
			// The databases are not both reachable right now: the rows stay due and
			// the next pass tries again.
			c.log.Warn("repair postponed", "error", safeErr(err))
			return
		}
		rs.resolve(rows, failed, time.Now(), c.RepairInterval)
	}
	c.maybePrune(ctx)
}

// seedRepair reads the dead-letter topic, once, for the rows parked before this
// service started or by another replica. A topic too big to read is left alone
// (rows parked from now on are still tracked); any other failure is retried.
func (c *CQRSService[TData, TResponse, TRequest, TID]) seedRepair(ctx context.Context) {
	rs := c.st.repair
	rs.mu.Lock()
	seeded := rs.seeded
	rs.mu.Unlock()
	if seeded || utils.IsNil(c.CDCService) {
		return
	}
	recs, err := c.CDCService.DeadLetters(ctx, c.topic, 0)
	switch {
	case errors.Is(err, cdc.ErrTooManyDeadLetters):
		c.log.Warn("the dead-letter topic is too big to read; rows parked before now are not repaired automatically", "error", err.Error())
	case err != nil:
		c.log.Debug("cannot read the dead-letter topic yet", "error", err.Error())
		return
	}
	for _, rec := range recs {
		c.noteParked(rec)
	}
	rs.mu.Lock()
	rs.seeded = true
	rs.mu.Unlock()
}

// repairChunk repairs ids. A chunk the read database rejects for good is split
// until the row at fault is alone; that row is added to failed and the rest is
// repaired. Anything else (a database that cannot be reached) is returned.
func (c *CQRSService[TData, TResponse, TRequest, TID]) repairChunk(ctx context.Context, ids []any, failed map[string]struct{}) error {
	got, err := c.repairOnce(ctx, ids)
	if err == nil {
		if n := got.inserted + got.updated + got.deleted; n > 0 {
			c.st.stats.repaired.Add(int64(n))
			c.log.Warn("rows repaired from the write db", "rows", n, "inserted", got.inserted,
				"updated", got.updated, "deleted", got.deleted)
		}
		return nil
	}
	if !c.permanent(err) {
		return err
	}
	if len(ids) > 1 {
		mid := len(ids) / 2
		if err := c.repairChunk(ctx, ids[:mid], failed); err != nil {
			return err
		}
		return c.repairChunk(ctx, ids[mid:], failed)
	}
	key := keyString(reflect.ValueOf(ids[0]))
	failed[key] = struct{}{}
	c.log.Warn("row still rejected by the read db", "key", key, "cause", safeErr(err))
	return nil
}

// repairCounts is what one repair transaction changed.
type repairCounts struct{ inserted, updated, deleted int }

// repairOnce makes exactly these rows equal to the write database, in one
// read-database transaction. It locks every checkpoint row of this service's
// group and topic first, which is what replication takes first when it applies a
// batch: for the length of the transaction no replica of this service writes the
// table, so what is read from the write database here cannot be older than what
// is already in the read database. The comparison is on the exact values both
// sides would write, so a row is touched only when it really differs.
//
// A partition that has never applied a batch has no checkpoint row to lock yet;
// before its first batch there is nothing to race.
func (c *CQRSService[TData, TResponse, TRequest, TID]) repairOnce(ctx context.Context, ids []any) (repairCounts, error) {
	var res repairCounts
	if len(ids) == 0 {
		return res, nil
	}
	rdb, err := c.readDB()
	if err != nil {
		return res, err
	}
	wdb, err := c.writeDB()
	if err != nil {
		return res, err
	}
	txCtx, cancel := context.WithTimeout(ctx, c.ApplyTimeout)
	defer cancel()
	err = rdb.RunInTx(txCtx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
		res = repairCounts{}
		if err := utils.SetReplicator(ctx, tx); err != nil {
			return fmt.Errorf("setting the replicator: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `SELECT 1 FROM `+CheckpointTable+
			` WHERE consumer_group = ? AND topic = ? ORDER BY partition FOR UPDATE`,
			c.CDCService.GroupID(), c.topic); err != nil {
			return fmt.Errorf("locking the checkpoints: %w", err)
		}
		var wrows, rrows []TData
		if err := wdb.NewSelect().Model(&wrows).
			Where("? IN (?)", bun.Ident(c.model.idCol), bun.In(ids)).Scan(ctx); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("reading the write db: %w", err)
		}
		if err := tx.NewSelect().Model(&rrows).
			Where("? IN (?)", bun.Ident(c.model.idCol), bun.In(ids)).Scan(ctx); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("reading the read db: %w", err)
		}
		gen := rdb.QueryGen()
		want := make(map[string]*change[TData], len(wrows))
		for i := range wrows {
			key, id, err := c.model.identify(&wrows[i])
			if err != nil {
				continue
			}
			want[key] = &change[TData]{op: cdc.OpUpdate, data: wrows[i], key: key, id: id}
		}
		have := make(map[string]*change[TData], len(rrows))
		for i := range rrows {
			key, id, err := c.model.identify(&rrows[i])
			if err != nil {
				continue
			}
			have[key] = &change[TData]{op: cdc.OpDelete, data: rrows[i], key: key, id: id}
		}
		var upserts, deletes []*change[TData]
		seen := make(map[string]struct{}, len(ids))
		for _, id := range ids {
			key := keyString(reflect.ValueOf(id))
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
			w, h := want[key], have[key]
			switch {
			case w != nil && h == nil:
				upserts = append(upserts, w)
				res.inserted++
			case w != nil && h != nil:
				if !bytes.Equal(c.appendValues(gen, nil, w), c.appendValues(gen, nil, h)) {
					upserts = append(upserts, w)
					res.updated++
				}
			case w == nil && h != nil:
				deletes = append(deletes, h)
				res.deleted++
			}
		}
		// The write database's row is the newest there is: no version guard in its way.
		if _, err := c.writeUpserts(ctx, tx, gen, upserts, false); err != nil {
			return fmt.Errorf("upserting into the read db: %w", err)
		}
		if err := c.deletePlain(ctx, tx, gen, deletes); err != nil {
			return fmt.Errorf("deleting from the read db: %w", err)
		}
		return nil
	})
	if err != nil {
		return repairCounts{}, err
	}
	return res, nil
}

// maybePrune drops the delete tombstones of OrderByLSN once they are older than
// RowStateRetention. A tombstone is what turns away an insert replayed after its
// row was deleted, so it is kept for longer than anything can still be replayed.
// Without pruning the table grows by one row per row ever deleted.
func (c *CQRSService[TData, TResponse, TRequest, TID]) maybePrune(ctx context.Context) {
	if !c.OrderByLSN || c.RowStateRetention <= 0 {
		return
	}
	rs := c.st.repair
	every := min(max(c.RowStateRetention/10, c.RepairInterval), maxPruneInterval)
	if time.Since(rs.lastPrune) < every {
		return
	}
	rs.lastPrune = time.Now()
	n, err := c.pruneRowState(ctx, c.RowStateRetention)
	if err != nil {
		c.log.Warn("pruning old tombstones failed", "error", safeErr(err))
		return
	}
	if n > 0 {
		c.log.Info("old tombstones pruned", "rows", n)
	}
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) pruneRowState(ctx context.Context, olderThan time.Duration) (int64, error) {
	db, err := c.readDB()
	if err != nil {
		return 0, err
	}
	res, err := db.ExecContext(ctx, `DELETE FROM `+RowStateTable+` WHERE table_name = ? AND deleted AND updated_at < ?`,
		c.model.tableKey, time.Now().Add(-olderThan))
	if err != nil {
		return 0, fmt.Errorf("pruning %s: %w", RowStateTable, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, nil
	}
	return n, nil
}
