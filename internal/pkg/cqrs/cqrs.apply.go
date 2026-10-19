package cqrs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	sqlsvc "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sql"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// outcome is what a batch did, kept across the attempts at it: a hook must
// fire once for a change that committed in an attempt that later failed, so
// the retry (which sees that change as a duplicate) must not forget it.
type outcome[TData any] struct {
	kept       []*change[TData]
	written    int
	stale      int
	duplicates int
	parked     map[int64]struct{} // offsets already on the dead-letter topic
}

func (o *outcome[TData]) add(r applied[TData]) {
	o.kept = append(o.kept, r.kept...)
	o.written += r.written
	o.stale += r.stale
	o.duplicates += r.duplicates
}

// applied is the result of one read-database transaction.
type applied[TData any] struct {
	kept       []*change[TData]
	written    int
	stale      int
	duplicates int
}

// handle is the cdc batch handler: it applies one run of consecutive records of
// one partition to the read database. It returns nil only when every record is
// durably handled (applied, a duplicate, skipped on purpose, or on the
// dead-letter topic), which is when cdc commits their offsets to Kafka. Until
// then it retries, with a capped backoff, so a database that is down or a
// dead-letter topic that is unreachable stalls the partition instead of
// losing a change.
func (c *CQRSService[TData, TResponse, TRequest, TID]) handle(ctx context.Context, batch []cdc.Record) (err error) {
	if len(batch) == 0 {
		return nil
	}
	done, ok := c.st.gate.Enter()
	if !ok {
		return ErrStopped
	}
	defer done()
	ctx, op := c.obs.Begin(ctx, observability.ComponentCQRS, "handle", slog.String(observability.KeyTopic, c.topic))
	defer func() { op.End(err) }()
	if err := checkBatch(batch); err != nil {
		c.log.Error("rejecting malformed batch", "error", err)
		return err
	}
	first, last := batch[0], batch[len(batch)-1]
	log := c.log.With("partition", first.Partition, "first_offset", first.Offset, "last_offset", last.Offset, "records", len(batch))
	started := time.Now()
	c.st.stats.batches.Add(1)
	c.st.stats.records.Add(int64(len(batch)))

	items := c.model.prepare(batch)
	out := &outcome[TData]{parked: map[int64]struct{}{}}
	backoff := c.RetryBackoff
	for attempt := 1; ; attempt++ {
		err := c.attempt(ctx, items, out)
		if err == nil {
			break
		}
		c.st.stats.fail(err)
		log.Warn("batch not applied, will retry", "attempt", attempt, "backoff", backoff.String(), "error", safeErr(err))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, c.MaxRetryBackoff)
	}

	for i := range items {
		switch {
		case items[i].note == noteTombstone:
			c.st.stats.tombstones.Add(1)
		case items[i].note != "":
			c.st.stats.skipped.Add(1)
			log.Warn("record skipped", "reason", items[i].note, "offset", items[i].rec.Offset)
		}
	}
	c.st.stats.applied.Add(int64(out.written))
	c.st.stats.stale.Add(int64(out.stale))
	c.st.stats.duplicates.Add(int64(out.duplicates))
	c.batchTime.Record(ctx, time.Since(started).Seconds())
	if out.duplicates == len(batch) {
		op.SetOutcome(observability.OutcomeSkipped) // the checkpoint already covered all of it
	}
	log.Debug("batch applied", "written", out.written, "stale_rejected", out.stale,
		"duplicates", out.duplicates, "duration_ms", time.Since(started).Milliseconds())
	c.fireHooks(ctx, out.kept)
	return nil
}

// checkBatch confirms what cdc promises: one topic and partition, offsets
// strictly increasing. Anything else would corrupt the checkpoint.
func checkBatch(batch []cdc.Record) error {
	for i := 1; i < len(batch); i++ {
		a, b := batch[i-1], batch[i]
		if a.Topic != b.Topic || a.Partition != b.Partition || b.Offset <= a.Offset {
			return fmt.Errorf("%w: %s[%d]@%d is followed by %s[%d]@%d",
				ErrBadBatch, a.Topic, a.Partition, a.Offset, b.Topic, b.Partition, b.Offset)
		}
	}
	return nil
}

// attempt makes one pass over a batch: poison records are parked first, then
// the rest is applied (splitting around any record the database rejects).
func (c *CQRSService[TData, TResponse, TRequest, TID]) attempt(ctx context.Context, items []item[TData], out *outcome[TData]) error {
	if err := c.parkPoison(ctx, items, out.parked); err != nil {
		return err
	}
	return c.applyRange(ctx, items, out)
}

// parkPoison puts the records that cannot even be decoded on the dead-letter
// topic, before the transaction that moves the checkpoint past them. A record
// the checkpoint already covers was parked by an earlier delivery.
func (c *CQRSService[TData, TResponse, TRequest, TID]) parkPoison(ctx context.Context, items []item[TData], parked map[int64]struct{}) error {
	var poison []int
	for i := range items {
		if items[i].dead != nil {
			poison = append(poison, i)
		}
	}
	if len(poison) == 0 {
		return nil
	}
	covered, err := c.coveredThrough(ctx, items[0].rec.Topic, items[0].rec.Partition)
	if err != nil {
		return fmt.Errorf("reading the checkpoint: %w", err)
	}
	for _, i := range poison {
		if items[i].rec.Offset < covered {
			continue
		}
		if err := c.park(ctx, items[i].rec, items[i].dead, parked); err != nil {
			return err
		}
	}
	return nil
}

// park publishes a record that can never be applied to the dead-letter topic.
func (c *CQRSService[TData, TResponse, TRequest, TID]) park(ctx context.Context, rec cdc.Record, cause error, parked map[int64]struct{}) error {
	if _, done := parked[rec.Offset]; done {
		return nil
	}
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), deadLetterTimeout)
	defer cancel()
	if err := c.CDCService.DeadLetter(dctx, rec, errors.New(safeErr(cause))); err != nil {
		return fmt.Errorf("dead-lettering offset %d: %w", rec.Offset, err)
	}
	parked[rec.Offset] = struct{}{}
	c.st.stats.deadLettered.Add(1)
	c.noteParked(rec) // the row it describes is repaired automatically once the read db accepts it
	c.log.Warn("record dead-lettered", "partition", rec.Partition, "offset", rec.Offset, "cause", safeErr(cause))
	return nil
}

// applyRange applies items in one transaction. When the database rejects the
// data itself (never transient), it halves the range until the single record at
// fault is found, parks that one, and applies everything around it.
func (c *CQRSService[TData, TResponse, TRequest, TID]) applyRange(ctx context.Context, items []item[TData], out *outcome[TData]) error {
	res, err := c.applyTx(ctx, items)
	if err == nil {
		out.add(res)
		return nil
	}
	if !c.permanent(err) {
		return err
	}
	if len(items) > 1 {
		mid := len(items) / 2
		c.log.Warn("the read db rejected a batch, splitting it", "records", len(items), "error", safeErr(err))
		if err := c.applyRange(ctx, items[:mid], out); err != nil {
			return err
		}
		return c.applyRange(ctx, items[mid:], out)
	}
	it := items[0]
	if err := c.park(ctx, it.rec, err, out.parked); err != nil {
		return err
	}
	it.change, it.dead = nil, err // nothing to write: only the checkpoint moves past it
	res, err = c.applyTx(ctx, []item[TData]{it})
	if err != nil {
		return err
	}
	out.add(res)
	return nil
}

// applyTx writes items to the read database and moves the checkpoint past them
// in one transaction, so the rows and the position can never disagree: a crash,
// a failover or a lost connection leaves either both or neither.
//
// It locks the partition's checkpoint row first. A second replica applying the
// same partition (a rebalance, a delayed redelivery) waits for this commit and
// then finds the checkpoint ahead, so every record is applied at most once
// however many replicas run.
func (c *CQRSService[TData, TResponse, TRequest, TID]) applyTx(ctx context.Context, items []item[TData]) (applied[TData], error) {
	var res applied[TData]
	db, err := c.readDB()
	if err != nil {
		return res, err
	}
	first, last := items[0].rec, items[len(items)-1].rec
	// Once begun, a batch is allowed to finish even if the service is asked to stop:
	// it commits whole or rolls back whole, bounded by ApplyTimeout.
	txCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.ApplyTimeout)
	defer cancel()
	err = db.RunInTx(txCtx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
		res = applied[TData]{}
		// The read model mirrors every tenant: row-level security lets the replicator through.
		if err := utils.SetReplicator(ctx, tx); err != nil {
			return fmt.Errorf("setting the replicator: %w", err)
		}
		cp, err := c.lockCheckpoint(ctx, tx, first.Topic, first.Partition)
		if err != nil {
			return fmt.Errorf("locking the checkpoint: %w", err)
		}
		skip := skipCovered(items, cp.NextOffset)
		if skip > 0 {
			if err := checkRegression(items[:skip], cp); err != nil {
				return err
			}
		}
		res.duplicates = skip
		if skip == len(items) {
			return nil // all of it was applied before
		}
		groups, kept, release := c.plan(items[skip:])
		defer release()
		stale, err := c.write(ctx, tx, db.QueryGen(), groups)
		if err != nil {
			return err
		}
		res.kept, res.written, res.stale = kept, len(groups)-stale, stale // one write per generation of a key
		return advance(ctx, tx, cp, last)
	})
	if err != nil {
		return applied[TData]{}, err
	}
	return res, nil
}

// skipCovered is how many leading items the checkpoint already covers.
func skipCovered[TData any](items []item[TData], next int64) int {
	n, _ := slices.BinarySearchFunc(items, next, func(it item[TData], off int64) int {
		switch {
		case it.rec.Offset < off:
			return -1
		case it.rec.Offset > off:
			return 1
		}
		return 0
	})
	return n
}

// checkRegression refuses to skip records that are newer than anything already
// applied. That only happens when the topic was recreated, so its offsets start
// from zero again: skipping them as "already applied" would silently drop
// every change. Failing keeps replication paused (and loud) instead.
func checkRegression[TData any](covered []item[TData], cp *Checkpoint) error {
	if cp.LastRecordAt == nil {
		return nil
	}
	limit := cp.LastRecordAt.Add(regressionSlack)
	for i := range covered {
		if t := covered[i].rec.Timestamp; !t.IsZero() && t.After(limit) {
			return fmt.Errorf("%w: offset %d is dated %s, after the last applied record (%s)",
				ErrOffsetRegression, covered[i].rec.Offset, t.UTC().Format(time.RFC3339), cp.LastRecordAt.UTC().Format(time.RFC3339))
		}
	}
	return nil
}

// lockCheckpoint creates the partition's checkpoint if there is none and locks
// it until the transaction ends, returning where it stands. It is one
// statement: the upsert takes the row lock and returns the stored offset.
func (c *CQRSService[TData, TResponse, TRequest, TID]) lockCheckpoint(ctx context.Context, tx bun.Tx, topic string, partition int32) (*Checkpoint, error) {
	cp := &Checkpoint{Group: c.CDCService.GroupID(), Topic: topic, Partition: partition, UpdatedAt: time.Now().UTC()}
	_, err := tx.NewInsert().Model(cp).
		On(`CONFLICT ("consumer_group", "topic", "partition") DO UPDATE`).
		Set("updated_at = EXCLUDED.updated_at").
		Returning("next_offset, last_record_at").
		Exec(ctx)
	if err != nil {
		return nil, err
	}
	return cp, nil
}

// advance moves the checkpoint past last.
func advance(ctx context.Context, tx bun.Tx, cp *Checkpoint, last cdc.Record) error {
	cp.NextOffset = last.Offset + 1
	if !last.Timestamp.IsZero() {
		at := last.Timestamp.UTC()
		cp.LastRecordAt = &at
	}
	cp.UpdatedAt = time.Now().UTC()
	if _, err := tx.NewUpdate().Model(cp).
		Column("next_offset", "last_record_at", "updated_at").
		WherePK().
		Exec(ctx); err != nil {
		return fmt.Errorf("advancing the checkpoint: %w", err)
	}
	return nil
}

// coveredThrough is the offset the partition's checkpoint stands at: every
// record before it has been handled. A partition never seen yet stands at 0.
func (c *CQRSService[TData, TResponse, TRequest, TID]) coveredThrough(ctx context.Context, topic string, partition int32) (int64, error) {
	db, err := c.readDB()
	if err != nil {
		return 0, err
	}
	cp := &Checkpoint{Group: c.CDCService.GroupID(), Topic: topic, Partition: partition}
	qctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.ApplyTimeout)
	defer cancel()
	err = db.NewSelect().Model(cp).Column("next_offset").WherePK().Scan(qctx)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return cp.NextOffset, nil
}

// permanent reports whether retrying the same data can never succeed: the
// database rejected the data itself, or the data cannot even be written as SQL
// (a NUL byte, which Postgres cannot store). Everything else is retried.
//
// One database rejection is not about the data at all: NOT NULL on a column the
// model does not map. The read table has a column this service has never heard
// of and cannot fill, which is a schema that is ahead of the code. Every row
// would fail the same way, so dead-lettering them would empty the stream; it
// waits for the migration or the deploy instead.
func (c *CQRSService[TData, TResponse, TRequest, TID]) permanent(err error) bool {
	if errors.Is(err, ErrMalformedChange) {
		return true
	}
	if !sqlsvc.IsPermanent(err) {
		return false
	}
	if sqlsvc.SQLState(err) == "23502" {
		if col := sqlsvc.ErrorColumn(err); col != "" {
			if _, mapped := c.model.fields[col]; !mapped {
				return false
			}
		}
	}
	return true
}

// safeErr is err as text that is safe to log and to put on a dead-letter
// record: for a PostgreSQL error only its SQLSTATE and location (never the
// message, which can quote the row), otherwise the error's own text.
func safeErr(err error) string {
	if err == nil {
		return ""
	}
	if sqlsvc.SQLState(err) != "" {
		return sqlsvc.Describe(err)
	}
	return err.Error()
}
