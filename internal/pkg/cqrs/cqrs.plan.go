package cqrs

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/schema"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
)

const (
	// maxStatementBytes ends a multi-row statement early: a handful of huge
	// rows is split before the statement grows past what the server and this
	// process should hold at once.
	maxStatementBytes = 8 << 20
	tableAlias        = `"t"`
	excluded          = "EXCLUDED."
)

// bunErrorMarker is what bun writes in place of a value it cannot render.
var bunErrorMarker = []byte("?!(bun:")

// group is the surviving change of one primary key in one generation of it. A
// generation ends with a delete: whatever the key does after being deleted is a
// new generation, applied in a later round, because the row it meets is no longer
// the row the earlier generation met. Almost every key has one generation.
type group[TData any] struct {
	key   string
	round int
	ch    *change[TData]
}

// plan collapses fresh to one final change per primary key and generation,
// sorted by round and then key. It returns the groups (backed by a pooled slice
// the caller hands back through release) and every change that is not
// superseded, in stream order, which is what change hooks report.
//
// The result is exactly what applying the changes one at a time would leave:
//
//   - Within a generation the stream order is the truth, except that with a
//     version column a change that carries an older version than the one before
//     it is dropped: it must not undo newer data. Equal versions are not older,
//     so the later change of the stream wins.
//   - A delete ends the generation. A later change starts the next one and is
//     not compared with the deleted row: it meets an empty slot, or a newer row
//     the database guard will protect, exactly as it would if applied alone.
//     (Without this, a row deleted at version 3 and recreated at version 1 in
//     the same batch would lose its recreate to the version it replaced.)
func (c *CQRSService[TData, TResponse, TRequest, TID]) plan(fresh []item[TData]) (groups []group[TData], kept []*change[TData], release func()) {
	keys := c.keyPool.Get()
	gp := c.groupPool.Get()
	groups = *gp
	kept = make([]*change[TData], 0, len(fresh))
	for i := range fresh {
		ch := fresh[i].change
		if ch == nil {
			continue
		}
		idx, seen := keys[ch.key]
		if !seen {
			keys[ch.key] = len(groups)
			groups = append(groups, group[TData]{key: ch.key, ch: ch})
			kept = append(kept, ch)
			continue
		}
		g := &groups[idx]
		if c.bothLSN(g.ch, ch) {
			// Ordered by WAL position: a delete does not end anything, the change
			// furthest along the WAL is the truth and the row-state table guards the
			// database against anything older than what it holds.
			if g.ch.lsn > ch.lsn {
				continue // an older position arrived after a newer one
			}
			g.ch = ch
			kept = append(kept, ch)
			continue
		}
		if g.ch.op == cdc.OpDelete {
			// The generation ended: this is the next one, applied in the next round.
			keys[ch.key] = len(groups)
			groups = append(groups, group[TData]{key: ch.key, round: g.round + 1, ch: ch})
			kept = append(kept, ch)
			continue
		}
		// A delete that does not say which version it removes (Debezium sends only
		// the key without REPLICA IDENTITY FULL) is ordered by the stream alone:
		// judging it "version 0" would let an earlier change outlive its own delete.
		ordered := ch.op == cdc.OpDelete && !c.model.versionKnown(&ch.data)
		if !ordered && c.model.versionNewer(&g.ch.data, &ch.data) {
			continue // an older version arrived after a newer one
		}
		g.ch = ch
		kept = append(kept, ch)
	}
	// Several replicas may apply overlapping batches at once. Taking row locks
	// in one fixed order (by key) means they can wait on each other but never
	// deadlock.
	slices.SortFunc(groups, func(a, b group[TData]) int {
		if a.round != b.round {
			return a.round - b.round
		}
		return strings.Compare(a.key, b.key)
	})
	*gp = groups
	return groups, kept, func() {
		*gp = groups // clear what was used, so the next batch starts empty
		c.groupPool.Put(gp)
		c.keyPool.Put(keys)
	}
}

// write applies the surviving changes round by round (a round holds at most one
// change per key, so its order across keys does not matter): upserts, then
// deletes, each in key order. It returns how many upserts the version guard
// turned away.
func (c *CQRSService[TData, TResponse, TRequest, TID]) write(
	ctx context.Context, tx bun.Tx, gen schema.QueryGen, groups []group[TData],
) (stale int, err error) {
	for start := 0; start < len(groups); {
		end := start
		var upserts, deletes, ordered []*change[TData]
		for end < len(groups) && groups[end].round == groups[start].round {
			ch := groups[end].ch
			switch {
			case c.usesLSN(ch):
				ordered = append(ordered, ch)
			case ch.op == cdc.OpDelete:
				deletes = append(deletes, ch)
			default:
				upserts = append(upserts, ch)
			}
			end++
		}
		n, err := c.writeUpserts(ctx, tx, gen, upserts, true)
		stale += n
		if err != nil {
			return stale, fmt.Errorf("upserting into the read db: %w", err)
		}
		if err = c.writeDeletes(ctx, tx, gen, deletes); err != nil {
			return stale, fmt.Errorf("deleting from the read db: %w", err)
		}
		n, err = c.writeOrdered(ctx, tx, gen, ordered)
		stale += n
		if err != nil {
			return stale, fmt.Errorf("writing into the read db by WAL position: %w", err)
		}
		start = end
	}
	return stale, nil
}

// usesLSN reports whether a change is ordered by its WAL position: the service
// asks for it and the event says where it sits.
func (c *CQRSService[TData, TResponse, TRequest, TID]) usesLSN(ch *change[TData]) bool {
	return c.OrderByLSN && ch.lsn > 0
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) bothLSN(a, b *change[TData]) bool {
	return c.usesLSN(a) && c.usesLSN(b)
}

// writeOrdered writes changes ordered by WAL position. For every row it first
// claims the row's entry in the row-state table, which succeeds only when the
// change is not older than the last one applied to that row (a delete counts: its
// entry stays as a tombstone). Only the rows whose claim succeeded are written,
// and written as they are, with no version in the way: the position already
// decided. It returns how many changes were turned away as older.
func (c *CQRSService[TData, TResponse, TRequest, TID]) writeOrdered(
	ctx context.Context, tx bun.Tx, gen schema.QueryGen, rows []*change[TData],
) (stale int, err error) {
	for start := 0; start < len(rows); start += upsertChunk {
		chunk := rows[start:min(start+upsertChunk, len(rows))]
		accepted, err := c.claimRows(ctx, tx, gen, chunk)
		if err != nil {
			return stale, fmt.Errorf("claiming rows: %w", err)
		}
		stale += len(chunk) - len(accepted)
		var upserts, deletes []*change[TData]
		for _, ch := range chunk {
			if _, ok := accepted[ch.key]; !ok {
				continue
			}
			if ch.op == cdc.OpDelete {
				deletes = append(deletes, ch)
			} else {
				upserts = append(upserts, ch)
			}
		}
		if _, err := c.writeUpserts(ctx, tx, gen, upserts, false); err != nil {
			return stale, err
		}
		if err := c.deletePlain(ctx, tx, gen, deletes); err != nil {
			return stale, err
		}
	}
	return stale, nil
}

// claimRows records each row's WAL position in the row-state table and returns
// the keys that were allowed to: those whose stored position is not newer than
// the change's. Equal positions pass, like equal versions: a replay of the very
// same change writes the same data again, and the later change of the stream wins.
func (c *CQRSService[TData, TResponse, TRequest, TID]) claimRows(
	ctx context.Context, tx bun.Tx, gen schema.QueryGen, rows []*change[TData],
) (map[string]struct{}, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	b := make([]byte, 0, 256+len(rows)*64)
	b = append(b, "INSERT INTO "...)
	b = append(b, RowStateTable...)
	b = append(b, " AS s (table_name, row_key, lsn, deleted, updated_at) VALUES "...)
	for i, ch := range rows {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = append(b, '(')
		b = gen.Append(b, c.model.tableKey)
		b = append(b, ", "...)
		b = gen.Append(b, ch.key)
		b = append(b, ", "...)
		b = strconv.AppendInt(b, ch.lsn, 10)
		b = append(b, ", "...)
		b = strconv.AppendBool(b, ch.op == cdc.OpDelete)
		b = append(b, ", now())"...)
	}
	b = append(b, " ON CONFLICT (table_name, row_key) DO UPDATE SET lsn = EXCLUDED.lsn, deleted = EXCLUDED.deleted, "+
		"updated_at = EXCLUDED.updated_at WHERE s.lsn <= EXCLUDED.lsn RETURNING row_key"...)
	if bytes.IndexByte(b, 0) >= 0 || bytes.Contains(b, bunErrorMarker) {
		return nil, fmt.Errorf("%w: a key cannot be written as SQL", ErrMalformedChange)
	}
	var keys []string
	if err := tx.NewRaw("?", bun.Safe(string(b))).Scan(ctx, &keys); err != nil {
		return nil, err
	}
	accepted := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		accepted[k] = struct{}{}
	}
	return accepted, nil
}

// writeUpserts writes rows with an upsert. With guard, and a version column, a
// stored row that is newer than the incoming one is kept (and counted as stale).
func (c *CQRSService[TData, TResponse, TRequest, TID]) writeUpserts(
	ctx context.Context, tx bun.Tx, gen schema.QueryGen, rows []*change[TData], guard bool,
) (stale int, err error) {
	b := make([]byte, 0, 4096)
	for start := 0; start < len(rows); {
		b = c.appendUpsertHead(b[:0])
		n := 0
		for start+n < len(rows) && n < upsertChunk && (n == 0 || len(b) < maxStatementBytes) {
			if n > 0 {
				b = append(b, ", "...)
			}
			b = c.appendValues(gen, b, rows[start+n])
			n++
		}
		b = c.appendUpsertTailGuarded(b, guard)
		affected, err := execRaw(ctx, tx, b)
		if err != nil {
			return stale, err
		}
		if guard && c.model.verField != nil {
			stale += n - int(affected)
		}
		start += n
	}
	return stale, nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) appendUpsertHead(b []byte) []byte {
	m := c.model
	b = append(b, "INSERT INTO "...)
	b = append(b, m.qualified...)
	b = append(b, " AS "...)
	b = append(b, tableAlias...)
	b = append(b, " ("...)
	for i, f := range m.colFields {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = append(b, f.SQLName...)
	}
	return append(b, ") VALUES "...)
}

// appendValues writes one row as (v1, v2, ...) with every column's exact value:
// unlike a bun model insert it never swaps a zero or nil value for the column's
// DEFAULT, so the read database holds precisely what the write database holds.
func (c *CQRSService[TData, TResponse, TRequest, TID]) appendValues(gen schema.QueryGen, b []byte, ch *change[TData]) []byte {
	rv := reflect.ValueOf(&ch.data).Elem()
	b = append(b, '(')
	for i, f := range c.model.colFields {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = f.AppendValue(gen, b, rv)
	}
	return append(b, ')')
}

// appendUpsertTailGuarded ends an upsert. With guard, a model with a version
// column keeps a stored row that is newer than the incoming one.
func (c *CQRSService[TData, TResponse, TRequest, TID]) appendUpsertTailGuarded(b []byte, guard bool) []byte {
	m := c.model
	b = append(b, " ON CONFLICT ("...)
	b = append(b, m.idField.SQLName...)
	b = append(b, ')')
	if len(m.setCols) == 0 {
		return append(b, " DO NOTHING"...)
	}
	b = append(b, " DO UPDATE SET "...)
	for i, name := range m.setCols {
		if i > 0 {
			b = append(b, ", "...)
		}
		f := m.fields[name]
		b = append(b, f.SQLName...)
		b = append(b, " = "...)
		b = append(b, excluded...)
		b = append(b, f.SQLName...)
	}
	if vf := m.verField; vf != nil && guard {
		// Keep the stored row when it is newer than the incoming change. A stored
		// NULL is older than anything, an incoming NULL is older than any value.
		b = append(b, " WHERE "...)
		b = append(b, tableAlias...)
		b = append(b, '.')
		b = append(b, vf.SQLName...)
		b = append(b, " IS NULL OR "...)
		b = append(b, tableAlias...)
		b = append(b, '.')
		b = append(b, vf.SQLName...)
		b = append(b, " <= "...)
		b = append(b, excluded...)
		b = append(b, vf.SQLName...)
	}
	return b
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) writeDeletes(
	ctx context.Context, tx bun.Tx, gen schema.QueryGen, rows []*change[TData],
) error {
	b := make([]byte, 0, 1024)
	var plain []*change[TData]
	for _, ch := range rows {
		if c.model.versionKnown(&ch.data) {
			// The old row's version is known: only delete a row that is not newer.
			b = c.appendGuardedDelete(gen, b[:0], ch)
			if _, err := execRaw(ctx, tx, b); err != nil {
				return err
			}
			continue
		}
		plain = append(plain, ch)
	}
	return c.deletePlain(ctx, tx, gen, plain)
}

// deletePlain deletes rows by key, whatever version they hold.
func (c *CQRSService[TData, TResponse, TRequest, TID]) deletePlain(
	ctx context.Context, tx bun.Tx, gen schema.QueryGen, plain []*change[TData],
) error {
	b := make([]byte, 0, 1024)
	for start := 0; start < len(plain); start += upsertChunk {
		end := min(start+upsertChunk, len(plain))
		b = c.appendDeleteHead(b[:0])
		for i, ch := range plain[start:end] {
			if i > 0 {
				b = append(b, ", "...)
			}
			b = c.model.idField.AppendValue(gen, b, reflect.ValueOf(&ch.data).Elem())
		}
		b = append(b, ')')
		if _, err := execRaw(ctx, tx, b); err != nil {
			return err
		}
	}
	return nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) appendDeleteHead(b []byte) []byte {
	m := c.model
	b = append(b, "DELETE FROM "...)
	b = append(b, m.qualified...)
	b = append(b, " AS "...)
	b = append(b, tableAlias...)
	b = append(b, " WHERE "...)
	b = append(b, tableAlias...)
	b = append(b, '.')
	b = append(b, m.idField.SQLName...)
	return append(b, " IN ("...)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) appendGuardedDelete(gen schema.QueryGen, b []byte, ch *change[TData]) []byte {
	m := c.model
	rv := reflect.ValueOf(&ch.data).Elem()
	b = append(b, "DELETE FROM "...)
	b = append(b, m.qualified...)
	b = append(b, " AS "...)
	b = append(b, tableAlias...)
	b = append(b, " WHERE "...)
	b = append(b, tableAlias...)
	b = append(b, '.')
	b = append(b, m.idField.SQLName...)
	b = append(b, " = "...)
	b = m.idField.AppendValue(gen, b, rv)
	b = append(b, " AND ("...)
	b = append(b, tableAlias...)
	b = append(b, '.')
	b = append(b, m.verField.SQLName...)
	b = append(b, " IS NULL OR "...)
	b = append(b, tableAlias...)
	b = append(b, '.')
	b = append(b, m.verField.SQLName...)
	b = append(b, " <= "...)
	b = m.verField.AppendValue(gen, b, rv)
	return append(b, ')')
}

// execRaw runs a statement built by this package and returns the rows it
// affected. The statement is passed as a trusted fragment so that a '?' inside
// a value is never mistaken for a placeholder; every value in it was escaped by
// bun's own appenders.
func execRaw(ctx context.Context, tx bun.Tx, stmt []byte) (int64, error) {
	if bytes.IndexByte(stmt, 0) >= 0 || bytes.Contains(stmt, bunErrorMarker) {
		// bun does not fail on a value it cannot render (a NUL byte, say): it writes
		// this marker into the statement, which the server then rejects as a syntax
		// error, a class of error that is retried. Catch it here, as the data's fault.
		return 0, fmt.Errorf("%w: a value cannot be written as SQL", ErrMalformedChange)
	}
	res, err := tx.NewRaw("?", bun.Safe(string(stmt))).Exec(ctx)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, nil // the driver cannot say; the guard count is only informational
	}
	return n, nil
}
