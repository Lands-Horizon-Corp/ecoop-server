//go:build integration

package cqrs_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cqrs"
)

// duplicate appends copies of records [from, to) of a partition at new offsets:
// what a connector does when it crashes after writing to Kafka but before it
// recorded its position, and so emits the same changes again.
func (w *world) duplicate(part int32, from, to int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	src := append([]cdc.Record(nil), w.log[part][from:to]...)
	for _, r := range src {
		w.clock++
		r.Offset = int64(len(w.log[part]))
		r.Timestamp = baseTime.Add(time.Duration(w.clock) * time.Millisecond)
		w.log[part] = append(w.log[part], r)
	}
}

// bumpTo updates one row's balance and version and emits the change.
func (w *world) bumpTo(id, balance int64) cdc.Record {
	w.t.Helper()
	w.exec(`UPDATE accounts SET balance = ?, version = version + 1, updated_at = clock_timestamp() WHERE id = ?`, balance, id)
	return w.emit(id, cdc.OpUpdate, "null", w.jsonRows(`id = ?`, id)[0].J)
}

// replayAll duplicates every partition's whole log once.
func (w *world) replayAll() {
	for part := range w.parts {
		w.duplicate(part, 0, w.size(part))
	}
}

// pairSync waits until the two databases of p are equal, or fails with what
// differs and the service's stats.
func pairSync(t testing.TB, p *pair, svc interface{ Stats() cqrs.Stats }, d time.Duration, except ...int64) {
	t.Helper()
	deadline := time.Now().Add(d)
	var last string
	for {
		diff, err := p.syncDiff(except...)
		switch {
		case err != nil:
			last = err.Error()
		case diff == "":
			return
		default:
			last = diff
		}
		if time.Now().After(deadline) {
			t.Fatalf("the databases never converged: %s (service: %+v)", last, svc.Stats())
		}
		time.Sleep(60 * time.Millisecond)
	}
}

// e2e is the whole real pipeline for one test: Postgres, a Debezium connector of
// its own, Kafka, the real cdc service and the CQRS service.
type e2e struct {
	t      testing.TB
	s      stack
	p      *pair
	prefix string
	group  string
	cdc    cdc.ICDCService
	svc    svcT
}

func newE2E(t testing.TB, mods ...func(*cqrsSvc)) *e2e {
	t.Helper()
	s := skipUnlessDebezium(t)
	p := newPair(t, 16)
	x := &e2e{t: t, s: s, p: p, group: fmt.Sprintf("g-e2e-%d-%d", runID%1_000_000, seqNo.Add(1))}
	x.prefix = s.connector(t, p)
	x.cdc = s.realCDC(t, x.group, x.prefix)
	x.svc = newService(t, p, nil, append([]func(*cqrsSvc){func(c *cqrsSvc) {
		c.CDCService = x.cdc
		c.Topic = ""
		c.BatchSize = 300
	}}, mods...)...)
	start(t, x.svc)
	return x
}

func (x *e2e) write(query string, args ...any) {
	x.t.Helper()
	if _, err := x.p.write.Client().ExecContext(context.Background(), query, args...); err != nil {
		x.t.Fatalf("write db %q: %v", query, err)
	}
}

func (x *e2e) insertRange(from, n int64, owner string) {
	x.t.Helper()
	x.write(`INSERT INTO accounts (id, owner, balance, version, updated_at)
		SELECT g, CAST(? AS text) || g, g, 1, clock_timestamp() FROM generate_series(CAST(? AS bigint), CAST(? AS bigint)) g`,
		owner, from, from+n-1)
}

func (x *e2e) bump(lo, hi, delta int64) {
	x.t.Helper()
	x.write(`UPDATE accounts SET balance = balance + ?, version = version + 1, updated_at = clock_timestamp() WHERE id BETWEEN ? AND ?`, delta, lo, hi)
}

func (x *e2e) sync(except ...int64) {
	x.t.Helper()
	pairSync(x.t, x.p, x.svc, 120*time.Second, except...)
}

var _ = (*testing.T)(nil)
