//go:build integration

package cqrs_test

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
)

// OrderByLSN removes the limit of the version guard: a row that is deleted and
// created again restarts its version counter at 1, so a replay of an old window
// of the log cannot be ordered against the newer generation by version. The WAL
// position never goes back, and the read database remembers it for every row,
// deletes included.

func withLSN(c *cqrsSvc) { c.OrderByLSN = true }

// forgetPosition is what an operator's restore (or a recreated topic) does: the
// checkpoints are gone, so the next delivery starts again from wherever Kafka
// hands it, while the data and the row states stay.
func (r *rig) forgetCheckpoints() {
	r.t.Helper()
	r.p.exec(r.p.read, `DELETE FROM cqrs_checkpoints`)
}

// The scenario of the report, on the version guard: it fails, which is why the
// limit existed. Kept as a test so the difference is on record.
func TestLSN_VersionGuardAloneCannotOrderGenerations(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	ctx := longCtx(t)
	r.w.bulkInsert(1, 10, "gen1")
	r.w.bulkUpdate(1, 10, 5)
	r.w.bulkUpdate(1, 10, 5) // version 3
	r.w.bulkRemove(1, 10)
	r.w.bulkInsert(1, 10, "gen2") // version 1 again
	if err := r.w.deliverNew(ctx, r.f, 50); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t)
	// Replay the first generation only (offsets 0..29: the inserts and both rounds
	// of updates), with the checkpoints lost, then stop. By version, the old
	// generation's version-3 update is newer than the recreated row's version 1, so
	// it overwrites it, and nothing after it comes to repair that.
	r.forgetCheckpoints()
	r.f.rewindToZero(r.w)
	if err := r.w.deliverRange(ctx, r.f, 0, 0, 30, 40); err != nil {
		t.Fatal(err)
	}
	if d, err := r.p.syncDiff(); err != nil || d == "" {
		t.Fatalf("the version guard alone was expected to lose here (the documented limit); diff=%q err=%v", d, err)
	}
}

// The same scenario with OrderByLSN: the databases stay equal.
func TestLSN_ReplayOfAnOldWindowAfterRecreateIsHarmless(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1, withLSN)
	ctx := longCtx(t)
	r.w.bulkInsert(1, 10, "gen1")
	r.w.bulkUpdate(1, 10, 5)
	r.w.bulkUpdate(1, 10, 5)
	r.w.bulkRemove(1, 10)
	r.w.bulkInsert(1, 10, "gen2")
	if err := r.w.deliverNew(ctx, r.f, 50); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t)
	r.forgetCheckpoints()
	r.f.rewindToZero(r.w)
	if err := r.w.deliverRange(ctx, r.f, 0, 0, 30, 40); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t)
	if s := r.stats(); s.Stale == 0 {
		t.Fatalf("the replay carries older changes: they must have been turned away: %+v", s)
	}
}

// Any prefix of the log replayed after everything was applied, in any batch size.
func TestLSN_AnyOldWindowReplayedAfterEverythingIsHarmless(t *testing.T) {
	for _, parts := range []int{1, 3} {
		t.Run(fmt.Sprintf("partitions=%d", parts), func(t *testing.T) {
			labTest(t, parts, []func(*cqrsSvc){withLSN}, func(l *lab) {
				for _, sc := range scripts() {
					for _, frac := range []float64{0, 0.25, 0.5, 0.9} {
						l.run(fmt.Sprintf("%s/from=%.2f", sc.name, frac), func(t *testing.T, r *rig) {
							sc.run(r.w)
							ctx := longCtx(t)
							if err := r.w.deliverNew(ctx, r.f, 40); err != nil {
								t.Fatal(err)
							}
							r.p.assertInSync(t)
							r.forgetCheckpoints()
							for part := range r.w.parts {
								n := r.w.size(part)
								from := int64(float64(n) * frac)
								// An old window: from somewhere earlier, up to a point in the
								// middle of the log, then the consumer is gone.
								to := from + (n-from)/2 + 1
								if err := r.w.deliverRange(ctx, r.f, part, from, min(to, n), 7); err != nil {
									t.Fatal(err)
								}
							}
							r.p.assertInSync(t)
						})
					}
				}
			})
		})
	}
}

// The strongest form: a window replayed at the very point the version guard is
// blind, in the middle of a generation change, then the consumer catches up.
func TestLSN_RestoredReadModelAndLostCheckpointsConverge(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2, withLSN)
	ctx := longCtx(t)
	r.w.bulkInsert(1, 60, "a")
	r.w.bulkUpdate(1, 60, 1)
	if err := r.w.deliverNew(ctx, r.f, 30); err != nil {
		t.Fatal(err)
	}
	snap := r.takeSnapshot()
	r.w.bulkRemove(1, 30)
	r.w.bulkInsert(1, 30, "reborn")
	r.w.bulkUpdate(1, 60, 2)
	r.w.bulkRemove(50, 60)
	r.w.bulkInsert(500, 10, "n")
	if err := r.w.deliverNew(ctx, r.f, 30); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t)
	// Restore the old data and forget the checkpoints, but keep the row states
	// (they are in the same database: restore them too, as a backup would, then
	// the delete tombstones of the later changes are older than the data).
	r.restore(snap, false)
	r.p.exec(r.p.read, `TRUNCATE cqrs_row_state`)
	r.f.rewindToZero(r.w)
	if err := r.w.deliverNew(ctx, r.f, 25); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t)
}

// Positions order a model that has no version column at all.
func TestLSN_OrdersModelsWithoutAVersionColumn(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1, withLSN)
	ctx := longCtx(t)
	r.w.bulkInsert(1, 5, "a")
	// A change of the same row delivered out of order: the newer is first.
	older := r.w.set(1, "older", 1)
	newer := r.w.set(1, "newer", 2)
	rest := r.w.slice(0, 0, 100)
	var head []cdc.Record
	for _, rec := range rest {
		if rec.Offset != older.Offset && rec.Offset != newer.Offset {
			head = append(head, rec)
		}
	}
	// Offsets must stay strictly increasing within a batch, so deliver in two
	// batches: the later change first would need a lower offset, which a real log
	// never has. What can happen is a replayed window: deliver all, then the older
	// again with a higher offset.
	if err := r.w.deliverNew(ctx, r.f, 50); err != nil {
		t.Fatal(err)
	}
	r.w.duplicate(0, older.Offset, older.Offset+1)
	if err := r.w.deliverNew(ctx, r.f, 50); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t)
	if got := r.p.readAccounts()[1].Owner; got != "newer" {
		t.Fatalf("a replayed older change overwrote a newer one: owner = %q", got)
	}
	_ = head
}

// An insert replayed after the row was deleted must not bring the row back.
func TestLSN_ReplayedInsertDoesNotResurrectADeletedRow(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1, withLSN)
	ctx := longCtx(t)
	r.w.bulkInsert(1, 10, "a")
	r.w.bulkRemove(1, 10)
	if err := r.w.deliverNew(ctx, r.f, 100); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t)
	if n := r.p.count(r.p.read, "accounts"); n != 0 {
		t.Fatalf("%d rows after everything was deleted", n)
	}
	r.forgetCheckpoints()
	r.f.rewindToZero(r.w)
	// Replay only the inserts: the deletes that followed them are not repeated.
	if err := r.w.deliverRange(ctx, r.f, 0, 0, 10, 4); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t)
	if n := r.p.count(r.p.read, "accounts"); n != 0 {
		t.Fatalf("a replayed insert resurrected %d deleted rows", n)
	}
	var dead int
	if err := r.p.read.Client().NewRaw(`SELECT count(*) FROM cqrs_row_state WHERE deleted`).Scan(ctx, &dead); err != nil || dead != 10 {
		t.Fatalf("the delete tombstones were not kept: %d (%v)", dead, err)
	}
}

// Without a position in the event the service behaves as it always did.
func TestLSN_EventsWithoutPositionKeepWorking(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1, withLSN)
	ctx := longCtx(t)
	var batch []cdc.Record
	for i := range 20 {
		batch = append(batch, create(0, int64(i), int64(i+1), "o", 10, 1)) // no lsn
	}
	if err := r.f.deliver(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if n := r.p.count(r.p.read, "accounts"); n != 20 {
		t.Fatalf("rows = %d", n)
	}
	if n := r.p.count(r.p.read, "cqrs_row_state"); n != 0 {
		t.Fatalf("changes without a position must not touch the row-state table, %d rows", n)
	}
}

// A service that asks for OrderByLSN refuses to start without the table.
func TestLSN_RunNeedsTheRowStateTable(t *testing.T) {
	t.Parallel()
	p := newPair(t, 4)
	p.exec(p.read, `DROP TABLE cqrs_row_state`)
	f := newFakeCDC("g-needs-table")
	svc := newService(t, p, f, withLSN)
	if err := svc.Run(ctxTimeout(t, 20e9)); err == nil {
		_ = svc.Stop(ctxTimeout(t, 20e9))
		t.Fatal("Run succeeded without cqrs_row_state")
	}
}

// Random histories, random windows replayed from random points with the
// checkpoints lost over and over: the databases must always end equal.
func TestLSN_RandomReplaysAlwaysConverge(t *testing.T) {
	seeds := fuzzSeeds(100)
	labTest(t, 3, []func(*cqrsSvc){withLSN}, func(l *lab) {
		for _, seed := range seeds {
			l.run(fmt.Sprintf("seed=%d", seed), func(t *testing.T, r *rig) {
				rng := rand.New(rand.NewPCG(seed, 2024))
				ctx := longCtx(t)
				randomHistory(r.w, rng, 60+rng.IntN(60))
				for range 2 + rng.IntN(3) {
					if err := r.w.deliverNew(ctx, r.f, 1+rng.IntN(30)); err != nil {
						t.Fatal(err)
					}
					// Checkpoints lost, then an arbitrary window of the log is replayed
					// (prefixes, middles, anything) and the consumer vanishes.
					r.forgetCheckpoints()
					for part := range r.w.parts {
						n := r.w.size(part)
						if n == 0 {
							continue
						}
						from := int64(rng.IntN(int(n)))
						to := min(n, from+int64(1+rng.IntN(int(n))))
						if err := r.w.deliverRange(ctx, r.f, part, from, to, 1+rng.IntN(20)); err != nil {
							t.Fatal(err)
						}
					}
					randomHistory(r.w, rng, 10+rng.IntN(20))
					r.f.rewindToZero(r.w)
				}
				// Finally the consumer restarts from zero and reads to the end.
				r.forgetCheckpoints()
				r.f.rewindToZero(r.w)
				if err := r.w.deliverNew(ctx, r.f, 1+rng.IntN(40)); err != nil {
					t.Fatal(err)
				}
				r.p.assertInSync(t)
			})
		}
	})
}

// Several replicas at once, with positions: still exactly the write database.
func TestLSN_ReplicasRaceOnRandomLogs(t *testing.T) {
	seeds := fuzzSeeds(30)
	labTest(t, 2, []func(*cqrsSvc){withLSN}, func(l *lab) {
		for _, seed := range seeds {
			l.run(fmt.Sprintf("seed=%d", seed), func(t *testing.T, r *rig) {
				rng := rand.New(rand.NewPCG(seed, 8080))
				randomHistory(r.w, rng, 80+rng.IntN(60))
				fs := []*fakeCDC{r.f}
				for range 1 + rng.IntN(3) {
					f, _ := r.replica(withLSN)
					fs = append(fs, f)
				}
				r.race(fs, 1+rng.IntN(40))
				r.p.assertInSync(t)
			})
		}
	})
}

// The whole script matrix again, with positions on.
func TestLSN_ScriptsByBatchSize(t *testing.T) {
	for _, parts := range []int{1, 3} {
		t.Run(fmt.Sprintf("partitions=%d", parts), func(t *testing.T) {
			labTest(t, parts, []func(*cqrsSvc){withLSN}, func(l *lab) {
				for _, sc := range scripts() {
					for _, size := range []int{1, 7, 100} {
						l.run(fmt.Sprintf("%s/batch=%d", sc.name, size), func(t *testing.T, r *rig) {
							sc.run(r.w)
							if err := r.w.deliverNew(longCtx(t), r.f, size); err != nil {
								t.Fatal(err)
							}
							r.p.assertInSync(t)
							for part := range r.w.parts {
								if got, want := r.p.checkpoint(r.group, part), r.w.size(part); got != want {
									t.Fatalf("partition %d checkpoint = %d, want %d", part, got, want)
								}
							}
						})
					}
				}
			})
		})
	}
}
