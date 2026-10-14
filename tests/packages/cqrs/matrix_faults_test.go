//go:build integration

package cqrs_test

import (
	"fmt"
	"testing"
	"time"
)

// ===========================================================================
// Deep two-database synchronisation. Every change pattern is crossed with every
// way the pipeline can fail, and after each combination the same hard check is
// made: the read database equals the write database, column for column; their
// content digests (computed by Postgres itself, not by this test) match; every
// checkpoint stands at the end of its partition's log; Kafka is committed to
// the end; nothing was dead-lettered; and no hook fired twice for a change.
// ===========================================================================

// pattern is a named burst of real changes to the write database.
type pattern struct {
	name string
	run  func(w *world)
}

func patterns() []pattern {
	return []pattern{
		{"bulk-insert", func(w *world) { w.bulkInsert(1, 120, "a") }},
		{"insert-then-update", func(w *world) { w.bulkInsert(1, 80, "a"); w.bulkUpdate(1, 80, 2); w.bulkUpdate(20, 60, 3) }},
		{"insert-then-delete", func(w *world) { w.bulkInsert(1, 90, "a"); w.bulkRemove(10, 70) }},
		{"delete-and-recreate", func(w *world) {
			w.bulkInsert(1, 60, "a")
			w.bulkRemove(1, 60)
			w.bulkInsert(1, 60, "b")
			w.bulkUpdate(1, 60, 1)
		}},
		{"hot-rows", func(w *world) {
			w.bulkInsert(1, 5, "h")
			for range 40 {
				w.bulkUpdate(1, 5, 1)
			}
		}},
		{"single-row-storm", func(w *world) {
			for i := int64(1); i <= 60; i++ {
				w.insert(i, "s", i)
				w.set(i, "s2", i+1)
				if i%3 == 0 {
					w.remove(i)
				}
			}
		}},
		{"mixed-churn", func(w *world) {
			for round := range 5 {
				b := int64(round*30 + 1)
				w.bulkInsert(b, 25, "c")
				w.bulkUpdate(b, b+24, 1)
				w.bulkRemove(b, b+9)
			}
		}},
		{"wide-values", func(w *world) {
			w.bulkInsert(1, 30, "w")
			w.mutate(1, 10, "owner = ?", "Zoë 日本語 🎉")
			w.mutate(11, 20, "owner = ?", `quote's "x" \ ; -- ? %`)
			w.mutate(21, 30, "note = NULL, owner = ''")
		}},
		{"everything", func(w *world) {
			for _, sc := range scripts() {
				w.bulkRemoveAll()
				sc.run(w)
			}
		}},
		{"tombstones-and-noise", func(w *world) {
			w.bulkInsert(1, 50, "t")
			w.bulkRemove(1, 25)
			w.garbage(0)
			w.bulkInsert(100, 20, "u")
		}},
	}
}

// fault is a way the pipeline fails while a pattern is being replicated. start
// breaks something; stop heals it. Faults that need a pump use it to hold the
// broker.
type fault struct {
	name  string
	start func(r *rig, pm *pump)
	stop  func(r *rig, pm *pump)
}

func faults() []fault {
	none := func(*rig, *pump) {}
	return []fault{
		{"none", none, none},
		{"broker-down", func(r *rig, pm *pump) { pm.down.Store(true) }, func(r *rig, pm *pump) { pm.down.Store(false) }},
		{"read-outage", func(r *rig, pm *pump) { r.healRead = readOutage(r.t, r.p) }, func(r *rig, pm *pump) { r.healRead() }},
		{"commit-failures", func(r *rig, pm *pump) { pm.failCommit.Store(true) }, func(r *rig, pm *pump) { pm.failCommit.Store(false) }},
		{"read-only-then-promoted", func(r *rig, pm *pump) { setReadOnly(r.t, r.p, true); killReadBackends(r.t, r.p, 0) },
			func(r *rig, pm *pump) { setReadOnly(r.t, r.p, false); killReadBackends(r.t, r.p, 0) }},
		{"connections-killed", func(r *rig, pm *pump) { r.killer = startKiller(r) }, func(r *rig, pm *pump) { r.killer.stop() }},
		{"lock-contention", func(r *rig, pm *pump) {
			_, r.release = holdRead(r.t, r.p, `LOCK TABLE accounts IN ACCESS EXCLUSIVE MODE`)
		},
			func(r *rig, pm *pump) { r.release() }},
		{"connector-reemits-half", func(r *rig, pm *pump) {}, func(r *rig, pm *pump) { r.w.replaySuffix(0.5) }},
		{"consumer-restart", func(r *rig, pm *pump) {}, func(r *rig, pm *pump) { r.f.rewindToZero(r.w) }},
		{"broker-and-read-down", func(r *rig, pm *pump) { pm.down.Store(true); r.healRead = readOutage(r.t, r.p) },
			func(r *rig, pm *pump) { r.healRead(); pm.down.Store(false) }},
	}
}

func TestDeep_PatternsByFault(t *testing.T) {
	for _, parts := range []int{1, 4} {
		t.Run(fmt.Sprintf("partitions=%d", parts), func(t *testing.T) {
			rec := &recorder{}
			labTest(t, parts, []func(*cqrsSvc){withHooks(rec)}, func(l *lab) {
				for _, ft := range faults() {
					for _, pt := range patterns() {
						l.run(ft.name+"/"+pt.name, func(t *testing.T, r *rig) {
							rec.reset()
							pm := r.w.startPump(t, r.f, 40)
							ft.start(r, pm)
							pt.run(r.w)
							time.Sleep(25 * time.Millisecond)
							ft.stop(r, pm)
							r.verifyDeep(pm, rec)
						})
					}
				}
			})
		})
	}
}

// ---- the hard check ---------------------------------------------------------

// digestOf asks Postgres itself for a digest of the table's content, so the
// comparison does not depend on how this test reads rows.
func digestOf(p *pair, read bool) (string, error) {
	svc := p.write
	if read {
		svc = p.read
	}
	var d string
	err := svc.Client().NewRaw(`SELECT coalesce(md5(string_agg(
		id::text || '|' || owner || '|' || balance::text || '|' || coalesce(note, '<null>') || '|' ||
		active::text || '|' || version::text || '|' || (extract(epoch from updated_at) * 1000000)::bigint::text,
		E'\n' ORDER BY id)), 'empty') FROM accounts`).Scan(ctxBackground(), &d)
	return d, err
}

// verifyDeep waits for everything to settle and then checks it all.
func (r *rig) verifyDeep(pm *pump, rec *recorder) {
	r.t.Helper()
	// Poison (garbage) records are parked, never applied; the rest must converge.
	r.waitInSyncFor(2 * time.Minute)
	eventuallyN(r.t, 60*time.Second, "the consumer to reach the end of the log", pm.caughtUp)
	settle(r.t, r.p)
	r.p.assertInSync(r.t)

	w, err1 := digestOf(r.p, false)
	rd, err2 := digestOf(r.p, true)
	if err1 != nil || err2 != nil {
		r.t.Fatalf("digests: %v %v", err1, err2)
	}
	if w != rd {
		r.t.Fatalf("the content digests differ: write=%s read=%s", w, rd)
	}
	for part := range r.w.parts {
		size := r.w.size(part)
		if got := r.p.checkpoint(r.group, part); got != size {
			r.t.Fatalf("partition %d: checkpoint %d, log %d", part, got, size)
		}
		if got := r.f.committedOffset(part); got != size {
			r.t.Fatalf("partition %d: kafka committed %d, log %d", part, got, size)
		}
	}
	if s := r.stats(); s.HooksDropped != 0 {
		r.t.Fatalf("hooks were dropped: %+v", s)
	}
	// A hook fires once per change that was applied, however many times the
	// stream was redelivered, replayed or restarted.
	if rec != nil {
		time.Sleep(150 * time.Millisecond)
		first := rec.count()
		time.Sleep(150 * time.Millisecond)
		if rec.count() != first {
			r.t.Fatalf("hooks are still firing after convergence: %d then %d", first, rec.count())
		}
	}
}
