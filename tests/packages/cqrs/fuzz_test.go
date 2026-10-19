//go:build integration

package cqrs_test

import (
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"
	"time"
)

// ===========================================================================
// Seeded random tests. Each seed generates its own history of changes to the
// write database and its own list of things that go wrong while it is being
// replicated, then heals everything. Whatever the seed, the two databases must
// end up equal. A failure prints the seed; set CQRS_FUZZ_SEED to replay it, and
// CQRS_FUZZ_SEEDS to run more seeds than the default.
// ===========================================================================

func fuzzSeeds(def int) []uint64 {
	if s := os.Getenv("CQRS_FUZZ_SEED"); s != "" {
		if v, err := strconv.ParseUint(s, 10, 64); err == nil {
			return []uint64{v}
		}
	}
	n := def
	if s := os.Getenv("CQRS_FUZZ_SEEDS"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			n = v
		}
	}
	out := make([]uint64, n)
	for i := range out {
		out[i] = uint64(i + 1)
	}
	return out
}

// randomHistory makes a random sequence of real changes. It tracks which ids
// exist so every statement is valid, and keeps ids in a small space so that
// rows are created, changed, deleted and created again many times.
func randomHistory(w *world, rng *rand.Rand, steps int) {
	const space = 60
	exists := map[int64]bool{}
	refresh(w, exists, space) // histories are generated in chunks: start from what really exists
	pick := func() int64 { return int64(1 + rng.IntN(space)) }
	for range steps {
		id := pick()
		switch k := rng.IntN(12); {
		case k < 4: // create or update one row
			if exists[id] {
				w.set(id, fmt.Sprintf("o%d", rng.IntN(1000)), int64(rng.IntN(1_000_000)))
			} else {
				w.insert(id, fmt.Sprintf("n%d", rng.IntN(1000)), int64(rng.IntN(1_000_000)))
				exists[id] = true
			}
		case k < 6: // delete one row
			if exists[id] {
				w.remove(id)
				delete(exists, id)
			}
		case k < 8: // bulk insert of missing rows
			lo := pick()
			w.insertMissing(lo, lo+int64(rng.IntN(12)), "bi")
			refresh(w, exists, space)
		case k < 10: // bulk update
			lo := pick()
			hi := lo + int64(rng.IntN(15))
			w.bulkUpdate(lo, hi, int64(rng.IntN(100))-50)
		case k == 10: // bulk delete
			lo := pick()
			hi := lo + int64(rng.IntN(10))
			w.bulkRemove(lo, hi)
			refresh(w, exists, space)
		default: // odd values
			if exists[id] {
				vals := []string{"", "Zoë 日本語 🎉", `it's "quoted" \ ; -- ? %`, "  spaces  ", "line\nbreak\ttab"}
				w.mutate(id, id, "owner = ?, note = ?", vals[rng.IntN(len(vals))], vals[rng.IntN(len(vals))])
			}
		}
	}
}

// refresh re-reads which ids exist after a bulk statement.
func refresh(w *world, exists map[int64]bool, space int64) {
	for id := range exists {
		delete(exists, id)
	}
	for _, r := range w.jsonRows(`id BETWEEN 1 AND ?`, space+100) {
		exists[r.ID] = true
	}
}

// chaosMonkey causes random faults while the history replicates and heals them.
type chaosMonkey struct {
	r   *rig
	pm  *pump
	rng *rand.Rand
	log []string
}

func (c *chaosMonkey) step() {
	switch c.rng.IntN(9) {
	case 0:
		c.log = append(c.log, "broker down")
		c.pm.down.Store(true)
	case 1:
		c.log = append(c.log, "broker up")
		c.pm.down.Store(false)
	case 2:
		c.log = append(c.log, "commit failures on")
		c.pm.failCommit.Store(true)
	case 3:
		c.log = append(c.log, "commit failures off")
		c.pm.failCommit.Store(false)
	case 4:
		c.log = append(c.log, "kill read connections")
		killReadBackends(c.r.t, c.r.p, 0)
	case 5:
		c.log = append(c.log, "read-only")
		setReadOnly(c.r.t, c.r.p, true)
		killReadBackends(c.r.t, c.r.p, 0)
	case 6:
		c.log = append(c.log, "writable")
		setReadOnly(c.r.t, c.r.p, false)
	case 7:
		c.log = append(c.log, "connector re-emits a suffix")
		c.r.w.replaySuffix(c.rng.Float64())
	case 8:
		c.log = append(c.log, "consumer restarts from zero")
		c.r.f.rewindToZero(c.r.w)
	}
}

func (c *chaosMonkey) heal() {
	c.pm.down.Store(false)
	c.pm.failCommit.Store(false)
	setReadOnly(c.r.t, c.r.p, false)
	settle(c.r.t, c.r.p)
}

func TestFuzz_RandomHistoriesWithRandomFaults(t *testing.T) {
	seeds := fuzzSeeds(100)
	for _, parts := range []int{1, 3} {
		t.Run(fmt.Sprintf("partitions=%d", parts), func(t *testing.T) {
			labTest(t, parts, nil, func(l *lab) {
				for _, seed := range seeds {
					l.run(fmt.Sprintf("seed=%d", seed), func(t *testing.T, r *rig) {
						rng := rand.New(rand.NewPCG(seed, uint64(parts)*0x9e3779b9))
						pm := r.w.startPump(t, r.f, 5+rng.IntN(60))
						cm := &chaosMonkey{r: r, pm: pm, rng: rng}
						defer func() {
							if t.Failed() {
								t.Logf("seed=%d partitions=%d; faults: %v", seed, parts, cm.log)
							}
						}()
						for range 6 {
							randomHistory(r.w, rng, 8+rng.IntN(12))
							cm.step()
							time.Sleep(time.Duration(rng.IntN(25)) * time.Millisecond)
						}
						cm.heal()
						r.verifyDeep(pm, nil)
					})
				}
			})
		})
	}
}

// The same histories delivered by hand in random batch sizes, with random
// redeliveries of earlier batches, to a service whose only fault is Kafka's
// behaviour. No pump, no infrastructure faults: pure ordering and idempotency.
func TestFuzz_RandomDeliveryOrderOfTheSameLog(t *testing.T) {
	seeds := fuzzSeeds(100)
	labTest(t, 3, nil, func(l *lab) {
		for _, seed := range seeds {
			l.run(fmt.Sprintf("seed=%d", seed), func(t *testing.T, r *rig) {
				rng := rand.New(rand.NewPCG(seed, 77))
				randomHistory(r.w, rng, 60+rng.IntN(60))
				ctx := longCtx(t)
				for range 3 + rng.IntN(3) {
					for part := range r.w.parts {
						size := r.w.size(part)
						if size == 0 {
							continue
						}
						// A random window of the log: the next unseen records, or a replay
						// from somewhere earlier, ending anywhere. Kafka never skips ahead, so
						// a window starts at or before what the database already holds.
						from := r.p.checkpoint(r.group, part)
						if rng.IntN(3) == 0 {
							from = int64(rng.IntN(int(from) + 1))
						}
						to := from + int64(1+rng.IntN(40))
						if to > size {
							to = size
						}
						if err := r.w.deliverRange(ctx, r.f, part, from, to, 1+rng.IntN(25)); err != nil {
							t.Fatal(err)
						}
					}
				}
				// Whatever happened, finish by delivering the rest in order.
				if err := r.w.deliverNew(ctx, r.f, 1+rng.IntN(30)); err != nil {
					t.Fatal(err)
				}
				for part := range r.w.parts {
					// deliverRange does not commit; finish with a clean pass from the checkpoint.
					cp := r.p.checkpoint(r.group, part)
					if err := r.w.deliverRange(ctx, r.f, part, cp, r.w.size(part), 50); err != nil {
						t.Fatal(err)
					}
				}
				r.p.assertInSync(t)
			})
		}
	})
}

// Several replicas, each handed random slices of the same log at once.
func TestFuzz_RandomReplicasRaceOnRandomLogs(t *testing.T) {
	seeds := fuzzSeeds(50)
	labTest(t, 2, nil, func(l *lab) {
		for _, seed := range seeds {
			l.run(fmt.Sprintf("seed=%d", seed), func(t *testing.T, r *rig) {
				rng := rand.New(rand.NewPCG(seed, 4242))
				randomHistory(r.w, rng, 80+rng.IntN(60))
				fs := []*fakeCDC{r.f}
				for range 1 + rng.IntN(3) {
					f, _ := r.replica()
					fs = append(fs, f)
				}
				r.race(fs, 1+rng.IntN(40))
				r.p.assertInSync(t)
			})
		}
	})
}

// Poison mixed into random histories: a row the read database rejects for good
// is parked and the rest still arrives, whatever the seed.
func TestFuzz_RandomPoisonDoesNotStopTheStream(t *testing.T) {
	seeds := fuzzSeeds(50)
	labTest(t, 2, nil, func(l *lab) {
		for _, seed := range seeds {
			l.run(fmt.Sprintf("seed=%d", seed), func(t *testing.T, r *rig) {
				rng := rand.New(rand.NewPCG(seed, 99))
				r.p.exec(r.p.read, `ALTER TABLE accounts DROP CONSTRAINT IF EXISTS balance_ok`)
				r.p.exec(r.p.read, `ALTER TABLE accounts ADD CONSTRAINT balance_ok CHECK (balance <> 13)`)
				// Rows with balance 13 are poison for this read model. Keep them out of the
				// comparison: they are rejected, and so is every later change of the same row.
				poisoned := map[int64]bool{}
				for range 40 + rng.IntN(40) {
					id := int64(1 + rng.IntN(40))
					bal := int64(rng.IntN(100))
					if rng.IntN(8) == 0 {
						bal = 13
					}
					if bal == 13 { // also reached by chance from the random range
						poisoned[id] = true
					}
					if _, ok := rowExists(r, id); ok {
						r.w.set(id, "x", bal)
					} else {
						r.w.insert(id, "x", bal)
					}
				}
				if err := r.w.deliverNew(longCtx(t), r.f, 1+rng.IntN(25)); err != nil {
					t.Fatal(err)
				}
				// A row that was poisoned at some point may hold an older accepted version in
				// the read model; compare only rows that were never poisoned.
				var skip []int64
				for id := range poisoned {
					skip = append(skip, id)
				}
				r.p.assertInSync(t, skip...)
			})
		}
	})
}

func rowExists(r *rig, id int64) (Account, bool) {
	all, err := accountsOf(r.p.write.Client())
	if err != nil {
		r.t.Fatal(err)
	}
	a, ok := all[id]
	return a, ok
}
