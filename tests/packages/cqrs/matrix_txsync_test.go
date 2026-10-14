//go:build integration

package cqrs_test

import (
	"fmt"
	"testing"
)

// ===========================================================================
// Transactional synchronisation matrix. Every script (a sequence of real
// changes to the write database) is replayed through the service with every
// batch size, over every partition count. Whatever the slicing, the read
// database must equal the write database, and the checkpoints must stand at the
// end of each partition's log.
// ===========================================================================

var batchSizes = []int{1, 2, 3, 5, 8, 13, 50, 1000}

func TestMatrix_ScriptsByBatchSize(t *testing.T) {
	for _, parts := range []int{1, 3} {
		t.Run(fmt.Sprintf("partitions=%d", parts), func(t *testing.T) {
			labTest(t, parts, nil, func(l *lab) {
				for _, sc := range scripts() {
					for _, size := range batchSizes {
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
							if s := r.stats(); s.DeadLettered != 0 {
								t.Fatalf("nothing is poison here: %+v", s)
							}
						})
					}
				}
			})
		})
	}
}
