//go:build integration

package pagination_test

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pagination"
)

// Run with -race. One service is shared by many goroutines issuing every
// kind of read; each must get the right answer for its own filter and none
// may see another's.
func TestRace_SharedServiceReturnsEachCallersAnswer(t *testing.T) {
	fx := need(t)
	tiers := map[string][]string{
		"gold":   {"Alice", "Carol", emile, "Grace"},
		"silver": {"Bob", "O'Brien", "Heidi"},
		"bronze": {"Dave_100%", "Frank", "Ivan"},
	}
	var wg sync.WaitGroup
	var bad atomic.Int64
	for i := range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tier, want := range tiers {
				sf := and(f("tier", pagination.ModeEqual, tier))
				got, err := fx.svc.Filter(ctxT(t), sf)
				if err != nil || !sameSet(nameList(got), want) {
					bad.Add(1)
					t.Errorf("goroutine %d: Filter(%s) = %v, %v", i, tier, nameList(got), err)
				}
				if n, err := fx.svc.Count(ctxT(t), sf); err != nil || int(n) != len(want) {
					bad.Add(1)
					t.Errorf("goroutine %d: Count(%s) = %d, %v", i, tier, n, err)
				}
				if got := walk(t, fx, filterWithSort(sf, asc("balance")), 2); !sameSet(got, want) {
					bad.Add(1)
					t.Errorf("goroutine %d: walk(%s) = %v", i, tier, got)
				}
			}
		}()
	}
	wg.Wait()
	if bad.Load() > 0 {
		t.Fatalf("%d wrong answers under concurrency", bad.Load())
	}
}

// The caller's filter value is not mutated by the service, so one filter
// can be reused by many goroutines at once.
func TestRace_FilterReusedAcrossGoroutines(t *testing.T) {
	fx := need(t)
	shared := and(
		f("name", pagination.ModeContains, "É"), // normalized internally
		f("tier", pagination.ModeInside, []any{"gold", "silver"}),
	)
	before := fmt.Sprintf("%#v", shared)
	var wg sync.WaitGroup
	for range 48 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got, err := fx.svc.Filter(ctxT(t), shared); err != nil || len(got) != 1 {
				t.Errorf("Filter = %v, %v", nameList(got), err)
			}
		}()
	}
	wg.Wait()
	if after := fmt.Sprintf("%#v", shared); after != before {
		t.Fatalf("filter was mutated:\n before %s\n after  %s", before, after)
	}
}

// Writers change the table while readers walk it. Readers always see a
// consistent page: sorted, no row twice within one walk.
func TestRace_ReadersWhileWriting(t *testing.T) {
	fx := ownFixture(t, seed())
	db := fx.sql.Client()
	stop := make(chan struct{})
	var writers sync.WaitGroup
	writers.Add(1)
	go func() {
		defer writers.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			now := time.Now()
			row := &Account{Name: fmt.Sprintf("W%d", i), Tier: "gold", Balance: float64(i), CreatedAt: now, UpdatedAt: now}
			_, _ = db.NewInsert().Model(row).Exec(ctxT(t))
			if i%3 == 0 {
				_, _ = db.NewDelete().Model((*Account)(nil)).Where("name = ?", fmt.Sprintf("W%d", i-2)).Exec(ctxT(t))
			}
		}
	}()
	var readers sync.WaitGroup
	for range 16 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for range 10 {
				got := walk(t, fx, filterWithSort(and(), asc("id")), 4)
				if dup := firstDuplicate(got); dup != "" {
					t.Errorf("%s served twice in one walk: %v", dup, got)
					return
				}
				for _, n := range allNames {
					if !slices.Contains(got, n) {
						t.Errorf("seeded row %s missing from a walk that raced with inserts", n)
						return
					}
				}
			}
		}()
	}
	readers.Wait()
	close(stop)
	writers.Wait()
}

func firstDuplicate(names []string) string {
	seen := map[string]bool{}
	for _, n := range names {
		if seen[n] {
			return n
		}
		seen[n] = true
	}
	return ""
}

// Reads inside a transaction see that transaction's own writes.
func TestRace_TransactionSeesItsOwnWrites(t *testing.T) {
	fx := ownFixture(t, seed())
	ctx := ctxT(t)
	tx, err := fx.sql.Client().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	now := time.Now()
	if _, err := tx.NewInsert().Model(&Account{Name: "Uncommitted", Tier: "gold", CreatedAt: now, UpdatedAt: now}).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	inTx, err := fx.svc.FilterWithTx(ctx, &tx, and(f("name", pagination.ModeEqual, "Uncommitted")))
	if err != nil || len(inTx) != 1 {
		t.Fatalf("inside the tx: %v, %v", nameList(inTx), err)
	}
	outside, err := fx.svc.Filter(ctx, and(f("name", pagination.ModeEqual, "Uncommitted")))
	if err != nil || len(outside) != 0 {
		t.Fatalf("outside the tx saw uncommitted data: %v, %v", nameList(outside), err)
	}
}
