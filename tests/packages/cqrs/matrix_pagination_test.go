//go:build integration

package cqrs_test

import (
	"fmt"
	"slices"
	"sort"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pagination"
)

// ===========================================================================
// Pagination over the replicated read model. The reader's queries are the point
// of CQRS: after replication, paging through the read database must show exactly
// what the write database holds, in order, with no row missed or repeated, at
// every page size, in both directions, under every sort the model offers.
// ===========================================================================

func paged(r *rig, f pagination.StructuredFilter, size int) (pages [][]*Account, err error) {
	svc := pagination.NewPaginationService(&pagination.PaginationService[Account, int64]{
		ReadSQLService: r.p.read, WriteSQLService: r.p.write,
	}, nil)
	var cursor *string
	for range 10000 {
		res, err := svc.PaginateFilter(longCtx(r.t), f, pagination.Pagination{PageSize: size, Cursor: cursor})
		if err != nil {
			return nil, err
		}
		if len(res.Data) > 0 {
			pages = append(pages, res.Data)
		}
		if res.NextCursor == nil {
			return pages, nil
		}
		cursor = res.NextCursor
	}
	return nil, fmt.Errorf("pagination never ended")
}

func flatten(pages [][]*Account) []Account {
	var out []Account
	for _, p := range pages {
		for _, a := range p {
			out = append(out, *a)
		}
	}
	return out
}

func sortedBy(as []Account, less func(a, b Account) int) []Account {
	out := slices.Clone(as)
	sort.SliceStable(out, func(i, j int) bool { return less(out[i], out[j]) < 0 })
	return out
}

type sortSpec struct {
	name   string
	fields []pagination.SortField
	less   func(a, b Account) int
}

func cmp[T int64 | string](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func sortSpecs() []sortSpec {
	asc, desc := pagination.SortOrderAsc, pagination.SortOrderDesc
	return []sortSpec{
		{"id-asc", []pagination.SortField{{Field: "id", Order: asc}}, func(a, b Account) int { return cmp(a.ID, b.ID) }},
		{"id-desc", []pagination.SortField{{Field: "id", Order: desc}}, func(a, b Account) int { return cmp(b.ID, a.ID) }},
		{"balance-asc", []pagination.SortField{{Field: "balance", Order: asc}, {Field: "id", Order: asc}}, func(a, b Account) int {
			if c := cmp(a.Balance, b.Balance); c != 0 {
				return c
			}
			return cmp(a.ID, b.ID)
		}},
		{"balance-desc", []pagination.SortField{{Field: "balance", Order: desc}, {Field: "id", Order: asc}}, func(a, b Account) int {
			if c := cmp(b.Balance, a.Balance); c != 0 {
				return c
			}
			return cmp(a.ID, b.ID)
		}},
		{"owner-asc", []pagination.SortField{{Field: "owner", Order: asc}, {Field: "id", Order: asc}}, func(a, b Account) int {
			if c := cmp(a.Owner, b.Owner); c != 0 {
				return c
			}
			return cmp(a.ID, b.ID)
		}},
		{"version-desc-balance-asc", []pagination.SortField{{Field: "version", Order: desc}, {Field: "balance", Order: asc}, {Field: "id", Order: asc}}, func(a, b Account) int {
			if c := cmp(b.Version, a.Version); c != 0 {
				return c
			}
			if c := cmp(a.Balance, b.Balance); c != 0 {
				return c
			}
			return cmp(a.ID, b.ID)
		}},
	}
}

func TestPagination_ReplicatedModelPagesLikeTheWriteModel(t *testing.T) {
	labTest(t, 3, nil, func(l *lab) {
		for _, sp := range sortSpecs() {
			for _, size := range []int{1, 2, 3, 7, 10, 25, 1000} {
				l.run(fmt.Sprintf("%s/page=%d", sp.name, size), func(t *testing.T, r *rig) {
					r.w.bulkInsert(1, 60, "p")
					r.w.bulkUpdate(1, 30, 7)
					r.w.mutate(31, 45, "owner = ?", "zz-late")
					r.w.bulkRemove(50, 55)
					if err := r.w.deliverNew(longCtx(t), r.f, 40); err != nil {
						t.Fatal(err)
					}
					r.p.assertInSync(t)

					want, err := accountsOf(r.p.write.Client())
					if err != nil {
						t.Fatal(err)
					}
					var wantList []Account
					for _, a := range want {
						wantList = append(wantList, a)
					}
					wantList = sortedBy(wantList, sp.less)

					pages, err := paged(r, pagination.StructuredFilter{SortFields: sp.fields}, size)
					if err != nil {
						t.Fatal(err)
					}
					got := flatten(pages)
					if len(got) != len(wantList) {
						t.Fatalf("paged %d rows, the write database holds %d", len(got), len(wantList))
					}
					seen := map[int64]bool{}
					for i, a := range got {
						if seen[a.ID] {
							t.Fatalf("row %d appears twice", a.ID)
						}
						seen[a.ID] = true
						if a.ID != wantList[i].ID {
							t.Fatalf("position %d: id %d, want %d", i, a.ID, wantList[i].ID)
						}
					}
					for i, p := range pages {
						if i < len(pages)-1 && len(p) != size {
							t.Fatalf("page %d has %d rows, want %d", i, len(p), size)
						}
					}
				})
			}
		}
	})
}

func filterOf(mode pagination.Mode, field string, value any) pagination.StructuredFilter {
	return pagination.StructuredFilter{
		Filters:    []pagination.Filter{{Field: field, Mode: mode, Value: value}},
		SortFields: []pagination.SortField{{Field: "id", Order: pagination.SortOrderAsc}},
	}
}

func TestPagination_FiltersAgreeWithTheWriteSide(t *testing.T) {
	labTest(t, 2, nil, func(l *lab) {
		type tc struct {
			name string
			f    pagination.StructuredFilter
			keep func(a Account) bool
		}
		cases := []tc{
			{"balance>=30", filterOf(pagination.ModeGTE, "balance", 30), func(a Account) bool { return a.Balance >= 30 }},
			{"balance>30", filterOf(pagination.ModeGT, "balance", 30), func(a Account) bool { return a.Balance > 30 }},
			{"balance<=30", filterOf(pagination.ModeLTE, "balance", 30), func(a Account) bool { return a.Balance <= 30 }},
			{"balance<30", filterOf(pagination.ModeLT, "balance", 30), func(a Account) bool { return a.Balance < 30 }},
			{"balance=30", filterOf(pagination.ModeEqual, "balance", 30), func(a Account) bool { return a.Balance == 30 }},
			{"balance!=30", filterOf(pagination.ModeNotEqual, "balance", 30), func(a Account) bool { return a.Balance != 30 }},
			{"owner=p5", filterOf(pagination.ModeEqual, "owner", "p5"), func(a Account) bool { return a.Owner == "p5" }},
			{"owner starts with p1", filterOf(pagination.ModeStartsWith, "owner", "p1"), func(a Account) bool { return len(a.Owner) >= 2 && a.Owner[:2] == "p1" }},
			{"version=2", filterOf(pagination.ModeEqual, "version", 2), func(a Account) bool { return a.Version == 2 }},
			{"version>=2", filterOf(pagination.ModeGTE, "version", 2), func(a Account) bool { return a.Version >= 2 }},
			{"active=true", filterOf(pagination.ModeEqual, "active", true), func(a Account) bool { return a.Active }},
			{"balance range", filterOf(pagination.ModeRange, "balance", pagination.RangeNumber{From: 10, To: 40}), func(a Account) bool { return a.Balance >= 10 && a.Balance <= 40 }},
		}
		for _, c := range cases {
			l.run(c.name, func(t *testing.T, r *rig) {
				r.w.bulkInsert(1, 80, "p")
				r.w.bulkUpdate(1, 40, 3)
				r.w.mutate(20, 30, "active = true")
				r.w.bulkRemove(70, 80)
				if err := r.w.deliverNew(longCtx(t), r.f, 25); err != nil {
					t.Fatal(err)
				}
				all, err := accountsOf(r.p.write.Client())
				if err != nil {
					t.Fatal(err)
				}
				var want []int64
				for id, a := range all {
					if c.keep(a) {
						want = append(want, id)
					}
				}
				slices.Sort(want)
				pages, err := paged(r, c.f, 9)
				if err != nil {
					t.Fatal(err)
				}
				var got []int64
				for _, a := range flatten(pages) {
					got = append(got, a.ID)
				}
				if !slices.Equal(got, want) {
					t.Fatalf("the read side returns %v, the write side says %v", got, want)
				}
				svc := pagination.NewPaginationService(&pagination.PaginationService[Account, int64]{ReadSQLService: r.p.read, WriteSQLService: r.p.write}, nil)
				n, err := svc.Count(longCtx(t), c.f)
				if err != nil || n != int64(len(want)) {
					t.Fatalf("Count = %d, %v; want %d", n, err, len(want))
				}
				exists, err := svc.Exists(longCtx(t), c.f)
				if err != nil || exists != (len(want) > 0) {
					t.Fatalf("Exists = %v, %v", exists, err)
				}
			})
		}
	})
}

func TestPagination_AggregatesAfterReplication(t *testing.T) {
	labTest(t, 2, nil, func(l *lab) {
		l.run("max-min-by-field", func(t *testing.T, r *rig) {
			r.w.bulkInsert(1, 50, "a")
			r.w.bulkUpdate(10, 20, 100)
			r.w.bulkRemove(45, 50)
			if err := r.w.deliverNew(longCtx(t), r.f, 20); err != nil {
				t.Fatal(err)
			}
			svc := pagination.NewPaginationService(&pagination.PaginationService[Account, int64]{ReadSQLService: r.p.read, WriteSQLService: r.p.write}, nil)
			all, _ := accountsOf(r.p.write.Client())
			var maxA, minA Account
			first := true
			for _, a := range all {
				if first || a.Balance > maxA.Balance {
					maxA = a
				}
				if first || a.Balance < minA.Balance {
					minA = a
				}
				first = false
			}
			gotMax, err := svc.GetMax(longCtx(t), "balance", pagination.StructuredFilter{})
			if err != nil || gotMax.Balance != maxA.Balance {
				t.Fatalf("GetMax = %+v, %v; want balance %d", gotMax, err, maxA.Balance)
			}
			gotMin, err := svc.GetMin(longCtx(t), "balance", pagination.StructuredFilter{})
			if err != nil || gotMin.Balance != minA.Balance {
				t.Fatalf("GetMin = %+v, %v; want balance %d", gotMin, err, minA.Balance)
			}
		})
		l.run("find-one-and-find", func(t *testing.T, r *rig) {
			r.w.bulkInsert(1, 30, "a")
			r.w.remove(7)
			if err := r.w.deliverNew(longCtx(t), r.f, 20); err != nil {
				t.Fatal(err)
			}
			svc := pagination.NewPaginationService(&pagination.PaginationService[Account, int64]{ReadSQLService: r.p.read, WriteSQLService: r.p.write}, nil)
			one, err := svc.FindOne(longCtx(t), filterOf(pagination.ModeEqual, "id", 9))
			if err != nil || one == nil || one.ID != 9 {
				t.Fatalf("FindOne(9) = %+v, %v", one, err)
			}
			if gone, err := svc.FindOne(longCtx(t), filterOf(pagination.ModeEqual, "id", 7)); err == nil && gone != nil {
				t.Fatalf("a deleted row was found: %+v", gone)
			}
			many, err := svc.Find(longCtx(t), filterOf(pagination.ModeGTE, "id", 25))
			if err != nil || len(many) != 6 {
				t.Fatalf("Find returned %d rows, %v", len(many), err)
			}
		})
		l.run("empty-read-model", func(t *testing.T, r *rig) {
			svc := pagination.NewPaginationService(&pagination.PaginationService[Account, int64]{ReadSQLService: r.p.read, WriteSQLService: r.p.write}, nil)
			res, err := svc.PaginateFilter(longCtx(t), pagination.StructuredFilter{}, pagination.Pagination{PageSize: 10})
			if err != nil || len(res.Data) != 0 || res.NextCursor != nil {
				t.Fatalf("%+v %v", res, err)
			}
			if n, err := svc.Count(longCtx(t), pagination.StructuredFilter{}); err != nil || n != 0 {
				t.Fatalf("Count = %d, %v", n, err)
			}
		})
	})
}

// A page is a consistent view: pages fetched while replication is writing never
// show a row twice or skip one that existed throughout.
func TestPagination_StableWhileReplicationRuns(t *testing.T) {
	labTest(t, 2, nil, func(l *lab) {
		for _, size := range []int{5, 17, 40} {
			l.run(fmt.Sprintf("page=%d", size), func(t *testing.T, r *rig) {
				r.w.bulkInsert(1, 200, "s") // present throughout
				if err := r.w.deliverNew(longCtx(t), r.f, 50); err != nil {
					t.Fatal(err)
				}
				pm := r.w.startPump(t, r.f, 20)
				done := make(chan struct{})
				go func() {
					defer close(done)
					next := int64(1000)
					for range 15 {
						r.w.bulkInsert(next, 20, "n")
						next += 20
						r.w.bulkUpdate(1, 200, 1)
					}
				}()
				pages, err := paged(r, pagination.StructuredFilter{SortFields: []pagination.SortField{{Field: "id", Order: pagination.SortOrderAsc}}}, size)
				if err != nil {
					t.Fatal(err)
				}
				<-done
				seen := map[int64]int{}
				for _, a := range flatten(pages) {
					seen[a.ID]++
				}
				for id, n := range seen {
					if n != 1 {
						t.Fatalf("row %d was returned %d times", id, n)
					}
				}
				for id := int64(1); id <= 200; id++ {
					if seen[id] != 1 {
						t.Fatalf("row %d existed throughout but was missed", id)
					}
				}
				r.waitInSync()
				eventuallyN(t, 30e9, "the consumer", pm.caughtUp)
			})
		}
	})
}
