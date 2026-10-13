//go:build integration

package pagination_test

import (
	"errors"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pagination"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// cursorFor builds a cursor by hand, the way a client (or attacker) could.
func cursorFor(t testing.TB, payload map[string]any) *string {
	t.Helper()
	s, err := utils.EncodeQueryParam(payload)
	if err != nil {
		t.Fatal(err)
	}
	return &s
}

var allNames = []string{"Alice", "Bob", "Carol", "Dave_100%", "O'Brien", emile, "Frank", "Grace", "Heidi", "Ivan"}

// Walking every page visits every row exactly once, whatever the page size
// and the sort (single, tied, mixed direction, nullable).
func TestPage_WalkVisitsEveryRowOnce(t *testing.T) {
	fx := need(t)
	sorts := map[string][]pagination.SortField{
		"default":                     nil,
		"id asc":                      {asc("id")},
		"id desc":                     {desc("id")},
		"tier asc (ties)":             {asc("tier")},
		"tier desc (ties)":            {desc("tier")},
		"balance asc (ties)":          {asc("balance")},
		"mixed tier asc balance desc": {asc("tier"), desc("balance")},
		"mixed tier desc balance asc": {desc("tier"), asc("balance")},
		"nullable score asc":          {asc("score")},
		"nullable score desc":         {desc("score")},
		"nullable then tie":           {desc("score"), asc("tier")},
		"email with NULL and empty":   {asc("email")},
	}
	for name, sorts := range sorts {
		for _, size := range []int{1, 2, 3, 4, 7, 9, 10, 11, 100} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				got := walk(t, fx, filterWithSort(and(), sorts...), size)
				if !sameSet(got, allNames) {
					t.Fatalf("size %d: walked %v, want each of %v once", size, got, allNames)
				}
			})
		}
	}
}

// The order does not depend on the page size: determinism under ties.
func TestPage_OrderIsStableAcrossPageSizes(t *testing.T) {
	fx := need(t)
	for _, sorts := range [][]pagination.SortField{
		{asc("tier")}, {desc("balance")}, {asc("tier"), desc("balance")}, {desc("score")}, nil,
	} {
		sf := filterWithSort(and(), sorts...)
		baseline := walk(t, fx, sf, 10)
		for _, size := range []int{1, 2, 3, 5} {
			if got := walk(t, fx, sf, size); !slices.Equal(got, baseline) {
				t.Fatalf("sort %+v size %d: %v != one-page order %v", sorts, size, got, baseline)
			}
		}
		for range 5 {
			if again := walk(t, fx, sf, 3); !slices.Equal(again, walk(t, fx, sf, 3)) {
				t.Fatalf("sort %+v: two identical walks disagree", sorts)
			}
		}
	}
}

// Rows that tie on the sort column are separated by id, which is appended
// as the last sort key automatically.
func TestPage_TieBreakerIsID(t *testing.T) {
	fx := need(t)
	// Alice, Carol, Émile, Grace are gold; the default id order is descending.
	got := walk(t, fx, filterWithSort(and(f("tier", pagination.ModeEqual, "gold")), asc("tier")), 1)
	want := []string{"Grace", emile, "Carol", "Alice"}
	if !slices.Equal(got, want) {
		t.Fatalf("tied rows = %v, want %v", got, want)
	}
}

// The sort really sorts.
func TestPage_SortOrders(t *testing.T) {
	fx := need(t)
	got := walk(t, fx, filterWithSort(and(), asc("balance"), asc("name")), 4)
	want := []string{"Frank", "Carol", "Ivan", "Dave_100%", "O'Brien", "Alice", "Grace", "Heidi", "Bob", emile}
	if !slices.Equal(got, want) {
		t.Fatalf("balance asc = %v, want %v", got, want)
	}
	if first := page(t, fx, filterWithSort(and(), desc("balance")), 1, nil).Data[0].Name; first != emile {
		t.Fatalf("balance desc first = %s", first)
	}
}

// Filters keep applying on every page of a cursor walk.
func TestPage_FilterHoldsAcrossPages(t *testing.T) {
	fx := need(t)
	sf := filterWithSort(and(f("balance", pagination.ModeGTE, 100)), desc("balance"))
	got := walk(t, fx, sf, 2)
	if !slices.Equal(got[:2], []string{emile, "Bob"}) || !sameSet(got, []string{"Alice", "Bob", emile, "Grace", "Heidi"}) {
		t.Fatalf("walked %v", got)
	}
}

// Forward and backward cursors on one result set line up.
func TestPage_BackwardMirrorsForward(t *testing.T) {
	fx := need(t)
	for _, sorts := range [][]pagination.SortField{{asc("id")}, {desc("balance")}, {asc("tier"), desc("balance")}, {asc("score")}} {
		sf := filterWithSort(and(), sorts...)
		p1 := page(t, fx, sf, 3, nil)
		if p1.PreviousCursor != nil {
			t.Fatalf("sort %+v: first page has a previous cursor", sorts)
		}
		p2 := page(t, fx, sf, 3, p1.NextCursor)
		p3 := page(t, fx, sf, 3, p2.NextCursor)
		if p2.PreviousCursor == nil || p3.PreviousCursor == nil {
			t.Fatalf("sort %+v: later pages lack a previous cursor", sorts)
		}
		back2 := page(t, fx, sf, 3, p3.PreviousCursor)
		if !slices.Equal(nameList(back2.Data), nameList(p2.Data)) {
			t.Fatalf("sort %+v: page 3 back = %v, want page 2 = %v", sorts, nameList(back2.Data), nameList(p2.Data))
		}
		back1 := page(t, fx, sf, 3, back2.PreviousCursor)
		if !slices.Equal(nameList(back1.Data), nameList(p1.Data)) {
			t.Fatalf("sort %+v: page 2 back = %v, want page 1 = %v", sorts, nameList(back1.Data), nameList(p1.Data))
		}
		if back1.PreviousCursor != nil {
			t.Fatalf("sort %+v: stepping back to the start still offers a previous cursor", sorts)
		}
	}
}

// The last page has no NextCursor; a page exactly full does not invent one.
func TestPage_Edges(t *testing.T) {
	fx := need(t)
	t.Run("exactly one full page", func(t *testing.T) {
		t.Parallel()
		if res := page(t, fx, and(), 10, nil); len(res.Data) != 10 || res.NextCursor != nil {
			t.Fatalf("got %d rows, next %v", len(res.Data), res.NextCursor)
		}
	})
	t.Run("one row over", func(t *testing.T) {
		t.Parallel()
		res := page(t, fx, and(), 9, nil)
		if len(res.Data) != 9 || res.NextCursor == nil {
			t.Fatalf("got %d rows, next %v", len(res.Data), res.NextCursor)
		}
		if last := page(t, fx, and(), 9, res.NextCursor); len(last.Data) != 1 || last.NextCursor != nil {
			t.Fatalf("last page = %d rows, next %v", len(last.Data), last.NextCursor)
		}
	})
	t.Run("page size of one", func(t *testing.T) {
		t.Parallel()
		if res := page(t, fx, and(), 1, nil); len(res.Data) != 1 || res.NextCursor == nil {
			t.Fatalf("got %d rows, next %v", len(res.Data), res.NextCursor)
		}
	})
	t.Run("current cursor is echoed", func(t *testing.T) {
		t.Parallel()
		p1 := page(t, fx, and(), 3, nil)
		if p2 := page(t, fx, and(), 3, p1.NextCursor); p2.CurrentCursor == nil || *p2.CurrentCursor != *p1.NextCursor {
			t.Fatalf("CurrentCursor = %v, want %v", p2.CurrentCursor, p1.NextCursor)
		}
	})
}

// Pagination drift: the table changes between two requests. A keyset cursor
// must neither repeat a row already seen nor skip one not yet seen.
func TestDrift_InsertsAndDeletesBetweenPages(t *testing.T) {
	fx := ownFixture(t, seed())
	ctx := ctxT(t)
	db := fx.sql.Client()
	sf := filterWithSort(and(), asc("id"))

	p1 := page(t, fx, sf, 4, nil)
	seen := nameList(p1.Data) // Alice Bob Carol Dave_100%

	// A new row sorts before the cursor, one already seen is deleted, one
	// not yet seen is updated: none of that may shift page 2.
	now := time.Now()
	if _, err := db.NewInsert().Model(&Account{Name: "Newcomer", Tier: "gold", CreatedAt: now, UpdatedAt: now}).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.NewDelete().Model((*Account)(nil)).Where("name = ?", "Bob").Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.NewUpdate().Model((*Account)(nil)).Set("balance = ?", 999).Where("name = ?", "Frank").Exec(ctx); err != nil {
		t.Fatal(err)
	}

	var rest []string
	for cursor := p1.NextCursor; cursor != nil; {
		res := page(t, fx, sf, 4, cursor)
		rest = append(rest, nameList(res.Data)...)
		cursor = res.NextCursor
	}
	for _, n := range rest {
		if slices.Contains(seen, n) {
			t.Fatalf("%s was served twice (%v then %v)", n, seen, rest)
		}
	}
	want := []string{"O'Brien", emile, "Frank", "Grace", "Heidi", "Ivan", "Newcomer"}
	if !slices.Equal(rest, want) {
		t.Fatalf("remaining pages = %v, want %v (nothing skipped)", rest, want)
	}
}

// A page cursor keeps working after its anchor row is deleted: it is a
// position in the sort order, not a row reference.
func TestDrift_CursorSurvivesItsRowBeingDeleted(t *testing.T) {
	fx := ownFixture(t, seed())
	sf := filterWithSort(and(), asc("id"))
	p1 := page(t, fx, sf, 3, nil)
	if _, err := fx.sql.Client().NewDelete().Model((*Account)(nil)).Where("name = ?", "Carol").Exec(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	p2 := page(t, fx, sf, 3, p1.NextCursor)
	if got := nameList(p2.Data); !slices.Equal(got, []string{"Dave_100%", "O'Brien", emile}) {
		t.Fatalf("page 2 = %v", got)
	}
}

// Rows added during a walk with the default sort (newest first) appear
// before the cursor and are not shown on later pages: no duplicates.
func TestDrift_NewestFirstWalkSeesNoDuplicates(t *testing.T) {
	fx := ownFixture(t, seed())
	ctx := ctxT(t)
	var cursor *string
	var all []string
	for i := 0; ; i++ {
		res := page(t, fx, and(), 3, cursor)
		all = append(all, nameList(res.Data)...)
		if i == 0 {
			now := time.Now()
			for j := range 5 {
				row := &Account{Name: "Late" + string(rune('A'+j)), Tier: "gold", CreatedAt: now, UpdatedAt: now.Add(time.Duration(j) * time.Second)}
				if _, err := fx.sql.Client().NewInsert().Model(row).Exec(ctx); err != nil {
					t.Fatal(err)
				}
			}
		}
		if cursor = res.NextCursor; cursor == nil {
			break
		}
	}
	seen := map[string]bool{}
	for _, n := range all {
		if seen[n] {
			t.Fatalf("%s served twice in %v", n, all)
		}
		seen[n] = true
	}
	for _, n := range allNames {
		if !seen[n] {
			t.Fatalf("%s was skipped in %v", n, all)
		}
	}
}

// Resource exhaustion: nothing a client sends in pageSize may overflow,
// panic or allocate by the requested size.
func TestLimit_PageSizeAbuse(t *testing.T) {
	fx := need(t)
	for _, size := range []int{0, -1, math.MinInt, 1, 10, 11, 1000, 1 << 20, 1 << 40, math.MaxInt32, math.MaxInt - 2, math.MaxInt - 1, math.MaxInt} {
		mustNotPanic(t, "page size", func() {
			res, err := fx.svc.Paginate(ctxT(t), pagination.Pagination{PageSize: size})
			if err != nil {
				t.Fatalf("size %d: %v", size, err)
			}
			if len(res.Data) > 10 {
				t.Fatalf("size %d: %d rows from a 10-row table", size, len(res.Data))
			}
			if size > 0 && size < 10 && len(res.Data) != size {
				t.Fatalf("size %d: got %d rows", size, len(res.Data))
			}
			if res.PageSize <= 0 {
				t.Fatalf("size %d: reported page size %d", size, res.PageSize)
			}
		})
	}
}

// Cursor tampering. A cursor is client-controlled input; whatever is put in
// it must produce a clean error or an in-scope page, never a crash, a leak
// across the hardcoded scope, or SQL.
func TestCursor_Tampering(t *testing.T) {
	fx := need(t)
	sf := filterWithSort(and(), asc("id"))
	run := func(cursor string) (pagination.PaginationResult[Account], error) {
		return fx.svc.Paginate(ctxT(t), pagination.Pagination{Filter: sf, PageSize: 3, Cursor: &cursor})
	}
	t.Run("garbage is rejected", func(t *testing.T) {
		t.Parallel()
		for _, c := range []string{"garbage", "%%%", "====", "e30", "bm90LWpzb24=", "W10%3D", " ", strings.Repeat("A", 1<<16)} {
			mustNotPanic(t, c[:min(len(c), 12)], func() {
				if _, err := run(c); !errors.Is(err, pagination.ErrInvalidCursor) {
					t.Fatalf("cursor %.20q: err = %v, want ErrInvalidCursor", c, err)
				}
			})
		}
	})
	t.Run("valid base64 JSON of the wrong shape", func(t *testing.T) {
		t.Parallel()
		for name, payload := range map[string]map[string]any{
			"no values":       {},
			"too few values":  {"v": []string{}},
			"too many values": {"v": []string{"1", "2", "3"}},
			"values not text": {"v": []int{1}},
			"values null":     {"v": nil},
			"wrong type":      {"v": "1"},
			"nulls not bools": {"v": []string{"1"}, "n": []string{"x"}},
		} {
			_, err := run(*cursorFor(t, payload))
			if !errors.Is(err, pagination.ErrInvalidCursor) {
				t.Fatalf("%s: err = %v, want ErrInvalidCursor", name, err)
			}
		}
	})
	t.Run("a NUL byte or bad UTF-8 in a value", func(t *testing.T) {
		t.Parallel()
		for _, v := range []string{"1\x00", "\xff"} {
			_, err := fx.svc.Paginate(ctxT(t), pagination.Pagination{Filter: sf, PageSize: 3, Cursor: cursorFor(t, map[string]any{"v": []string{v, "1"}})})
			if err == nil {
				t.Fatalf("value %q accepted", v)
			}
		}
	})
	t.Run("a value of the wrong type fails cleanly", func(t *testing.T) {
		t.Parallel()
		mustNotPanic(t, "wrong type", func() {
			_, err := run(*cursorFor(t, map[string]any{"v": []string{"not-a-number"}}))
			if err == nil {
				t.Fatal("expected a database error for a non-numeric id cursor")
			}
		})
	})
	t.Run("SQL in a value is data", func(t *testing.T) {
		t.Parallel()
		for _, payload := range injections {
			mustNotPanic(t, payload, func() {
				_, _ = fx.svc.Paginate(ctxT(t), pagination.Pagination{
					Filter: filterWithSort(and(), asc("name")), PageSize: 3,
					Cursor: cursorFor(t, map[string]any{"v": []string{payload, "1"}}),
				})
			})
		}
		tableIntact(t, fx)
	})
	t.Run("a null mask shorter or longer than the values", func(t *testing.T) {
		t.Parallel()
		mustNotPanic(t, "null mask", func() {
			for _, n := range [][]bool{nil, {}, {true}, {false, false, false, false, true}} {
				_, _ = fx.svc.Paginate(ctxT(t), pagination.Pagination{
					Filter: filterWithSort(and(), asc("score")), PageSize: 3,
					Cursor: cursorFor(t, map[string]any{"v": []string{"10", "1"}, "n": n}),
				})
			}
		})
	})
	t.Run("a forward cursor flipped to backward and the reverse", func(t *testing.T) {
		t.Parallel()
		p1 := page(t, fx, sf, 3, nil)
		p2 := page(t, fx, sf, 3, p1.NextCursor)
		raw, err := utils.DecodeQueryParam[map[string]any](*p2.PreviousCursor)
		if err != nil {
			t.Fatal(err)
		}
		raw["b"] = false
		flipped := cursorFor(t, raw)
		res, err := fx.svc.Paginate(ctxT(t), pagination.Pagination{Filter: sf, PageSize: 3, Cursor: flipped})
		if err != nil {
			t.Fatalf("flipped cursor: %v", err)
		}
		if len(res.Data) == 0 {
			t.Fatal("flipped cursor returned nothing")
		}
		for _, r := range res.Data {
			if slices.Contains(nameList(p1.Data), r.Name) {
				t.Fatalf("flipped cursor went back into page 1: %s", r.Name)
			}
		}
	})
	t.Run("a cursor from another sort is refused when it cannot fit", func(t *testing.T) {
		t.Parallel()
		two := page(t, fx, filterWithSort(and(), asc("tier"), asc("balance")), 3, nil)
		if two.NextCursor == nil {
			t.Fatal("setup: no next cursor")
		}
		_, err := fx.svc.Paginate(ctxT(t), pagination.Pagination{Filter: sf, PageSize: 3, Cursor: two.NextCursor})
		if !errors.Is(err, pagination.ErrInvalidCursor) {
			t.Fatalf("err = %v, want ErrInvalidCursor", err)
		}
	})
}

// A cursor never widens the hardcoded scope: forged values may move the
// position but rows outside the scope stay out of reach.
func TestCursor_CannotLeaveScope(t *testing.T) {
	fx := need(t)
	scope := and(f("tier", pagination.ModeEqual, "gold"))
	sf := filterWithSort(and(), asc("id"))
	for _, v := range [][]string{{"0"}, {"5"}, {"1000000"}, {"-1"}} {
		for _, backward := range []bool{false, true} {
			res, err := fx.svc.PaginateFilter(ctxT(t), scope, pagination.Pagination{
				Filter: sf, PageSize: 20,
				Cursor: cursorFor(t, map[string]any{"v": v, "b": backward}),
			})
			if err != nil {
				t.Fatalf("cursor %v backward=%v: %v", v, backward, err)
			}
			for _, r := range res.Data {
				if r.Tier != "gold" {
					t.Fatalf("cursor %v backward=%v leaked %s (%s)", v, backward, r.Name, r.Tier)
				}
			}
		}
	}
	// A cursor taken under one filter, replayed under another, still obeys
	// the new filter.
	p1 := page(t, fx, filterWithSort(and(), asc("id")), 3, nil)
	res := page(t, fx, filterWithSort(and(f("tier", pagination.ModeEqual, "bronze")), asc("id")), 20, p1.NextCursor)
	for _, r := range res.Data {
		if r.Tier != "bronze" {
			t.Fatalf("replayed cursor leaked %s (%s) past the new filter", r.Name, r.Tier)
		}
	}
}

// Cursors carry no expiry: an old one keeps working as long as the sort it
// was issued for does. (If expiry is added, this test is where to change it.)
func TestCursor_NoExpiry(t *testing.T) {
	fx := need(t)
	p1 := page(t, fx, and(), 3, nil)
	time.Sleep(50 * time.Millisecond)
	if p2 := page(t, fx, and(), 3, p1.NextCursor); len(p2.Data) != 3 {
		t.Fatalf("old cursor returned %d rows", len(p2.Data))
	}
}
