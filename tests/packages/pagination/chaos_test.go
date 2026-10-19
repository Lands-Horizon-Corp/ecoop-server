//go:build integration

package pagination_test

import (
	"errors"
	"math/rand"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pagination"
)

// Chaos: thousands of random filters, sorts, page sizes and cursors. The
// service may reject any of them, but it must never panic, never return more
// rows than the table holds, and never return rows alongside an error. The
// seed is printed on failure so a run can be replayed.
func TestChaos_RandomFilters(t *testing.T) {
	fx := need(t)
	seedVal := rand.Int63()
	rng := rand.New(rand.NewSource(seedVal))
	t.Logf("seed %d", seedVal)

	fields := []string{"id", "name", "email", "tier", "balance", "active", "score", "created_at", "createdAt", "updated_at", "nope", "", "name;", `"name"`}
	modes := []pagination.Mode{
		pagination.ModeEqual, pagination.ModeNotEqual, pagination.ModeContains, pagination.ModeNotContains,
		pagination.ModeStartsWith, pagination.ModeEndsWith, pagination.ModeInside, pagination.ModeOutside,
		pagination.ModeGT, pagination.ModeGTE, pagination.ModeLT, pagination.ModeLTE, pagination.ModeRange,
		pagination.ModeBefore, pagination.ModeAfter, pagination.ModeIsEmpty, pagination.ModeIsNotEmpty,
		pagination.ModeCustom, pagination.Mode("bogus"), "",
	}
	types := []pagination.DataType{"", pagination.DataTypeText, pagination.DataTypeNumber, pagination.DataTypeBool, pagination.DataTypeDate, pagination.DataTypeTime, "weird"}
	values := []any{
		nil, "", "gold", "Alice", "%", "_", "a'b", "2024-05-01", "12:30", 0, 1, -1, 100, 1e308, 3.14, true, false,
		[]any{}, []any{"gold"}, []any{1, 2}, []any{"x", 1}, map[string]any{"from": 1, "to": 5}, map[string]any{"from": "2024-01-01", "to": "2024-06-01"},
		map[string]any{}, "\x00", "\xff", "É", pagination.RangeNumber{From: 1, To: 9},
	}
	pick := func() pagination.Filter {
		return pagination.Filter{
			Field:    fields[rng.Intn(len(fields))],
			Mode:     modes[rng.Intn(len(modes))],
			Value:    values[rng.Intn(len(values))],
			DataType: types[rng.Intn(len(types))],
		}
	}
	sortables := []string{"id", "name", "tier", "balance", "score", "email", "created_at", "nope"}

	for i := range 1500 {
		sf := pagination.StructuredFilter{Logic: []pagination.Logic{pagination.LogicAnd, pagination.LogicOr, ""}[rng.Intn(3)]}
		for n := rng.Intn(4); n > 0; n-- {
			sf.Filters = append(sf.Filters, pick())
		}
		for n := rng.Intn(3); n > 0; n-- {
			sf.SortFields = append(sf.SortFields, pagination.SortField{
				Field: sortables[rng.Intn(len(sortables))],
				Order: []pagination.SortOrder{pagination.SortOrderAsc, pagination.SortOrderDesc, ""}[rng.Intn(3)],
			})
		}
		size := []int{-3, 0, 1, 2, 3, 5, 10, 50}[rng.Intn(8)]

		mustNotPanic(t, "random request", func() {
			var cursor *string
			for range 4 {
				res, err := fx.svc.Paginate(ctxT(t), pagination.Pagination{Filter: sf, PageSize: size, Cursor: cursor})
				if err != nil {
					if len(res.Data) != 0 {
						t.Fatalf("iteration %d: %d rows returned with error %v", i, len(res.Data), err)
					}
					return
				}
				if len(res.Data) > 10 {
					t.Fatalf("iteration %d: %d rows from a 10-row table", i, len(res.Data))
				}
				if cursor = res.NextCursor; cursor == nil {
					return
				}
			}
		})
		mustNotPanic(t, "random Filter/Count/Exists", func() {
			_, _ = fx.svc.Filter(ctxT(t), sf)
			_, _ = fx.svc.Count(ctxT(t), sf)
			_, _ = fx.svc.Exists(ctxT(t), sf)
			_, err := fx.svc.FindOne(ctxT(t), sf)
			_ = errors.Is(err, nil)
		})
	}
	tableIntact(t, fx)
}

// Random cursors: any bytes at all as a cursor yield an error or a page,
// never a panic.
func TestChaos_RandomCursors(t *testing.T) {
	fx := need(t)
	rng := rand.New(rand.NewSource(7))
	for range 500 {
		raw := make([]byte, rng.Intn(120))
		rng.Read(raw)
		c := string(raw)
		mustNotPanic(t, "cursor", func() {
			_, _ = fx.svc.Paginate(ctxT(t), pagination.Pagination{PageSize: 3, Cursor: &c})
		})
		payload := cursorFor(t, map[string]any{
			"v": []string{string(raw[:len(raw)/2]), string(raw[len(raw)/2:])},
			"n": []bool{rng.Intn(2) == 0, rng.Intn(2) == 0},
			"b": rng.Intn(2) == 0,
		})
		for _, sorts := range [][]pagination.SortField{nil, {asc("score")}, {desc("tier")}} {
			mustNotPanic(t, "structured cursor", func() {
				_, _ = fx.svc.Paginate(ctxT(t), pagination.Pagination{Filter: filterWithSort(and(), sorts...), PageSize: 3, Cursor: payload})
			})
		}
	}
	tableIntact(t, fx)
}
