//go:build integration

package pagination_test

import (
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pagination"
)

// Smoke: build the service, filter, page, count, stop. If this fails, nothing
// else is worth reading.
func TestSmoke(t *testing.T) {
	fx := need(t)
	ctx := ctxT(t)
	res, err := fx.svc.Paginate(ctx, pagination.Pagination{PageSize: 4})
	if err != nil || len(res.Data) != 4 || res.NextCursor == nil {
		t.Fatalf("Paginate = %d rows, next %v, %v", len(res.Data), res.NextCursor, err)
	}
	if n, err := fx.svc.Count(ctx, and(f("tier", pagination.ModeEqual, "gold"))); err != nil || n != 4 {
		t.Fatalf("Count = %d, %v", n, err)
	}
	if ok, err := fx.svc.Exists(ctx, and(f("name", pagination.ModeEqual, "Alice"))); err != nil || !ok {
		t.Fatalf("Exists = %v, %v", ok, err)
	}
}
