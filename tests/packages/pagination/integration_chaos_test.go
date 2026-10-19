//go:build integration

package pagination_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pagination"
)

// sleepFilter makes the database sleep, standing in for a slow query.
func sleepFilter(seconds int) pagination.Filter {
	return pagination.Filter{Mode: pagination.ModeCustom, Custom: func(q *bun.SelectQuery, _ any) (*bun.SelectQuery, error) {
		return q.Where("pg_sleep(?) IS NOT NULL", seconds), nil
	}}
}

// killConnections terminates every server connection to the fixture's
// database, as a failover or a proxy reset would.
func killConnections(t testing.TB, fx *fixture) {
	t.Helper()
	var name string
	if err := fx.sql.Client().NewRaw("SELECT current_database()").Scan(ctxT(t), &name); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctxT(t), "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1", name); err != nil {
		t.Fatal(err)
	}
}

func runningSleeps(t testing.TB, fx *fixture) int {
	t.Helper()
	var n int
	err := fx.sql.Client().NewRaw(
		"SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND state = 'active' AND query LIKE '%pg\\_sleep%'",
	).Scan(ctxT(t), &n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// A client that gives up must not leave its query running on the server:
// no orphaned queries burning CPU for nobody.
func TestChaos_ContextCancellationStopsTheQuery(t *testing.T) {
	fx := ownFixture(t, seed())
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := fx.svc.Filter(ctx, and(sleepFilter(30)))
	if err == nil {
		t.Fatal("a 30s query under a 300ms deadline returned no error")
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("returned after %v, the deadline was not honored", took)
	}
	deadline := time.Now().Add(5 * time.Second)
	for runningSleeps(t, fx) > 0 {
		if time.Now().After(deadline) {
			t.Fatal("the cancelled query is still running on the server")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Cancelling mid-walk, many times at once, leaks no goroutines and leaves
// the service usable.
func TestChaos_CancelStormLeavesServiceHealthy(t *testing.T) {
	fx := ownFixture(t, seed())
	var wg sync.WaitGroup
	for i := range 40 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(i%5)*time.Millisecond)
			defer cancel()
			_, _ = fx.svc.Paginate(ctx, pagination.Pagination{PageSize: 3, Filter: filterWithSort(and(), asc("tier"))})
			_, _ = fx.svc.Filter(ctx, and(sleepFilter(2)))
		}()
	}
	wg.Wait()
	deadline := time.Now().Add(5 * time.Second)
	for runningSleeps(t, fx) > 0 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if n := runningSleeps(t, fx); n != 0 {
		t.Fatalf("%d cancelled queries still running", n)
	}
	if got := names(t, fx, and()); len(got) != 10 {
		t.Fatalf("service unhealthy after the storm: %v", got)
	}
}

// Pool starvation: with every connection held by someone else, a request
// waits only as long as its own deadline, then fails; it never hangs, and
// the service recovers when connections come back.
func TestChaos_PoolStarvation(t *testing.T) {
	fx := ownFixture(t, seed())
	db := fx.sql.Client()
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)
	ctx := ctxT(t)

	var held []*sql.Conn
	for range 2 {
		c, err := db.DB.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, c)
	}
	short, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := fx.svc.Filter(short, and()); err == nil {
		t.Fatal("expected an error while the pool is exhausted")
	} else if !errors.Is(err, context.DeadlineExceeded) {
		t.Logf("starved request failed with: %v", err)
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("starved request hung for %v", took)
	}
	for _, c := range held {
		_ = c.Close()
	}
	if got := names(t, fx, and()); len(got) != 10 {
		t.Fatalf("after the pool recovered: %v", got)
	}
}

// Many more concurrent requests than connections all complete.
func TestChaos_MoreRequestsThanConnections(t *testing.T) {
	fx := ownFixture(t, seed())
	fx.sql.Client().SetMaxOpenConns(3)
	var wg sync.WaitGroup
	errs := make(chan error, 200)
	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := fx.svc.Filter(ctxT(t), and(f("tier", pagination.ModeEqual, "gold")))
			if err != nil || len(got) != 4 {
				errs <- fmt.Errorf("%w, %d rows", err, len(got))
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// Connections killed by the server (failover, proxy reset) are recovered
// from: the service answers again within a few attempts.
func TestChaos_ConnectionsKilledUnderneath(t *testing.T) {
	fx := ownFixture(t, seed())
	ctx := ctxT(t)
	if got := names(t, fx, and()); len(got) != 10 {
		t.Fatalf("warm-up: %v", got)
	}
	killConnections(t, fx)
	var last error
	for range 5 {
		got, err := fx.svc.Filter(ctx, and())
		if err == nil && len(got) == 10 {
			return
		}
		last = err
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("never recovered after the connections were killed: %v", last)
}

// A walk interrupted halfway by a connection kill can be resumed with the
// cursor it already holds.
func TestChaos_CursorResumesAfterOutage(t *testing.T) {
	fx := ownFixture(t, seed())
	ctx := ctxT(t)
	sf := filterWithSort(and(), asc("id"))
	p1 := page(t, fx, sf, 4, nil)
	killConnections(t, fx)
	var p2 pagination.PaginationResult[Account]
	var err error
	for range 5 {
		if p2, err = fx.svc.Paginate(ctx, pagination.Pagination{Filter: sf, PageSize: 4, Cursor: p1.NextCursor}); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil || len(p2.Data) != 4 || p2.Data[0].Name != "O'Brien" {
		t.Fatalf("resume = %v, %v", nameList(p2.Data), err)
	}
}

// Rolling deployment: the code runs against a schema that is one step
// behind or ahead. Both fail or work predictably, never panic.
func TestChaos_SchemaDrift(t *testing.T) {
	t.Run("database has a column the struct does not know (new schema, old code)", func(t *testing.T) {
		fx := ownFixture(t, seed())
		if _, err := fx.sql.Client().ExecContext(ctxT(t), "ALTER TABLE pg_accounts ADD COLUMN nickname text DEFAULT 'x'"); err != nil {
			t.Fatal(err)
		}
		wantSet(t, names(t, fx, and(f("tier", pagination.ModeEqual, "silver"))), "Bob", "O'Brien", "Heidi")
		if got := walk(t, fx, and(), 3); len(got) != 10 {
			t.Fatalf("walk = %v", got)
		}
	})
	t.Run("the struct has a column the database lacks (old schema, new code)", func(t *testing.T) {
		fx := ownFixture(t, seed())
		if _, err := fx.sql.Client().ExecContext(ctxT(t), "ALTER TABLE pg_accounts DROP COLUMN score"); err != nil {
			t.Fatal(err)
		}
		mustNotPanic(t, "missing column", func() {
			if _, err := fx.svc.Filter(ctxT(t), and()); err == nil {
				t.Error("selecting a dropped column returned no error")
			}
			if _, err := fx.svc.Paginate(ctxT(t), pagination.Pagination{PageSize: 3}); err == nil {
				t.Error("Paginate: no error for a dropped column")
			}
		})
	})
	t.Run("a column changed type under a filter", func(t *testing.T) {
		fx := ownFixture(t, seed())
		if _, err := fx.sql.Client().ExecContext(ctxT(t), "ALTER TABLE pg_accounts ALTER COLUMN tier TYPE int USING 0"); err != nil {
			t.Fatal(err)
		}
		mustNotPanic(t, "type change", func() {
			if _, err := fx.svc.Filter(ctxT(t), and(f("tier", pagination.ModeEqual, "gold"))); err == nil {
				t.Error("no error comparing an int column to text")
			}
		})
	})
	t.Run("table missing", func(t *testing.T) {
		fx := ownFixture(t, seed())
		if _, err := fx.sql.Client().ExecContext(ctxT(t), "DROP TABLE pg_accounts"); err != nil {
			t.Fatal(err)
		}
		mustNotPanic(t, "no table", func() {
			if _, err := fx.svc.Filter(ctxT(t), and()); err == nil {
				t.Error("no error for a missing table")
			}
		})
	})
}

// Index miss / full scan blowout. A big table is filtered and walked: the
// keyset cursor must stay cheap at depth (no OFFSET), and an index must be
// usable by the cursor predicate.
func TestProd_LargeTable(t *testing.T) {
	if testing.Short() {
		t.Skip("large table")
	}
	fx := ownFixture(t, nil)
	ctx := ctxT(t)
	db := fx.sql.Client()
	const rows = 50_000
	_, err := db.ExecContext(ctx, `
		INSERT INTO pg_accounts (name, email, tier, balance, active, score, created_at, updated_at)
		SELECT 'user' || g, 'u' || g || '@x.com', (ARRAY['gold','silver','bronze'])[1 + g % 3], g % 1000, g % 2 = 0,
		       CASE WHEN g % 10 = 0 THEN NULL ELSE g % 100 END,
		       timestamp '2024-01-01' + (g % 5000) * interval '1 minute',
		       timestamp '2024-01-01' + (g % 5000) * interval '1 minute'
		FROM generate_series(1, ?) g`, rows)
	if err != nil {
		t.Fatal(err)
	}
	// The library orders with NULLS LAST, so an index must say so too: a plain
	// (updated_at DESC, id DESC) index sorts NULLS FIRST, is ignored, and every
	// page becomes a full scan plus sort.
	if _, err := db.ExecContext(ctx, "CREATE INDEX pg_accounts_cursor ON pg_accounts (updated_at DESC NULLS LAST, id DESC NULLS LAST); ANALYZE pg_accounts"); err != nil {
		t.Fatal(err)
	}

	t.Run("deep page costs about the same as the first", func(t *testing.T) {
		sf := and()
		first := time.Now()
		p := page(t, fx, sf, 100, nil)
		firstTook := time.Since(first)
		cursor := p.NextCursor
		for i := 0; i < 400 && cursor != nil; i++ { // 40,000 rows deep
			p = page(t, fx, sf, 100, cursor)
			cursor = p.NextCursor
		}
		deep := time.Now()
		page(t, fx, sf, 100, cursor)
		if deepTook := time.Since(deep); deepTook > 20*firstTook+250*time.Millisecond {
			t.Fatalf("page 400 took %v vs %v for page 1: cost grows with depth", deepTook, firstTook)
		}
	})
	t.Run("every row is reachable exactly once", func(t *testing.T) {
		seen := map[string]bool{}
		var cursor *string
		for {
			p := page(t, fx, and(f("tier", pagination.ModeEqual, "gold")), 1000, cursor)
			for _, r := range p.Data {
				if seen[r.Name] {
					t.Fatalf("%s served twice", r.Name)
				}
				seen[r.Name] = true
			}
			if cursor = p.NextCursor; cursor == nil {
				break
			}
		}
		if want := rows / 3; len(seen) < want || len(seen) > want+1 {
			t.Fatalf("saw %d gold rows, want about %d", len(seen), want)
		}
	})
	t.Run("the cursor predicate can use the index", func(t *testing.T) {
		var plan []string
		err := db.NewRaw(`EXPLAIN SELECT * FROM pg_accounts
			WHERE ("updated_at", "id") < ('2024-01-02 00:00:00', 40000)
			ORDER BY "updated_at" DESC NULLS LAST, "id" DESC NULLS LAST LIMIT 101`).Scan(ctx, &plan)
		if err != nil {
			t.Fatal(err)
		}
		if joined := strings.Join(plan, "\n"); !strings.Contains(joined, "Index") {
			t.Fatalf("keyset predicate does not use the (updated_at, id) index:\n%s", joined)
		}
	})
	t.Run("the SQL the library really emits is index-friendly and OFFSET-free", func(t *testing.T) {
		rec := &sqlRecorder{}
		// WithQueryHook returns a copy; the library under test holds this *bun.DB, so
		// the hook has to go onto it.
		//lint:ignore SA1019 see above
		db.AddQueryHook(rec) //nolint:staticcheck // see above
		sf := and()
		p := page(t, fx, sf, 100, nil)
		page(t, fx, sf, 100, p.NextCursor)
		var selects []string
		for _, q := range rec.all() {
			if strings.HasPrefix(q, "SELECT") && strings.Contains(q, "pg_accounts") {
				selects = append(selects, q)
			}
		}
		if len(selects) < 2 {
			t.Fatalf("captured %d selects, want the two pages", len(selects))
		}
		for _, q := range selects {
			if strings.Contains(strings.ToUpper(q), "OFFSET") {
				t.Fatalf("page query uses OFFSET: %s", q)
			}
			if !strings.Contains(q, "LIMIT 101") {
				t.Fatalf("page query is not bounded by pageSize+1: %s", q)
			}
		}
		var plan []string
		if err := db.NewRaw("EXPLAIN "+selects[1]).Scan(ctx, &plan); err != nil {
			t.Fatalf("EXPLAIN: %v\n%s", err, selects[1])
		}
		if joined := strings.Join(plan, "\n"); !strings.Contains(joined, "Index") {
			t.Fatalf("the real cursor query does not use an index:\n%s\n%s", selects[1], joined)
		}
	})
	t.Run("a leading-wildcard search still answers in bounded time", func(t *testing.T) {
		start := time.Now()
		got, err := fx.svc.Filter(ctx, and(f("name", pagination.ModeContains, "user4999")))
		if err != nil || len(got) == 0 {
			t.Fatalf("got %d rows, %v", len(got), err)
		}
		if took := time.Since(start); took > 5*time.Second {
			t.Fatalf("a LIKE '%%x%%' scan of %d rows took %v", rows, took)
		}
	})
	t.Run("a filter matching everything is capped by the page size", func(t *testing.T) {
		if p := page(t, fx, and(), 100, nil); len(p.Data) != 100 {
			t.Fatalf("got %d rows for a page of 100", len(p.Data))
		}
	})
}

// Scraping: a bot following NextCursor as fast as it can gets pages in a
// bounded number of requests, each bounded in size, ending in a nil cursor.
// The service has no rate limit of its own (that belongs to the HTTP layer);
// this checks that it gives a scraper no shortcut around the page size.
func TestProd_ScraperCannotBypassPageSize(t *testing.T) {
	fx := need(t)
	var cursor *string
	requests := 0
	for {
		p := page(t, fx, and(), 2, cursor)
		requests++
		if len(p.Data) > 2 {
			t.Fatalf("a page of 2 returned %d rows", len(p.Data))
		}
		if cursor = p.NextCursor; cursor == nil {
			break
		}
		if requests > 100 {
			t.Fatal("cursor chain does not end")
		}
	}
	if requests != 5 {
		t.Fatalf("took %d requests for 10 rows at 2 per page, want 5", requests)
	}
}

// sqlRecorder keeps every statement bun sends.
type sqlRecorder struct {
	mu sync.Mutex
	qs []string
}

func (r *sqlRecorder) BeforeQuery(ctx context.Context, _ *bun.QueryEvent) context.Context { return ctx }
func (r *sqlRecorder) AfterQuery(_ context.Context, e *bun.QueryEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.qs = append(r.qs, e.Query)
}
func (r *sqlRecorder) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.qs...)
}
