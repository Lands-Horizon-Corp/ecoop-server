package regressions

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/pagination"
	"github.com/Lands-Horizon-Corp/ecoop-server/utils"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/uptrace/bun"
)

// 17 Pagination features on the production read side (Postgres 16 with pg_search and pg_partman,
// `make test-cdc`): BM25 search, partitioned tables, Hertz request parsing, preloads, range and
// time-zone filters, cursor stability under concurrent writes, forged cursors, and keyset
// pagination over a million rows using an index rather than a sort.

type bdEvent struct {
	bun.BaseModel `bun:"table:bank_events"`
	ID            string    `bun:"id,pk"`
	OccurredAt    time.Time `bun:"occurred_at,pk,notnull"`
	Amount        int64     `bun:"amount,notnull"`
	UpdatedAt     time.Time `bun:"updated_at,nullzero,notnull,default:current_timestamp"`
}

type bdTransferRel struct {
	bun.BaseModel `bun:"table:bank_transfers"`
	ID            string     `bun:"id,pk"`
	FromAccount   string     `bun:"from_account"`
	ToAccount     string     `bun:"to_account"`
	Amount        int64      `bun:"amount"`
	UpdatedAt     time.Time  `bun:"updated_at"`
	From          *bdAccount `bun:"rel:belongs-to,join:from_account=id"`
	To            *bdAccount `bun:"rel:belongs-to,join:to_account=id"`
}

// extBank is a bank on the Postgres 16 pair with the read-side extensions installed in its reader.
func extBank(t *testing.T) *bdLedger {
	t.Helper()
	requireReachableHint(t, hostOf(envOr("CDC_READ_DSN", defaultCDCReadDSN)), "make test-up-cdc")
	b := newBDBank(t, bdOpts{target: "pg16", noRun: true})
	for _, stmt := range []string{
		`CREATE EXTENSION IF NOT EXISTS vector`,
		`CREATE EXTENSION IF NOT EXISTS pg_search`,
		`CREATE SCHEMA IF NOT EXISTS partman`,
		`CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman`,
	} {
		_, err := b.h.reader.Exec(stmt)
		must(t, err)
	}
	return b
}

func readerPager[T any](b *bdLedger) *pagination.PaginationService[T, string] {
	return pagination.NewPaginationService(pagination.PaginationService[T, string]{
		ReadSQLService: b.svc.Reader(), WriteSQLService: b.svc.Writer(),
	}).(*pagination.PaginationService[T, string])
}

func seedReaderAccounts(t *testing.T, b *bdLedger, rows ...bdAccount) {
	t.Helper()
	for _, a := range rows {
		_, err := b.h.reader.Exec(`INSERT INTO bank_accounts (id, owner, balance, updated_at) VALUES ($1, $2, $3, COALESCE($4, now()))`,
			a.ID, a.Owner, a.Balance, nullTime(a.UpdatedAt))
		must(t, err)
	}
}

func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

func TestBankDBPaginationExt_BM25SearchFindsByWordsAndWholeIndex(t *testing.T) {
	b := extBank(t)
	seedReaderAccounts(t, b,
		bdAccount{ID: "a1", Owner: "Ann Cruz", Balance: 1}, bdAccount{ID: "a2", Owner: "Ben Cruz", Balance: 2},
		bdAccount{ID: "a3", Owner: "Cy Santos", Balance: 3})
	ctx := withDeadline(t, 20*time.Second)
	must(t, readerPager[bdAccount](b).EnableSearchIndex(ctx, "owner"))

	for name, f := range map[string]pagination.Filter{
		"by column":       {Field: "owner", Mode: pagination.ModeSearch, Value: "cruz"},
		"across an index": {Mode: pagination.ModeSearch, Value: "owner:cruz"},
	} {
		got, err := b.accounts.Find(ctx, pagination.StructuredFilter{Filters: []pagination.Filter{f},
			SortFields: []pagination.SortField{{Field: "id", Order: pagination.SortOrderAsc}}})
		if err != nil || fmt.Sprint(accountIDs(got)) != "[a1 a2]" {
			t.Errorf("%s: search cruz = %v, %v; want [a1 a2]", name, accountIDs(got), err)
		}
	}
}

func TestBankDBPaginationExt_PartitionedTableAcrossPartitions(t *testing.T) {
	b := extBank(t)
	ctx := withDeadline(t, 30*time.Second)
	pager := readerPager[bdEvent](b)
	must(t, pager.EnablePartitioning(ctx, "occurred_at", "1 day"))
	if n := count(t, b.h.reader, `SELECT count(*) FROM pg_inherits WHERE inhparent = 'bank_events'::regclass`); n < 2 {
		t.Fatalf("bank_events has %d partitions; pg_partman did not create them", n)
	}
	base := time.Now().UTC().Truncate(time.Hour)
	for i := range 48 {
		_, err := b.h.reader.Exec(`INSERT INTO bank_events (id, occurred_at, amount) VALUES ($1, $2, $3)`,
			fmt.Sprintf("e%02d", i), base.Add(time.Duration(i-24)*time.Hour), i)
		must(t, err)
	}
	if n, err := pager.Count(ctx, pagination.StructuredFilter{}); err != nil || n != 48 {
		t.Fatalf("Count across partitions = %d, %v; want 48", n, err)
	}
	lastDay, err := pager.Find(ctx, pagination.StructuredFilter{Filters: []pagination.Filter{
		{Field: "occurred_at", Mode: pagination.ModeGTE, DataType: pagination.DataTypeDate, Value: base.Format(time.RFC3339)},
	}})
	if err != nil || len(lastDay) != 24 {
		t.Fatalf("events in the newest day = %d, %v; want 24", len(lastDay), err)
	}
	if spread := count(t, b.h.reader, `SELECT count(DISTINCT tableoid) FROM bank_events`); spread < 2 {
		t.Fatalf("all rows landed in %d partition(s)", spread)
	}
}

func TestBankDBPaginationExt_HertzRequestParsing(t *testing.T) {
	b := extBank(t)
	for i := range 5 {
		seedReaderAccounts(t, b, bdAccount{ID: fmt.Sprintf("h%d", i), Owner: fmt.Sprintf("owner %d", i), Balance: int64(i * 100)})
	}
	filter, err := utils.EncodeQueryParam(pagination.StructuredFilter{Filters: []pagination.Filter{
		{Field: "balance", Mode: pagination.ModeGTE, Value: 100, DataType: pagination.DataTypeNumber}}})
	must(t, err)
	sort, err := utils.EncodeQueryParam([]pagination.SortField{{Field: "balance", Order: "DESC"}})
	must(t, err)
	req := app.NewContext(0)
	req.Request.SetRequestURI(fmt.Sprintf("/accounts?pageSize=2&filter=%s&sort=%s", filter, sort))

	page, err := b.accounts.PaginateWithHertz(withDeadline(t, 5*time.Second), nil, pagination.StructuredFilter{}, req)
	if err != nil || fmt.Sprint(accountIDs(page.Data)) != "[h4 h3]" || page.NextCursor == nil {
		t.Fatalf("Hertz page = %v (next %v), %v; want [h4 h3] with a next cursor", accountIDs(page.Data), page.NextCursor, err)
	}
	bad := app.NewContext(0)
	bad.Request.SetRequestURI("/accounts?filter=not-base64!!")
	if _, err := b.accounts.PaginateWithHertz(withDeadline(t, 5*time.Second), nil, pagination.StructuredFilter{}, bad); err == nil {
		t.Fatal("a malformed filter parameter was accepted")
	}
}

func TestBankDBPaginationExt_PreloadsLoadRelationsAndDropUnknownOnes(t *testing.T) {
	b := extBank(t)
	seedReaderAccounts(t, b, bdAccount{ID: "x", Owner: "Xi", Balance: 1}, bdAccount{ID: "y", Owner: "Yu", Balance: 1})
	_, err := b.h.reader.Exec(`INSERT INTO bank_transfers (id, idempotency_key, from_account, to_account, amount, request_hash) VALUES ('t1', 'k1', 'x', 'y', 5, 'h')`)
	must(t, err)

	got, err := readerPager[bdTransferRel](b).Find(withDeadline(t, 5*time.Second), pagination.StructuredFilter{}, "From", "To", "Nope")
	if err != nil || len(got) != 1 {
		t.Fatalf("Find with preloads = %v, %v", got, err)
	}
	if got[0].From == nil || got[0].From.Owner != "Xi" || got[0].To == nil || got[0].To.Owner != "Yu" {
		t.Fatalf("relations not loaded: %+v", got[0])
	}
}

func TestBankDBPaginationExt_RangeAndTimeZoneFilters(t *testing.T) {
	b := extBank(t)
	at := time.Date(2026, 1, 1, 15, 30, 0, 0, time.UTC) // 23:30 in Manila
	seedReaderAccounts(t, b,
		bdAccount{ID: "r1", Owner: "o1", Balance: 50, UpdatedAt: at.Add(-time.Hour)},
		bdAccount{ID: "r2", Owner: "o2", Balance: 150, UpdatedAt: at},
		bdAccount{ID: "r3", Owner: "o3", Balance: 250, UpdatedAt: at.Add(time.Hour)})
	ctx := withDeadline(t, 5*time.Second)
	ids := func(f pagination.Filter) string {
		got, err := b.accounts.Find(ctx, pagination.StructuredFilter{Filters: []pagination.Filter{f},
			SortFields: []pagination.SortField{{Field: "id", Order: pagination.SortOrderAsc}}})
		must(t, err)
		return fmt.Sprint(accountIDs(got))
	}
	for name, c := range map[string]struct {
		f    pagination.Filter
		want string
	}{
		"number range":           {pagination.Filter{Field: "balance", Mode: pagination.ModeRange, Value: pagination.RangeNumber{From: 100, To: 300}}, "[r2 r3]"},
		"after, Manila offset":   {pagination.Filter{Field: "updated_at", Mode: pagination.ModeAfter, DataType: pagination.DataTypeDate, Value: "2026-01-01T23:00:00+08:00"}, "[r2 r3]"},
		"before, UTC":            {pagination.Filter{Field: "updated_at", Mode: pagination.ModeBefore, DataType: pagination.DataTypeDate, Value: "2026-01-01T15:00:00Z"}, "[r1]"},
		"date range across zone": {pagination.Filter{Field: "updated_at", Mode: pagination.ModeRange, DataType: pagination.DataTypeDate, Value: map[string]any{"from": "2026-01-01T23:00:00+08:00", "to": "2026-01-02T00:00:00+08:00"}}, "[r2]"},
	} {
		if got := ids(c.f); got != c.want {
			t.Errorf("%s = %s; want %s", name, got, c.want)
		}
	}
}

func TestBankDBPaginationExt_CursorWalkIsStableUnderConcurrentWrites(t *testing.T) {
	b := extBank(t)
	for i := range 100 {
		seedReaderAccounts(t, b, bdAccount{ID: fmt.Sprintf("s%03d", i), Owner: fmt.Sprintf("o%03d", i), Balance: int64(i)})
	}
	ctx := withDeadline(t, 30*time.Second)
	var (
		mu      sync.Mutex
		deleted = map[string]bool{}
		stop    = make(chan struct{})
		wg      sync.WaitGroup
	)
	wg.Go(func() { // concurrent traffic: new rows before and after the walk position, deletions
		r := rand.New(rand.NewPCG(7, 7))
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_, _ = b.h.reader.Exec(`INSERT INTO bank_accounts (id, owner, balance) VALUES ($1, $1, 0)`, fmt.Sprintf("r%03d", i))
			_, _ = b.h.reader.Exec(`INSERT INTO bank_accounts (id, owner, balance) VALUES ($1, $1, 0)`, fmt.Sprintf("t%03d", i))
			victim := fmt.Sprintf("s%03d", r.IntN(100))
			if _, err := b.h.reader.Exec(`DELETE FROM bank_accounts WHERE id = $1`, victim); err == nil {
				mu.Lock()
				deleted[victim] = true
				mu.Unlock()
			}
			time.Sleep(2 * time.Millisecond)
		}
	})

	seen := map[string]int{}
	page := pagination.Pagination{PageSize: 7, Filter: pagination.StructuredFilter{
		Filters:    []pagination.Filter{{Field: "id", Mode: pagination.ModeStartsWith, Value: "s"}},
		SortFields: []pagination.SortField{{Field: "id", Order: pagination.SortOrderAsc}},
	}}
	for {
		res, err := b.accounts.Paginate(ctx, page)
		must(t, err)
		for _, a := range res.Data {
			seen[a.ID]++
		}
		if res.NextCursor == nil {
			break
		}
		page.Cursor = res.NextCursor
		time.Sleep(3 * time.Millisecond)
	}
	close(stop)
	wg.Wait()

	for id, n := range seen {
		if n > 1 {
			t.Errorf("%s returned %d times", id, n)
		}
	}
	for i := range 100 {
		id := fmt.Sprintf("s%03d", i)
		if !deleted[id] && seen[id] == 0 {
			t.Errorf("%s existed throughout the walk but was skipped", id)
		}
	}
}

func TestBankDBPaginationExt_ForgedCursorCannotEscapeTheFixedFilter(t *testing.T) {
	b := extBank(t)
	for i := range 10 {
		owner := fmt.Sprintf("public-%d", i)
		if i%2 == 0 {
			owner = fmt.Sprintf("mine-%d", i)
		}
		seedReaderAccounts(t, b, bdAccount{ID: fmt.Sprintf("f%d", i), Owner: owner, Balance: int64(i)})
	}
	pager := readerPager[bdAccount](b)
	ctx := withDeadline(t, 10*time.Second)
	fixed := pagination.StructuredFilter{Filters: []pagination.Filter{ // the server-side scope, e.g. "my accounts"
		{Field: "owner", Mode: pagination.ModeStartsWith, Value: "mine-"}}}
	for name, values := range map[string][]string{
		"valid values from another listing": {time.Now().UTC().Format(time.RFC3339Nano), "f9"},
		"injection in the values":           {"2026-01-01T00:00:00Z", "f1' OR '1'='1"},
		"sql in the timestamp":              {"now()); DROP TABLE bank_accounts; --", "x"},
	} {
		raw, err := utils.EncodeQueryParam(struct {
			V []string `json:"v"`
		}{values})
		must(t, err)
		res, err := pager.PaginateFilter(ctx, fixed, pagination.Pagination{PageSize: 50, Cursor: &raw})
		if err != nil {
			continue // rejecting the cursor is acceptable
		}
		for _, a := range res.Data {
			if !strings.HasPrefix(a.Owner, "mine-") {
				t.Errorf("%s: forged cursor returned %s owned by %q", name, a.ID, a.Owner)
			}
		}
	}
	if n := count(t, b.h.reader, `SELECT count(*) FROM bank_accounts`); n != 10 {
		t.Fatalf("reader has %d accounts; a forged cursor changed data", n)
	}
}

func TestBankDBPaginationExt_KeysetPagingOverAMillionRowsUsesTheIndex(t *testing.T) {
	b := extBank(t)
	for _, stmt := range []string{
		`INSERT INTO bank_accounts (id, owner, balance, updated_at)
			SELECT 'm' || lpad(g::text, 7, '0'), 'o' || g, (g::bigint * 7919) % 1000003, now() FROM generate_series(1, 1000000) g`,
		`CREATE INDEX bank_accounts_balance_id ON bank_accounts (balance, id)`,
		`ANALYZE bank_accounts`,
	} {
		_, err := b.h.reader.Exec(stmt)
		must(t, err)
	}
	capture := &queryLog{table: "bank_accounts"}
	b.svc.Reader().Client().AddQueryHook(capture)
	ctx := withDeadline(t, 30*time.Second)
	page := pagination.Pagination{PageSize: 50, Filter: pagination.StructuredFilter{SortFields: []pagination.SortField{
		{Field: "balance", Order: pagination.SortOrderAsc}, {Field: "id", Order: pagination.SortOrderAsc}}}}
	var deep *string
	for range 20 { // walk 20 pages in
		res, err := b.accounts.Paginate(ctx, page)
		must(t, err)
		deep, page.Cursor = res.NextCursor, res.NextCursor
	}
	started := time.Now()
	page.Cursor = deep
	res, err := b.accounts.Paginate(ctx, page)
	must(t, err)
	elapsed := time.Since(started)
	if len(res.Data) != 50 {
		t.Fatalf("page has %d rows", len(res.Data))
	}
	q := capture.lastQuery()
	plan := queryLines(t, b.h.reader, `EXPLAIN `+q)
	if !strings.Contains(plan, "Index") || strings.Contains(plan, "Seq Scan") {
		t.Fatalf("a deep keyset page does not use the (balance, id) index:\n%s\nquery: %s", plan, q)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("a page 20 deep into 1M rows took %v", elapsed)
	}
	t.Logf("page 21 of 1M rows in %v", elapsed)
}

func TestBankDBPaginationExt_FiftyThousandRowBulkInsert(t *testing.T) {
	b := extBank(t)
	rows := make([]bdAccount, 50_000)
	for i := range rows {
		rows[i] = bdAccount{ID: fmt.Sprintf("bulk%05d", i), Owner: fmt.Sprintf("o%05d", i), Balance: int64(i)}
	}
	started := time.Now()
	created, err := b.accounts.CreateMany(withDeadline(t, 2*time.Minute), rows)
	if err != nil || len(created) != 50_000 {
		t.Fatalf("CreateMany(50k) = %d rows, %v", len(created), err)
	}
	if n := count(t, b.h.writer, `SELECT count(*) FROM bank_accounts`); n != 50_000 {
		t.Fatalf("writer has %d rows", n)
	}
	t.Logf("50k rows in %v", time.Since(started))
}
