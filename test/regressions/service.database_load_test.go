package regressions

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/cqrs"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/pagination"
	"github.com/uptrace/bun"
	"go.uber.org/goleak"
)

// Latency, timeout and load tests for the database service: deadlines must abort blocked and slow
// queries in every package (and cancel them on the server), a small pool must queue rather than hang
// or leak, and a full lifecycle under load must leave no goroutines or server connections behind.

// loadService is a started DatabaseService over the harness databases with its own pool size. Its
// connections carry application_name app-w / app-r so pg_stat_activity can count exactly them.
func loadService(t testing.TB, h *dbHarness, maxOpen int, app string) (*database.DatabaseService, dbMemberService) {
	t.Helper()
	svc := database.NewDatabaseService(
		h.writerDSN+"&application_name="+app+"-w", h.readerDSN+"&application_name="+app+"-r",
		maxOpen, maxOpen,
		nil, nil, nil,
		h.migrations, true, nil, nil,
		nil, h.broker, nil,
		50, 10*time.Millisecond,
	)
	if err := database.Register(svc, dbMemberRegistration()); err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(bg); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = svc.Stop(bg) })
	members, err := database.Get[dbMember, dbMemberResource, dbMemberRequest, string](svc)
	if err != nil {
		t.Fatal(err)
	}
	return svc, members
}

// serverConns counts backend connections opened under application_name, optionally only those in state.
const serverConns = `SELECT count(*) FROM pg_stat_activity WHERE application_name = $1`

// sleepFilter makes the database spend `seconds` on every row it evaluates.
func sleepFilter(seconds int) pagination.StructuredFilter {
	return pagination.StructuredFilter{Filters: []pagination.Filter{{
		Mode: pagination.ModeCustom,
		Custom: func(q *bun.SelectQuery, _ any) (*bun.SelectQuery, error) {
			return q.Where("pg_sleep(?) IS NOT NULL", seconds), nil
		},
	}}}
}

// expectDeadline runs call under a 10ms deadline and requires context.DeadlineExceeded well before
// the operation itself could have finished.
func expectDeadline(t *testing.T, name string, call func(ctx context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(bg, 10*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := call(ctx)
	elapsed := time.Since(started)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("%s: err = %v; want context.DeadlineExceeded", name, err)
	}
	if elapsed > 2*time.Second {
		t.Errorf("%s took %v to give up; the deadline was 10ms", name, elapsed)
	}
}

// ---------------------------------------------------------------------------------------------------
// 1. Context cancellation and timeout propagation
// ---------------------------------------------------------------------------------------------------

// Sad: with the row locked by another transaction, every write path (and the locking read inside a
// transaction) gives up at the deadline, the server stops waiting on the lock, and the row is unchanged.
func TestDatabaseLoad_SadDeadlineAbortsWritesBlockedOnALock(t *testing.T) {
	h := newDBHarness(t)
	svc, members := loadService(t, h, 8, "deadline-lock")
	if _, err := members.Create(bg, dbMember{ID: "m1", Name: "Ann", Balance: 10}); err != nil {
		t.Fatal(err)
	}
	holder, err := h.writer.BeginTx(bg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback() }()
	if _, err := holder.Exec(`SELECT 1 FROM db_members WHERE id = 'm1' FOR UPDATE`); err != nil {
		t.Fatal(err)
	}

	expectDeadline(t, "cqrs UpdateByID", func(ctx context.Context) error {
		return second(members.UpdateByID(ctx, "m1", dbMember{ID: "m1", Name: "x"}))
	})
	expectDeadline(t, "cqrs IncrementByID", func(ctx context.Context) error {
		return second(members.IncrementByID(ctx, "m1", "balance", 1))
	})
	expectDeadline(t, "cqrs DeleteByID", func(ctx context.Context) error { return members.DeleteByID(ctx, "m1") })
	expectDeadline(t, "pagination GetByIDWithTx (FOR UPDATE)", func(ctx context.Context) error {
		tx, err := members.StartTx(bg) // the transaction outlives the deadline; only the read is bounded
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		return second(members.GetByIDWithTx(ctx, &tx, "m1"))
	})
	expectDeadline(t, "sql client", func(ctx context.Context) error {
		return second(svc.Writer().Client().ExecContext(ctx, `UPDATE db_members SET name = 'x' WHERE id = 'm1'`))
	})

	awaitCount(t, h.writer, 0, serverConns+` AND wait_event_type = 'Lock'`, "deadline-lock-w")
	if err := holder.Rollback(); err != nil {
		t.Fatal(err)
	}
	if got := count(t, h.writer, `SELECT balance FROM db_members WHERE id = 'm1'`); got != 10 {
		t.Fatalf("balance = %d after only timed-out writes; want 10", got)
	}
}

// Sad: a slow query on the reader is cut off at the deadline in pagination (Find, Count, Exists,
// Paginate) and in the raw sql client, and the server-side query is cancelled rather than left running.
func TestDatabaseLoad_SadDeadlineCancelsSlowReadsOnTheServer(t *testing.T) {
	h := newDBHarness(t)
	svc, members := loadService(t, h, 8, "deadline-read")
	h.seedReader(1) // pg_sleep only runs once there is a row to filter
	slow := sleepFilter(5)

	expectDeadline(t, "pagination Find", func(ctx context.Context) error { return second(members.Find(ctx, slow)) })
	expectDeadline(t, "pagination Count", func(ctx context.Context) error { return second(members.Count(ctx, slow)) })
	expectDeadline(t, "pagination Exists", func(ctx context.Context) error { return second(members.Exists(ctx, slow)) })
	expectDeadline(t, "pagination Paginate", func(ctx context.Context) error {
		return second(members.Paginate(ctx, pagination.Pagination{Filter: slow, PageSize: 5}))
	})
	expectDeadline(t, "sql client", func(ctx context.Context) error {
		return second(svc.Reader().Client().ExecContext(ctx, `SELECT pg_sleep(5)`))
	})

	awaitCount(t, h.reader, 0, serverConns+` AND state = 'active' AND query LIKE '%pg_sleep%'`, "deadline-read-r")
}

// Sad: an already-cancelled context is refused by every package before any work is done.
func TestDatabaseLoad_SadCancelledContextIsRefusedEverywhere(t *testing.T) {
	h := newDBHarness(t)
	svc, members := loadService(t, h, 8, "cancelled")
	ctx, cancel := context.WithCancel(bg)
	cancel()

	for name, call := range map[string]func() error{
		"cqrs StartTx":         func() error { return second(members.StartTx(ctx)) },
		"cqrs Create":          func() error { return second(members.Create(ctx, dbMember{ID: "m1", Name: "Ann"})) },
		"cqrs CreateMany":      func() error { return second(members.CreateMany(ctx, []dbMember{{ID: "m2", Name: "Bo"}})) },
		"pagination Find":      func() error { return second(members.Find(ctx, pagination.StructuredFilter{})) },
		"pagination Paginate":  func() error { return second(members.Paginate(ctx, pagination.Pagination{})) },
		"sql writer ping":      func() error { return svc.Writer().Ping(ctx) },
		"sql reader exec":      func() error { return second(svc.Reader().Client().ExecContext(ctx, `SELECT 1`)) },
		"sql writer migration": func() error { return svc.Writer().Migrate(ctx) },
	} {
		if err := call(); !errors.Is(err, context.Canceled) {
			t.Errorf("%s with a cancelled context = %v; want context.Canceled", name, err)
		}
	}
	if n := count(t, h.writer, `SELECT count(*) FROM db_members`); n != 0 {
		t.Fatalf("%d rows written with a cancelled context", n)
	}
}

// ---------------------------------------------------------------------------------------------------
// 2. Connection pool exhaustion
// ---------------------------------------------------------------------------------------------------

// Happy: 50 concurrent pipelines through a pool of 5 (writer transaction with cqrs + pagination reads,
// then reader queries). Some roll back on a business error and some abandon their transaction by
// cancelling its context. Everything finishes, nothing is leaked, and the pool never grows past 5.
func TestDatabaseLoad_HappyPoolOfFiveQueuesFiftyPipelines(t *testing.T) {
	h := newDBHarness(t)
	svc, members := loadService(t, h, 5, "pool")
	errBusiness := errors.New("business rule")
	var committed atomic.Int64

	errs := runParallel(t, 50, func(ctx context.Context, i int) error {
		id := fmt.Sprintf("m%02d", i)
		txCtx, cancelTx := context.WithCancel(ctx)
		defer cancelTx()
		tx, err := members.StartTx(txCtx)
		if err != nil {
			return err
		}
		if _, err := members.CreateWithTx(txCtx, tx, dbMember{ID: id, Name: id}); err != nil {
			return members.EndTx(txCtx, tx, err)
		}
		if _, err := members.IncrementByIDWithTx(txCtx, tx, id, "balance", 5); err != nil {
			return members.EndTx(txCtx, tx, err)
		}
		if _, err := members.GetByIDWithTx(txCtx, &tx, id); err != nil {
			return members.EndTx(txCtx, tx, err)
		}
		switch {
		case i%10 == 9: // abandoned: never ended, only its context is cancelled
			cancelTx()
			return nil
		case i%5 == 4: // business failure: rolled back
			if err := members.EndTx(txCtx, tx, errBusiness); !errors.Is(err, errBusiness) {
				return err
			}
		default:
			if err := members.EndTx(txCtx, tx, nil); err != nil {
				return err
			}
			committed.Add(1)
		}
		if _, err := members.Find(ctx, pagination.StructuredFilter{}); err != nil {
			return err
		}
		_, err = members.Count(ctx, pagination.StructuredFilter{})
		return err
	})
	if len(errs) > 0 {
		t.Fatalf("%d pipelines failed, first: %v", len(errs), errs[0])
	}
	if got := count(t, h.writer, `SELECT count(*) FROM db_members`); got != committed.Load() || got != 40 {
		t.Fatalf("writer has %d members; want the %d committed (40: 5 abandoned, 5 rolled back)", got, committed.Load())
	}

	for name, sqlSvc := range map[string]interface{ Client() *bun.DB }{"writer": svc.Writer(), "reader": svc.Reader()} {
		deadline := time.Now().Add(5 * time.Second)
		stats := sqlSvc.Client().DB.Stats()
		for stats.InUse > 0 && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
			stats = sqlSvc.Client().DB.Stats()
		}
		if stats.InUse != 0 {
			t.Errorf("%s pool still has %d connections in use; a pipeline leaked one", name, stats.InUse)
		}
		if stats.MaxOpenConnections != 5 || stats.OpenConnections > 5 {
			t.Errorf("%s pool: max %d, open %d; want at most 5", name, stats.MaxOpenConnections, stats.OpenConnections)
		}
	}
	if w := svc.Writer().Client().DB.Stats().WaitCount; w == 0 {
		t.Error("the writer pool never made a caller wait; the test did not exhaust it")
	}
}

// Sad: with every pooled connection held, a new pipeline waits only until its deadline and then
// fails with context.DeadlineExceeded instead of hanging; once connections return it succeeds.
func TestDatabaseLoad_SadExhaustedPoolHonorsTheDeadline(t *testing.T) {
	h := newDBHarness(t)
	_, members := loadService(t, h, 5, "exhausted")

	var held []bun.Tx
	for range 5 {
		tx, err := members.StartTx(bg)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, tx)
	}
	ctx, cancel := context.WithTimeout(bg, 200*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := members.Create(ctx, dbMember{ID: "m1", Name: "Ann"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Create on an exhausted pool = %v; want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("Create waited %v on an exhausted pool; the deadline was 200ms", elapsed)
	}

	for _, tx := range held {
		if err := members.EndTx(bg, tx, nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := members.Create(bg, dbMember{ID: "m1", Name: "Ann"}); err != nil {
		t.Fatalf("Create after the pool drained: %v", err)
	}
}

// ---------------------------------------------------------------------------------------------------
// 3. Goroutine and connection leaks
// ---------------------------------------------------------------------------------------------------

// pipeline is one pass through all three packages: a cqrs transaction with a pagination read inside
// it, an increment, a sync event to the runner and a pagination read on the reader.
func pipeline(ctx context.Context, members dbMemberService, events func(key, value []byte) error, id string) error {
	tx, err := members.StartTx(ctx)
	if err != nil {
		return err
	}
	err = func() error {
		if _, err := members.CreateWithTx(ctx, tx, dbMember{ID: id, Name: id}); err != nil {
			return err
		}
		_, err := members.GetByIDWithTx(ctx, &tx, id)
		return err
	}()
	if err := members.EndTx(ctx, tx, err); err != nil {
		return err
	}
	m, err := members.IncrementByID(ctx, id, "balance", 1)
	if err != nil {
		return err
	}
	if events != nil {
		value := fmt.Appendf(nil, `{"event_id":"e-%s","change_type":%d,"payload":{"id":%q,"name":%q,"balance":%d}}`,
			id, cqrs.ChangeTypeCreated, m.ID, m.Name, m.Balance)
		if err := events([]byte(id), value); err != nil {
			return err
		}
	}
	_, err = members.Find(ctx, pagination.StructuredFilter{Filters: []pagination.Filter{{Field: "id", Mode: pagination.ModeEqual, Value: id}}})
	return err
}

// Happy: a full lifecycle under load (Start, Run, 300 concurrent pipelines feeding the runner, Stop with
// events still in flight) leaves no goroutines and no server connections behind.
func TestDatabaseLoad_HappyNoGoroutineOrConnectionLeaksAfterStop(t *testing.T) {
	h := newDBHarness(t)
	baseline := goleak.IgnoreCurrent() // the harness's own inspection pools are not under test

	svc, members := loadService(t, h, 8, "leak")
	svc.Run(bg)
	events := h.broker.await(t, "members")

	errs := runParallel(t, 300, func(ctx context.Context, i int) error {
		return pipeline(ctx, members, events, fmt.Sprintf("m%03d", i))
	})
	if len(errs) > 0 {
		t.Fatalf("%d pipelines failed, first: %v", len(errs), errs[0])
	}
	if err := svc.Stop(bg); err != nil { // the runner may still be flushing the last batch
		t.Fatalf("Stop: %v", err)
	}

	verifyNoDBLeaks(t, baseline)
	awaitCount(t, h.writer, 0, serverConns, "leak-w")
	awaitCount(t, h.reader, 0, serverConns, "leak-r")
}

// BenchmarkDatabasePipeline measures one pass through all three packages and then checks, like the
// test above, that the run leaked neither goroutines nor connections.
//
//	go test ./test/regressions/ -run '^$' -bench DatabasePipeline -benchmem
func BenchmarkDatabasePipeline(b *testing.B) {
	h := newDBHarness(b)
	baseline := goleak.IgnoreCurrent()
	svc, members := loadService(b, h, 8, "bench")
	svc.Run(bg)
	events := h.broker.await(b, "members")

	var seq atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := pipeline(bg, members, events, fmt.Sprintf("b%d", seq.Add(1))); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.StopTimer()

	if inUse := svc.Writer().Client().DB.Stats().InUse; inUse != 0 {
		b.Errorf("%d writer connections still in use after the run", inUse)
	}
	if err := svc.Stop(bg); err != nil {
		b.Fatalf("Stop: %v", err)
	}
	verifyNoDBLeaks(b, baseline)
}
