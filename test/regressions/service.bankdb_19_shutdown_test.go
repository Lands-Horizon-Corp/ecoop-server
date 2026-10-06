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
	"github.com/uptrace/bun"
)

// 19 Graceful shutdown: Stop while transfers are in flight leaves each one fully committed or fully
// absent; afterwards every entry point reports ErrUnavailable; and the runners outlive the context
// they were started with (fx cancels OnStart's context right after start-up).

func TestBankDBShutdown_StopDuringInFlightTransfersIsAtomic(t *testing.T) {
	b := newBDBank(t, bdOpts{noRun: true, maxOpen: 16})
	ids := []string{"a", "b", "c", "d"}
	for _, id := range ids {
		b.open(id, 100_000)
	}
	var ok atomic.Int64
	started := make(chan struct{})
	go func() {
		<-started
		time.Sleep(15 * time.Millisecond)
		_ = b.svc.Stop(bg) // the pod is told to terminate mid-traffic
	}()

	var once atomic.Bool
	_ = runParallel(t, 200, func(ctx context.Context, i int) error {
		if once.CompareAndSwap(false, true) {
			close(started)
		}
		_, _, err := b.Transfer(ctx, transferReq{Key: fmt.Sprintf("k%03d", i), From: ids[i%4], To: ids[(i+1)%4], Amount: 1})
		switch {
		case err == nil:
			ok.Add(1)
		case errors.Is(err, database.ErrUnavailable), errors.Is(err, database.ErrCanceled):
		default:
			t.Errorf("transfer %d failed with %v; want success or ErrUnavailable", i, err)
		}
		return nil
	})

	if ok.Load() == 0 || ok.Load() == 200 {
		t.Logf("note: %d of 200 transfers committed (shutdown landed at an edge)", ok.Load())
	}
	for q, want := range map[string]int64{
		`SELECT count(*) FROM bank_transfers`:    ok.Load(),
		`SELECT count(*) FROM bank_audit`:        2 * ok.Load(),
		`SELECT SUM(balance) FROM bank_accounts`: 400_000,
	} {
		if got := count(t, b.h.writer, q); got != want {
			t.Errorf("%s = %d; want %d (a transfer was half applied)", q, got, want)
		}
	}
}

func TestBankDBShutdown_EntryPointsAfterStopAreUnavailable(t *testing.T) {
	b := newBDBank(t, bdOpts{noRun: true})
	b.open("a", 1)
	must(t, b.svc.Stop(bg))
	ctx := withDeadline(t, 5*time.Second)

	calls := map[string]func() error{
		"cqrs write":       func() error { return second(b.accounts.Create(ctx, bdAccount{ID: "x", Owner: "x"})) },
		"cqrs transaction": func() error { return second(b.accounts.StartTx(ctx)) },
		"pagination read":  func() error { return second(b.accounts.Find(ctx, eqFilter("id", "a"))) },
		"pagination count": func() error { return second(b.accounts.Count(ctx, eqFilter("id", "a"))) },
		"database.RunInTx": func() error {
			return database.RunInTx(ctx, b.svc, nil, func(context.Context, bun.Tx) error { return nil })
		},
		"ledger transfer": func() error { return second3(b.Transfer(ctx, transferReq{"k", "a", "a", 1})) },
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			noPanic(t, name, func() { requireKind(t, call(), database.ErrUnavailable) })
		})
	}
	if err := b.svc.Stop(bg); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
}

func TestBankDBShutdown_RunnersOutliveTheirStartContext(t *testing.T) {
	b := newBDBank(t, bdOpts{noRun: true})
	startCtx, cancel := context.WithCancel(bg)
	b.svc.Run(startCtx)
	cancel() // what fx does to OnStart's context once start-up is over
	handler := b.h.broker.await(t, "bank_accounts")
	time.Sleep(50 * time.Millisecond)

	dbPublish(t, handler, "late-1", cqrs.ChangeTypeCreated, bdAccount{ID: "a", Owner: "Ann", Balance: 5, Version: 1, UpdatedAt: time.Now()})
	b.awaitReader(`SELECT count(*) FROM bank_accounts WHERE id = 'a'`, 1)
}

func TestBankDBShutdown_RunTwiceThenStopDoesNotHang(t *testing.T) {
	b := newBDBank(t, bdOpts{noRun: true})
	b.svc.Run(bg)
	b.svc.Run(bg) // a second call must not start a second set of runners Stop cannot reach
	done := make(chan error, 1)
	go func() { done <- b.svc.Stop(bg) }()
	select {
	case err := <-done:
		must(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Stop hung after Run was called twice")
	}
}

func TestBankDBShutdown_LifecycleMisuseIsHarmless(t *testing.T) {
	var nilSvc *database.DatabaseService
	noPanic(t, "nil service", func() {
		if err := database.Register(nilSvc, dbMemberRegistration()); !errors.Is(err, database.ErrNilService) {
			t.Errorf("Register(nil) = %v; want ErrNilService", err)
		}
		if _, err := database.Get[dbMember, dbMemberResource, dbMemberRequest, string](nilSvc); !errors.Is(err, database.ErrNilService) {
			t.Errorf("Get(nil) = %v; want ErrNilService", err)
		}
	})

	// Every optional dependency nil: no loggers, broker, broadcaster or validator.
	h := newDBHarness(t)
	svc := database.NewDatabaseService(h.writerDSN, h.readerDSN, 2, 8, nil, nil, nil, h.migrations, true, nil, nil, nil, nil, nil, 0, 0)
	must(t, database.Register(svc, database.Registration[dbMember, dbMemberResource, dbMemberRequest, string]{}))
	noPanic(t, "out-of-order lifecycle", func() {
		must(t, svc.Stop(bg)) // before Start
		svc.Run(bg)           // before Start: nothing to run
		if svc.Writer() != nil {
			t.Error("connections exist before Start")
		}
		must(t, svc.Start(bg))
		members, err := database.Get[dbMember, dbMemberResource, dbMemberRequest, string](svc)
		must(t, err)
		must(t, second(members.Create(bg, dbMember{ID: "m1", Name: "Ann"})))
		svc.Run(bg) // no broker: the runner exits with an error instead of panicking
		must(t, svc.Stop(bg))
		must(t, svc.Stop(bg)) // twice
	})
}
