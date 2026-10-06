package regressions

import (
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
)

// 12 Server-side timeouts and pool hygiene: Postgres itself stops a runaway statement, a lock wait
// and an abandoned transaction even when the caller set no deadline, and pooled connections are
// recycled and survive being killed.

func timeoutBank(t *testing.T) *bdLedger {
	t.Helper()
	b := newBDBank(t, bdOpts{noRun: true, app: "timeouts", svcOpts: []database.Option{database.WithServerTimeouts(database.ServerTimeouts{
		Statement: 300 * time.Millisecond, Lock: 200 * time.Millisecond, IdleInTransaction: 400 * time.Millisecond,
	})}})
	b.open("a", 100)
	b.open("b", 0)
	return b
}

func TestBankDBTimeouts_SettingsAreStoredOnBothDatabases(t *testing.T) {
	b := timeoutBank(t)
	for _, name := range []string{"writer", "reader"} {
		var stmt, lock, idle string
		// A fresh session picks up the database-level settings.
		conn := openInspect(t, map[string]string{"writer": b.h.writerDSN, "reader": b.h.readerDSN}[name])
		must(t, conn.QueryRow(`SELECT current_setting('statement_timeout'), current_setting('lock_timeout'),
			current_setting('idle_in_transaction_session_timeout')`).Scan(&stmt, &lock, &idle))
		if stmt != "300ms" || lock != "200ms" || idle != "400ms" {
			t.Errorf("%s: statement=%s lock=%s idle_in_tx=%s", name, stmt, lock, idle)
		}
	}
}

func TestBankDBTimeouts_RunawayStatementIsStoppedByTheServer(t *testing.T) {
	b := timeoutBank(t)
	_, err := b.h.reader.Exec(`INSERT INTO bank_accounts (id, owner, balance) VALUES ('r', 'r', 1)`) // pg_sleep needs a row
	must(t, err)
	started := time.Now()
	_, err = b.accounts.Find(bg, sleepFilter(10)) // no client deadline at all
	requireKind(t, err, database.ErrTimeout)
	if d := time.Since(started); d > 3*time.Second {
		t.Fatalf("the runaway query ran for %v", d)
	}
}

func TestBankDBTimeouts_LockWaitIsBounded(t *testing.T) {
	b := timeoutBank(t)
	holder, err := b.h.writer.Begin()
	must(t, err)
	defer func() { _ = holder.Rollback() }()
	_, err = holder.Exec(`SELECT 1 FROM bank_accounts WHERE id = 'a' FOR UPDATE`)
	must(t, err)

	started := time.Now()
	_, err = b.accounts.IncrementByID(bg, "a", "balance", 1) // no client deadline
	requireKind(t, err, database.ErrTimeout)
	if d := time.Since(started); d > 2*time.Second {
		t.Fatalf("waited %v for the lock", d)
	}
}

func TestBankDBTimeouts_AbandonedTransactionReleasesItsLocks(t *testing.T) {
	b := timeoutBank(t)
	tx, err := b.accounts.StartTx(bg)
	must(t, err)
	_, err = b.accounts.GetByIDWithTx(bg, &tx, "a") // locks the row, then the caller "forgets" the tx
	must(t, err)
	time.Sleep(time.Second) // past idle_in_transaction_session_timeout

	if _, err := b.accounts.IncrementByID(bg, "a", "balance", 5); err != nil {
		t.Fatalf("the abandoned transaction still holds the lock: %v", err)
	}
	_, err = b.accounts.IncrementByIDWithTx(bg, tx, "a", "balance", 1)
	requireKind(t, err, database.ErrUnavailable) // its session was terminated
	_ = tx.Rollback()
	if got := b.writerBalance("a"); got != 105 {
		t.Fatalf("balance a = %d; want 105", got)
	}
}

func TestBankDBTimeouts_ConnectionsAreRecycledAndSurviveBeingKilled(t *testing.T) {
	b := newBDBank(t, bdOpts{noRun: true, app: "recycle", maxOpen: 4, svcOpts: []database.Option{database.WithConnMaxLifetime(100 * time.Millisecond)}})
	b.open("a", 0)
	pids := map[int]bool{}
	for range 30 {
		var pid int
		must(t, b.svc.Writer().Client().QueryRowContext(withDeadline(t, 5*time.Second), `SELECT pg_backend_pid()`).Scan(&pid))
		pids[pid] = true
		time.Sleep(20 * time.Millisecond)
	}
	if st := b.svc.Writer().Client().DB.Stats(); st.MaxLifetimeClosed == 0 || len(pids) <= 4 {
		t.Fatalf("lifetime closes=%d, distinct backends=%d; connections are not being recycled", st.MaxLifetimeClosed, len(pids))
	}

	// Kill every idle pooled connection behind the pool's back (failover, PgBouncer restart).
	_, err := b.h.writer.Exec(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'recycle-w' AND state = 'idle'`)
	must(t, err)
	for i := range 10 {
		if _, err := b.accounts.IncrementByID(withDeadline(t, 5*time.Second), "a", "balance", 1); err != nil {
			t.Fatalf("write %d after the pool's connections were killed: %v", i, err)
		}
	}
	if got := b.writerBalance("a"); got != 10 {
		t.Fatalf("balance = %d; want 10", got)
	}
}
