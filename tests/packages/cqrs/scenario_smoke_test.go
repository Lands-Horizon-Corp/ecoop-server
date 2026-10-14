//go:build integration

package cqrs_test

import (
	"testing"
	"time"
)

// The harness itself: a world that changes the write database for real and
// replays its log must leave the read database equal to the write database, and
// the comparison must notice when it is not.
func TestHarness_RealWorldReplicates(t *testing.T) {
	t.Parallel()
	r := newRig(t, 3)
	ctx := longCtx(t)
	r.w.bulkInsert(1, 60, "a")
	r.w.bulkUpdate(10, 30, 5)
	r.w.remove(7)
	r.w.set(2, "renamed", 42)
	if err := r.w.deliverNew(ctx, r.f, 25); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t)
	if len(r.p.readAccounts()) != 59 {
		t.Fatalf("rows = %d", len(r.p.readAccounts()))
	}
}

func TestHarness_ComparisonNoticesDrift(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	ctx := longCtx(t)
	r.w.bulkInsert(1, 10, "a")
	if err := r.w.deliverNew(ctx, r.f, 50); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t)
	r.p.exec(r.p.read, `UPDATE accounts SET balance = balance + 1 WHERE id = 3`)
	if d, _ := r.p.syncDiff(); d == "" {
		t.Fatal("a changed column went unnoticed")
	}
	r.p.exec(r.p.read, `UPDATE accounts SET balance = balance - 1 WHERE id = 3`)
	r.p.exec(r.p.read, `DELETE FROM accounts WHERE id = 4`)
	if d, _ := r.p.syncDiff(); d == "" {
		t.Fatal("a missing row went unnoticed")
	}
	r.p.exec(r.p.read, `INSERT INTO accounts (id, owner) VALUES (4, 'a4')`)
	r.p.exec(r.p.read, `INSERT INTO accounts (id, owner) VALUES (500, 'extra')`)
	if d, _ := r.p.syncDiff(); d == "" {
		t.Fatal("an extra row went unnoticed")
	}
}

func TestHarness_PumpAndOutage(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	pm := r.w.startPump(t, r.f, 20)
	r.w.bulkInsert(1, 100, "p")
	r.waitInSync()
	pm.down.Store(true)
	r.w.bulkUpdate(1, 50, 3)
	time.Sleep(150 * time.Millisecond)
	if pm.caughtUp() {
		t.Fatal("a pump that is down must fall behind")
	}
	pm.down.Store(false)
	r.waitInSync()
}
