package regressions

import (
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
)

// 11 Commit outcome unknown: the connection dies while COMMIT is in flight, so the client gets an
// error without knowing whether the transfer committed. Retrying with the same idempotency key must
// settle it exactly once either way.

func TestBankDBCommitUnknown_ReplyLostAfterCommitRetriesAsReplay(t *testing.T) {
	proxy := newFaultProxy(t)
	b := newBDBank(t, bdOpts{route: proxy.route, noRun: true})
	b.open("a", 1000)
	b.open("b", 0)
	req := transferReq{Key: "pay-1", From: "a", To: "b", Amount: 300}

	proxy.commitFault.Store(commitCutReply)
	_, _, err := b.Transfer(withDeadline(t, 10*time.Second), req)
	requireKind(t, err, database.ErrUnavailable) // the client cannot tell what happened
	if got := b.writerBalance("b"); got != 300 {
		t.Fatalf("balance b = %d; the server should have committed before the reply was lost", got)
	}

	tr, replayed, err := b.Transfer(withDeadline(t, 10*time.Second), req) // the client retries
	if err != nil || !replayed || tr.ID != "t-pay-1" {
		t.Fatalf("retry = %+v, replayed=%v, %v; want the committed transfer as a replay", tr, replayed, err)
	}
	if a, bb := b.writerBalance("a"), b.writerBalance("b"); a != 700 || bb != 300 {
		t.Fatalf("balances a=%d b=%d; want 700/300 (money moved once)", a, bb)
	}
}

func TestBankDBCommitUnknown_CommitNeverArrivedRetriesExecuteOnce(t *testing.T) {
	proxy := newFaultProxy(t)
	b := newBDBank(t, bdOpts{route: proxy.route, noRun: true})
	b.open("a", 1000)
	b.open("b", 0)
	req := transferReq{Key: "pay-2", From: "a", To: "b", Amount: 300}

	proxy.commitFault.Store(commitDrop)
	_, _, err := b.Transfer(withDeadline(t, 10*time.Second), req)
	requireKind(t, err, database.ErrUnavailable)
	if got := b.writerBalance("b"); got != 0 {
		t.Fatalf("balance b = %d; nothing should have committed", got)
	}

	_, replayed, err := b.Transfer(withDeadline(t, 10*time.Second), req)
	if err != nil || replayed {
		t.Fatalf("retry = replayed %v, %v; want a first execution", replayed, err)
	}
	if a, bb := b.writerBalance("a"), b.writerBalance("b"); a != 700 || bb != 300 {
		t.Fatalf("balances a=%d b=%d; want 700/300", a, bb)
	}
	if n := count(t, b.h.writer, `SELECT count(*) FROM bank_transfers`); n != 1 {
		t.Fatalf("%d transfers recorded; want 1", n)
	}
}
