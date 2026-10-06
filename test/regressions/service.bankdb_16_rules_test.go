package regressions

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
)

// 16 Cooperative-bank rules, enforced twice: by the transfer service before it touches the database,
// and by the database itself (triggers) for any code path that bypasses the service.

func TestBankDBRules_TransfersRespectAccountRules(t *testing.T) {
	b := newBDBank(t, bdOpts{noRun: true})
	ctx := withDeadline(t, 10*time.Second)
	closed := time.Now().UTC()
	for _, a := range []bdAccount{
		{ID: "php1", Owner: "p1", Balance: 1000},
		{ID: "php2", Owner: "p2", Balance: 1000},
		{ID: "usd", Owner: "u", Currency: "USD", Balance: 1000},
		{ID: "gone", Owner: "g", Balance: 1000, ClosedAt: &closed},
		{ID: "full", Owner: "f", Balance: math.MaxInt64 - 5},
	} {
		must(t, second(b.accounts.Create(ctx, a)))
	}
	before := map[string]int64{}
	for _, id := range []string{"php1", "php2", "usd", "gone", "full"} {
		before[id] = b.writerBalance(id)
	}

	cases := []struct {
		name string
		req  transferReq
		want error
	}{
		{"from a closed account", transferReq{"k1", "gone", "php1", 10}, errBankAccountClosed},
		{"into a closed account", transferReq{"k2", "php1", "gone", 10}, errBankAccountClosed},
		{"across currencies", transferReq{"k3", "php1", "usd", 10}, errBankCurrency},
		{"overflowing the destination", transferReq{"k4", "php1", "full", 10}, errBankOverflow},
		{"zero amount", transferReq{"k5", "php1", "php2", 0}, errBankBadAmount},
		{"MinInt64 amount", transferReq{"k6", "php1", "php2", math.MinInt64}, errBankBadAmount},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, _, err := b.Transfer(ctx, c.req); !errors.Is(err, c.want) {
				t.Fatalf("Transfer = %v; want %v", err, c.want)
			}
		})
	}
	for id, bal := range before {
		if got := b.writerBalance(id); got != bal {
			t.Errorf("balance %s changed from %d to %d on a refused transfer", id, bal, got)
		}
	}
	if n := count(t, b.h.writer, `SELECT count(*) FROM bank_transfers`); n != 0 {
		t.Fatalf("%d transfers recorded; want none", n)
	}
	if _, _, err := b.Transfer(ctx, transferReq{"ok", "php1", "php2", 10}); err != nil {
		t.Fatalf("a valid transfer next to the refused ones: %v", err)
	}
}

func TestBankDBRules_DatabaseRefusesRuleBreaksThatBypassTheService(t *testing.T) {
	b := newBDBank(t, bdOpts{noRun: true})
	ctx := withDeadline(t, 10*time.Second)
	closed := time.Now().UTC()
	must(t, second(b.accounts.Create(ctx, bdAccount{ID: "gone", Owner: "g", Balance: 100, ClosedAt: &closed})))
	must(t, second(b.accounts.Create(ctx, bdAccount{ID: "php", Owner: "p", Balance: 100})))
	must(t, second(b.accounts.Create(ctx, bdAccount{ID: "usd", Owner: "u", Currency: "USD", Balance: 100})))

	_, err := b.accounts.IncrementByID(ctx, "gone", "balance", 50) // straight to cqrs, no ledger
	requireKind(t, err, database.ErrRejected)
	_, err = b.transfers.Create(ctx, bdTransfer{ID: "x", IdempotencyKey: "x", FromAccount: "php", ToAccount: "usd", Amount: 1, RequestHash: "h"})
	requireKind(t, err, database.ErrRejected)
	if got := b.writerBalance("gone"); got != 100 {
		t.Fatalf("closed account balance = %d; want 100", got)
	}
}
