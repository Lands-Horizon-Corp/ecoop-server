package regressions

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/broker"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/cqrs"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/pagination"
)

// 25 UTF-8 and Unicode edge-case matrix. Text is checked before any SQL runs: what Postgres cannot
// store (NUL, invalid, truncated or overlong UTF-8) fails with ErrInvalidEncoding; what is storable
// but dangerous in a bank (U+FFFD from a lossy conversion, control characters, BiDi overrides) fails
// with ErrInvalidInput; everything else is stored NFC-normalized and round-trips exactly.

func TestBankDBUnicode_RejectedBeforeAnySQL(t *testing.T) {
	b := newBDBank(t, bdOpts{noRun: true})
	ctx := withDeadline(t, 10*time.Second)
	counter := &queryLog{table: "bank_accounts"}
	b.svc.Writer().Client().AddQueryHook(counter)

	cases := []struct {
		row, input string
		kind       error
	}{
		{"embedded NUL", "John\x00Doe", database.ErrInvalidEncoding},
		{"embedded NUL escape", "acc_\u0000_123", database.ErrInvalidEncoding},
		{"invalid bytes", "\xff\xff", database.ErrInvalidEncoding},
		{"truncated euro sign", string([]byte{0xE2, 0x82}), database.ErrInvalidEncoding},
		{"overlong apostrophe", string([]byte{0xC0, 0x27}), database.ErrInvalidEncoding},
		{"overlong slash", "\xc0\xaf", database.ErrInvalidEncoding},
		{"replacement character", "User � Name", database.ErrInvalidInput},
		{"right-to-left override", "Acc‮1234567", database.ErrInvalidInput},
		{"left-to-right embedding", "‪spoof", database.ErrInvalidInput},
		{"BiDi isolate", "⁦x⁩", database.ErrInvalidInput},
		{"ANSI escape (log injection)", "\x1b[31mRED", database.ErrInvalidInput},
		{"C1 control", "a\u0085b", database.ErrInvalidInput},
		{"bell", "ding\x07", database.ErrInvalidInput},
	}
	for i, c := range cases {
		t.Run(c.row, func(t *testing.T) {
			before := counter.n.Load()
			_, err := b.accounts.Create(ctx, bdAccount{ID: fmt.Sprintf("u%02d", i), Owner: c.input, Balance: 1})
			requireKind(t, err, c.kind)
			if counter.n.Load() != before {
				t.Fatal("the invalid text reached the database")
			}
		})
	}
	if n := count(t, b.h.writer, `SELECT count(*) FROM bank_accounts`); n != 0 {
		t.Fatalf("%d rows stored from rejected text", n)
	}
	if n := count(t, b.h.writer, `SELECT count(*) FROM bank_accounts WHERE owner LIKE '%' || chr(65533) || '%'`); n != 0 {
		t.Fatal("invalid bytes were silently turned into U+FFFD")
	}
}

func TestBankDBUnicode_SafeTextRoundTripsByteForByte(t *testing.T) {
	b := newBDBank(t, bdOpts{})
	ctx := withDeadline(t, 20*time.Second)
	cases := []struct{ name, owner string }{
		{"4-byte emoji", "Transfer 💳 to 🏦"},
		{"ZWJ family sequence", "👨‍👩‍👧 family"},
		{"flag", "🇵🇭 branch"},
		{"skin tone modifier", "👍🏽 ok"},
		{"CJK", "銀行口座"},
		{"Arabic (plain RTL)", "حساب مصرفي"},
		{"tab and newline kept", "line one\n\tline two"},
		{"zero-width non-joiner", "می\u200cخواهم"},
	}
	for i, c := range cases {
		if _, err := b.accounts.Create(ctx, bdAccount{ID: fmt.Sprintf("s%02d", i), Owner: c.owner, Balance: 1, Profile: map[string]any{"display": c.owner}}); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
	}
	b.replicate() // writer -> CDC JSON -> reader

	for i, c := range cases {
		id := fmt.Sprintf("s%02d", i)
		t.Run(c.name, func(t *testing.T) {
			var w, r string
			must(t, b.h.writer.QueryRow(`SELECT owner FROM bank_accounts WHERE id = $1`, id).Scan(&w))
			must(t, b.h.reader.QueryRow(`SELECT owner FROM bank_accounts WHERE id = $1`, id).Scan(&r))
			if w != c.owner || r != c.owner {
				t.Fatalf("writer %q, reader %q; want %q byte for byte", w, r, c.owner)
			}
			got, err := b.accounts.Find(ctx, eqFilter("owner", c.owner))
			if err != nil || len(got) != 1 || got[0].ID != id || got[0].Profile["display"] != c.owner {
				t.Fatalf("lookup by the exact text = %v, %v", accountIDs(got), err)
			}
		})
	}
}

func TestBankDBUnicode_NFCAndNFDAreTheSameIdentity(t *testing.T) {
	b := newBDBank(t, bdOpts{})
	ctx := withDeadline(t, 10*time.Second)
	nfc, nfd := "Müller", "Müller"
	if nfc == nfd {
		t.Fatal("test setup: the two forms must differ in bytes")
	}

	must(t, second(b.accounts.Create(ctx, bdAccount{ID: "m1", Owner: nfd, Balance: 1}))) // typed decomposed
	var stored string
	must(t, b.h.writer.QueryRow(`SELECT owner FROM bank_accounts WHERE id = 'm1'`).Scan(&stored))
	if stored != nfc {
		t.Fatalf("stored %q; want the composed (NFC) form", stored)
	}
	_, err := b.accounts.Create(ctx, bdAccount{ID: "m2", Owner: nfc, Balance: 1})
	requireKind(t, err, database.ErrDuplicate) // the unique index sees the same name
	b.replicate()

	for form, q := range map[string]string{"NFC": nfc, "NFD": nfd} {
		got, err := b.accounts.Find(ctx, eqFilter("owner", q))
		if err != nil || len(got) != 1 || got[0].ID != "m1" {
			t.Errorf("lookup in %s form = %v, %v; want m1", form, accountIDs(got), err)
		}
	}
	_, err = b.accounts.Create(ctx, bdAccount{ID: "m3", Owner: "x", Balance: 1, Profile: map[string]any{nfc: 1, nfd: 2}})
	requireKind(t, err, database.ErrInvalidInput) // two JSON keys collapse into one
}

func TestBankDBUnicode_FiltersAndCursorsWithBadTextAreRejected(t *testing.T) {
	b := newBDBank(t, bdOpts{noRun: true})
	ctx := withDeadline(t, 10*time.Second)
	for name, v := range map[string]string{"NUL": "a\x00b", "overlong": "\xc0\x27", "truncated": "\xe2\x82"} {
		_, err := b.accounts.Count(ctx, eqFilter("owner", v))
		if !errors.Is(err, pagination.ErrInvalidFilter) {
			t.Errorf("%s filter = %v; want ErrInvalidFilter", name, err)
		}
		requireKind(t, err, database.ErrInvalidEncoding)
	}
}

func TestBankDBUnicode_CDCMessageWithInvalidUTF8IsDeadLetteredNotCorrupted(t *testing.T) {
	bb := newBatchBroker()
	b := newBDBank(t, bdOpts{broker: bb})
	good, err := json.Marshal(cqrs.CQRSQueuePayload[bdAccount]{EventID: "e1", ChangeType: cqrs.ChangeTypeCreated,
		Payload: bdAccount{ID: "c1", Owner: "PLACEHOLDER", Balance: 1, Version: 1, UpdatedAt: time.Now()}})
	must(t, err)
	bad := []byte(strings.Replace(string(good), "PLACEHOLDER", "Jo\xffn", 1)) // json would read it as U+FFFD
	must(t, bb.deliver(t, "bank_accounts", broker.Message{Key: []byte("e1"), Value: bad}))
	if n := len(bb.publishedTo("bank_accounts.dlq")); n != 1 {
		t.Fatalf("%d dead-lettered; want the invalid message", n)
	}
	if n := count(t, b.h.reader, `SELECT count(*) FROM bank_accounts`); n != 0 {
		t.Fatal("a copy with U+FFFD reached the read model")
	}
}

func TestBankDBUnicode_RawTextOptOutKeepsBytesButNotInvalidOnes(t *testing.T) {
	h := newDBHarness(t)
	svc := h.newService(h.writerDSN, h.readerDSN)
	must(t, database.Register(svc, database.Registration[dbMember, dbMemberResource, dbMemberRequest, string]{RawText: true}))
	must(t, svc.Start(bg))
	t.Cleanup(func() { _ = svc.Stop(bg) })
	members, err := database.Get[dbMember, dbMemberResource, dbMemberRequest, string](svc)
	must(t, err)
	ctx := withDeadline(t, 5*time.Second)

	for id, name := range map[string]string{"r1": "Müller", "r2": "kept � as given"} {
		must(t, second(members.Create(ctx, dbMember{ID: id, Name: name})))
		var stored string
		must(t, h.writer.QueryRow(`SELECT name FROM db_members WHERE id = $1`, id).Scan(&stored))
		if stored != name {
			t.Errorf("RawText stored %q; want %q unchanged", stored, name)
		}
	}
	_, err = members.Create(ctx, dbMember{ID: "r3", Name: "a\x00b"})
	requireKind(t, err, database.ErrInvalidEncoding) // never storable, opt-out or not
}
