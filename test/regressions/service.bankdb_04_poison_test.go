package regressions

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/cqrs"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/pagination"
	"golang.org/x/text/unicode/norm"
)

// 04 Poison pills and boundaries: hostile or malformed input (broken CDC messages, integer edges,
// injection strings, invalid UTF-8, arbitrary bytes, unserializable JSONB) is rejected or stored
// verbatim, never executed, never panics, and never takes valid work down with it.

func TestBankDBPoison_RunnerSurvivesMalformedMessages(t *testing.T) {
	b := newBDBank(t, bdOpts{})
	accounts := b.events["bank_accounts"]

	poison := map[string][]byte{
		"truncated json":       []byte(`{"event_id":"p1","change_type":1,"payload":{"id":"x"`),
		"wrong field type":     []byte(`{"event_id":"p2","change_type":1,"payload":{"id":"x","balance":"lots"}}`),
		"int64 overflow":       []byte(`{"event_id":"p3","change_type":1,"payload":{"id":"x","balance":1e30}}`),
		"binary garbage":       {0x00, 0xff, 0xfe, 0x7b, 0x00},
		"empty":                {},
		"json null":            []byte(`null`),
		"json array":           []byte(`[1,2,3]`),
		"unknown change type":  []byte(`{"event_id":"p4","change_type":99,"payload":{"id":"x"}}`),
		"payload without id":   []byte(`{"event_id":"p5","change_type":1,"payload":{"owner":"nobody"}}`),
		"huge id-less payload": fmt.Appendf(nil, `{"event_id":"p6","change_type":1,"payload":{"owner":%q}}`, strings.Repeat("A", 1<<20)),
	}
	for name, msg := range poison {
		noPanic(t, name, func() {
			if err := accounts([]byte(name), msg); err != nil {
				t.Errorf("%s: handler returned %v; poison must be dropped, not bounced back to the broker", name, err)
			}
		})
	}
	valid := bdAccount{ID: "ok", Owner: "valid", Balance: 7, Version: 1, UpdatedAt: time.Now()}
	dbPublish(t, accounts, "valid-1", cqrs.ChangeTypeCreated, valid)
	b.awaitReader(`SELECT count(*) FROM bank_accounts WHERE id = 'ok'`, 1)
	time.Sleep(50 * time.Millisecond)
	if n := count(t, b.h.reader, `SELECT count(*) FROM bank_accounts`); n != 1 {
		t.Fatalf("reader has %d accounts; want only the valid one", n)
	}
}

// One message that the read model rejects (here: a negative balance violating its CHECK) must not
// make the runner drop the valid messages that happened to share its batch.
func TestBankDBPoison_OnePoisonMessageDoesNotSinkItsBatch(t *testing.T) {
	b := newBDBank(t, bdOpts{})
	accounts := b.events["bank_accounts"]
	now := time.Now()
	batch := []bdAccount{
		{ID: "a", Owner: "a", Balance: 1, Version: 1, UpdatedAt: now},
		{ID: "poison", Owner: "p", Balance: -5, Version: 1, UpdatedAt: now},
		{ID: "c", Owner: "c", Balance: 3, Version: 1, UpdatedAt: now},
	}
	for _, a := range batch { // pushed back to back so they land in one batch
		dbPublish(t, accounts, "e-"+a.ID, cqrs.ChangeTypeCreated, a)
	}
	b.awaitReader(`SELECT count(*) FROM bank_accounts WHERE id IN ('a', 'c')`, 2)
	if n := count(t, b.h.reader, `SELECT count(*) FROM bank_accounts WHERE id = 'poison'`); n != 0 {
		t.Fatal("the poison row reached the read model")
	}
}

func TestBankDBPoison_IntegerBoundaries(t *testing.T) {
	b := newBDBank(t, bdOpts{})
	ctx := withDeadline(t, 10*time.Second)

	t.Run("max int64 balance is stored exactly", func(t *testing.T) {
		b.open("max", math.MaxInt64)
		if got := b.writerBalance("max"); got != math.MaxInt64 {
			t.Fatalf("balance = %d; want MaxInt64", got)
		}
	})
	t.Run("overflow past int64 is rejected, not wrapped", func(t *testing.T) {
		_, err := b.accounts.IncrementByID(ctx, "max", "balance", 1)
		requireKind(t, err, database.ErrOutOfRange)
		if got := b.writerBalance("max"); got != math.MaxInt64 {
			t.Fatalf("balance changed to %d", got)
		}
	})
	t.Run("underflow below zero hits the CHECK", func(t *testing.T) {
		b.open("low", 5)
		_, err := b.accounts.IncrementByID(ctx, "low", "balance", -6)
		requireKind(t, err, database.ErrConstraint)
	})
	t.Run("increments above 2^53 stay exact", func(t *testing.T) {
		const big = 1<<53 + 1 // not representable as float64
		b.open("big", big)
		got, err := b.accounts.IncrementByID(ctx, "big", "balance", 1)
		if err != nil || got.Balance != big+1 || b.writerBalance("big") != big+1 {
			t.Fatalf("2^53+1 + 1 = %v, %v; want %d exactly (float64 rounding corrupts money)", got, err, int64(big+1))
		}
	})
	t.Run("amount that overflows the destination is rejected", func(t *testing.T) {
		b.open("src", 100)
		_, _, err := b.Transfer(ctx, transferReq{Key: "ovf", From: "src", To: "max", Amount: 10})
		if !errors.Is(err, errBankOverflow) {
			t.Fatalf("Transfer into an account at MaxInt64 = %v; want errBankOverflow (checked before int64 wraps)", err)
		}
		if b.writerBalance("src") != 100 || b.writerBalance("max") != math.MaxInt64 {
			t.Fatal("money moved on an overflowing transfer")
		}
	})
	t.Run("MinInt64 amount is rejected", func(t *testing.T) {
		_, _, err := b.Transfer(ctx, transferReq{Key: "min", From: "low", To: "big", Amount: math.MinInt64})
		if err == nil {
			t.Fatal("a MinInt64 transfer succeeded")
		}
		if got := b.writerBalance("low"); got != 5 {
			t.Fatalf("source balance = %d; want 5", got)
		}
	})
}

func TestBankDBPoison_InjectionAndSpecialCharactersAreInert(t *testing.T) {
	b := newBDBank(t, bdOpts{})
	ctx := withDeadline(t, 15*time.Second)
	owners := []string{
		`'; DROP TABLE bank_accounts; --`,
		`Robert'); DELETE FROM bank_audit; --`,
		`" OR 1=1 --`,
		`100% _real_ \ name`,
		`?TableAlias ?0 $1 :name`,
		`💸🏦 emoji`,
		`مرحبا بالعالم`,
		"é combining",
		strings.Repeat("x", 4096),
	}
	for i, o := range owners {
		if _, err := b.accounts.Create(ctx, bdAccount{ID: fmt.Sprintf("a%d", i), Owner: o, Balance: int64(i)}); err != nil {
			t.Fatalf("owner %q: %v", o, err)
		}
	}
	b.replicate()

	for i, o := range owners {
		got, err := b.accounts.Find(ctx, eqFilter("owner", o))
		if err != nil || len(got) != 1 || got[0].ID != fmt.Sprintf("a%d", i) || got[0].Owner != norm.NFC.String(o) {
			t.Errorf("exact lookup of %.40q = %v, %v; want the one account storing it (NFC-normalized)", o, got, err)
		}
	}
	contains := func(v string) int64 {
		n, err := b.accounts.Count(ctx, pagination.StructuredFilter{Filters: []pagination.Filter{{Field: "owner", Mode: pagination.ModeContains, Value: v}}})
		must(t, err)
		return n
	}
	if n := contains("%"); n != 1 {
		t.Errorf("contains %% matched %d rows; want 1 (wildcard must be literal)", n)
	}
	if n := contains("_real_"); n != 1 { // as a LIKE pattern _real_ would also match e.g. "areal1"
		t.Errorf("contains _real_ matched %d rows; want 1 (wildcard must be literal)", n)
	}
	for _, field := range []string{"owner; DROP TABLE bank_accounts", "owner) OR (1=1", `"owner"`} {
		_, err := b.accounts.Count(ctx, pagination.StructuredFilter{Filters: []pagination.Filter{{Field: field, Mode: pagination.ModeEqual, Value: "x"}}})
		if !errors.Is(err, pagination.ErrUnknownField) {
			t.Errorf("filter on field %q = %v; want ErrUnknownField", field, err)
		}
		_, err = b.accounts.Paginate(ctx, pagination.Pagination{Filter: pagination.StructuredFilter{SortFields: []pagination.SortField{{Field: field}}}})
		if !errors.Is(err, pagination.ErrUnknownField) {
			t.Errorf("sort on field %q = %v; want ErrUnknownField", field, err)
		}
	}
	if n := count(t, b.h.writer, `SELECT count(*) FROM bank_accounts`); n != int64(len(owners)) {
		t.Fatalf("writer has %d accounts; want %d (an injection executed)", n, len(owners))
	}
	if n := count(t, b.h.reader, `SELECT count(*) FROM information_schema.tables WHERE table_name LIKE 'bank_%'`); n != 3 {
		t.Fatalf("reader has %d bank tables; want 3", n)
	}
}

func TestBankDBPoison_InvalidUTF8AndNULBytesAreRejected(t *testing.T) {
	b := newBDBank(t, bdOpts{})
	ctx := withDeadline(t, 5*time.Second)
	for name, owner := range map[string]string{
		"invalid UTF-8":   "\xff\xfe\xfd",
		"truncated rune":  "abc\xe2\x82",
		"NUL byte":        "abc\x00def",
		"overlong encode": "\xc0\xaf",
	} {
		t.Run(name, func(t *testing.T) {
			noPanic(t, name, func() {
				_, err := b.accounts.Create(ctx, bdAccount{ID: name, Owner: owner, Balance: 1})
				requireKind(t, err, database.ErrInvalidEncoding)
			})
		})
	}
	if n := count(t, b.h.writer, `SELECT count(*) FROM bank_accounts`); n != 0 {
		t.Fatalf("%d rows stored from invalid text", n)
	}
}

func TestBankDBPoison_BinaryAndJSONBRoundTrip(t *testing.T) {
	b := newBDBank(t, bdOpts{})
	ctx := withDeadline(t, 10*time.Second)
	all := make([]byte, 256)
	for i := range all {
		all[i] = byte(i)
	}
	profile := map[string]any{
		"ключ":   "значение",
		"quote":  `he said "hi"\n`,
		"nested": map[string]any{"deep": []any{1.5, "two", nil, true, map[string]any{}}},
		"empty":  "",
	}
	if _, err := b.accounts.Create(ctx, bdAccount{ID: "bin", Owner: "bin", Balance: 1, Signature: all, Profile: profile}); err != nil {
		t.Fatal(err)
	}
	b.replicate() // bytea travels as base64 and JSONB as JSON through the CDC message

	got, err := b.accounts.GetByID(ctx, "bin")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Signature, all) {
		t.Errorf("signature bytes changed in transit: %x", got.Signature)
	}
	if !reflect.DeepEqual(got.Profile, profile) {
		t.Errorf("profile changed in transit:\n got  %#v\n want %#v", got.Profile, profile)
	}
}

func TestBankDBPoison_UnserializableJSONBFailsBeforeTheDatabase(t *testing.T) {
	b := newBDBank(t, bdOpts{})
	ctx := withDeadline(t, 5*time.Second)
	for name, profile := range map[string]map[string]any{
		"NaN":      {"rate": math.NaN()},
		"+Inf":     {"rate": math.Inf(1)},
		"channel":  {"ch": make(chan int)},
		"function": {"fn": func() {}},
	} {
		noPanic(t, name, func() {
			if _, err := b.accounts.Create(ctx, bdAccount{ID: "j", Owner: name, Balance: 1, Profile: profile}); err == nil {
				t.Errorf("%s in JSONB was accepted", name)
			}
		})
	}
	if n := count(t, b.h.writer, `SELECT count(*) FROM bank_accounts`); n != 0 {
		t.Fatalf("%d rows stored from unserializable JSONB", n)
	}
}
