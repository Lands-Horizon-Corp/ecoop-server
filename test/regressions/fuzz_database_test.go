package regressions

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"unicode/utf8"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/cqrs"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/pagination"
	"golang.org/x/text/unicode/norm"
)

// Fuzz targets for every input the database packages take from outside: CDC messages, filter
// values, cursors and text. The seed corpora run as ordinary tests; `make fuzz` explores further.
// Each target's property: never panic, and either a typed error or a correct result.

func FuzzDebeziumDecode(f *testing.F) {
	for _, seed := range []string{
		`{"before":null,"after":{"id":"a","owner":"Ann","balance":5,"version":1,"profile":"{\"k\":1}","signature":"AAE=","updated_at":"2026-01-02T03:04:05Z"},"source":{"schema":"public","table":"bank_accounts","lsn":10,"txId":2},"op":"c"}`,
		`{"before":{"id":"a"},"after":null,"source":{"schema":"public","table":"bank_accounts","lsn":11,"txId":3},"op":"d"}`,
		`{"schema":{},"payload":{"before":null,"after":{"id":"b","closed_at":1767225600000000,"updated_at":1767225600000},"source":{"lsn":12},"op":"r"}}`,
		`{"before":null,"after":{"id":"c","balance":"x"},"source":{},"op":"u"}`,
		`{"op":"t","source":{}}`,
		`{"op":"c","after":{"profile":"not json"}}`,
		`{"op":"c","after":{"balance":1e400}}`,
	} {
		f.Add([]byte(`{"id":"a"}`), []byte(seed))
	}
	f.Fuzz(func(t *testing.T, key, value []byte) {
		got, err := cqrs.DecodeChange[bdAccount](key, value)
		if err != nil {
			if !errors.Is(err, cqrs.ErrMalformedMessage) && !errors.Is(err, cqrs.ErrUnsupportedChange) {
				t.Fatalf("untyped decode error %v for %q", err, value)
			}
			return
		}
		again, err := cqrs.DecodeChange[bdAccount](key, value)
		if err != nil || again.EventID != got.EventID {
			t.Fatalf("decoding is not deterministic: %q then %q (%v)", got.EventID, again.EventID, err)
		}
		if got.EventID == "" {
			t.Fatalf("decoded message without an event id: %q", value)
		}
	})
}

func FuzzRunnerPayload(f *testing.F) {
	for _, seed := range []string{
		`{"event_id":"e1","change_type":1,"payload":{"id":"a","owner":"Ann","balance":1}}`,
		`{"event_id":"e2","change_type":3,"payload":{"id":"a"}}`,
		`{"change_type":1,"payload":null}`,
		`{"event_id":"","change_type":"x"}`,
		`[]`, `null`, `{`, "\x00\xff",
	} {
		f.Add([]byte("k"), []byte(seed))
	}
	f.Fuzz(func(t *testing.T, key, value []byte) {
		got, err := cqrs.DecodeChange[bdAccount](key, value)
		if err == nil && got.EventID == "" {
			t.Fatalf("accepted a message with no event id: key %q value %q", key, value)
		}
	})
}

func FuzzFilterValue(f *testing.F) {
	b := fuzzBank(f)
	b.open("a", 100)
	for _, s := range []struct{ field, mode, value string }{
		{"owner", "equal", "x"}, {"owner", "contains", "%_\\"}, {"balance", "gte", "1e309"},
		{"balance", "equal", "'; DROP TABLE bank_accounts; --"}, {"id; DROP", "equal", "x"},
		{"updated_at", "after", "2026-13-45"}, {"owner", "search", "x"}, {"owner", "nope", ""},
	} {
		f.Add(s.field, s.mode, s.value)
	}
	f.Fuzz(func(t *testing.T, field, mode, value string) {
		ctx := withDeadline(t, 10e9)
		_, err := b.accounts.Count(ctx, pagination.StructuredFilter{Filters: []pagination.Filter{{Field: field, Mode: pagination.Mode(mode), Value: value}}})
		if err != nil && errors.Is(database.MapError(err), database.ErrInternal) && !isExpectedFilterError(err) {
			t.Fatalf("filter %q %q %q failed with an unclassified error: %v", field, mode, value, err)
		}
		if n := count(t, b.h.writer, `SELECT count(*) FROM bank_accounts`); n != 1 {
			t.Fatalf("a filter changed data: %d accounts", n)
		}
	})
}

// isExpectedFilterError covers rejections that are the caller's fault but are not Postgres errors.
func isExpectedFilterError(err error) bool {
	return errors.Is(err, pagination.ErrInvalidFilter) || errors.Is(err, pagination.ErrUnknownField) ||
		strings.Contains(err.Error(), "@@@") || strings.Contains(err.Error(), "operator does not exist")
}

func FuzzCursorDecode(f *testing.F) {
	b := fuzzBank(f)
	for _, s := range []string{"", "x", "eyJ2IjpbXX0%3D", "eyJ2IjpbIjEiLCIyIl19", "%%%", "eyJ2IjpbIjIwMjYtMDEtMDFUMDA6MDA6MDBaIiwiYSdPUiAnMSc9JzEiXX0="} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, cursor string) {
		ctx := withDeadline(t, 10e9)
		_, err := b.accounts.Paginate(ctx, pagination.Pagination{PageSize: 5, Cursor: &cursor})
		if err != nil && !errors.Is(err, pagination.ErrInvalidCursor) && errors.Is(database.MapError(err), database.ErrInternal) {
			t.Fatalf("cursor %q failed with an unclassified error: %v", cursor, err)
		}
	})
}

func FuzzCheckText(f *testing.F) {
	b := fuzzBank(f)
	var seq atomic.Int64
	for _, s := range []string{"Ann", "", "é", "💸", "\xff", "a\x00b", strings.Repeat("x", 300), "مرحبا"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, owner string) {
		ctx := withDeadline(t, 10e9)
		id := fmt.Sprintf("fz%d", seq.Add(1))
		_, err := b.accounts.Create(ctx, bdAccount{ID: id, Owner: owner, Balance: 1})
		storable := utf8.ValidString(owner) && !strings.ContainsRune(owner, 0)
		safe := storable && !strings.ContainsFunc(owner, func(r rune) bool {
			return r == utf8.RuneError || (r < 0x20 && r != '\t' && r != '\n' && r != '\r') || (r >= 0x7f && r <= 0x9f) ||
				(r >= 0x202a && r <= 0x202e) || (r >= 0x2066 && r <= 0x2069)
		})
		switch {
		case !storable:
			requireKind(t, err, database.ErrInvalidEncoding)
		case !safe:
			requireKind(t, err, database.ErrInvalidInput)
		case err != nil && errors.Is(database.MapError(err), database.ErrDuplicate):
			// the same owner text (after normalization) was generated before
		case err != nil:
			t.Fatalf("valid text %q rejected: %v", owner, err)
		default:
			var stored string
			must(t, b.h.writer.QueryRow(`SELECT owner FROM bank_accounts WHERE id = $1`, id).Scan(&stored))
			if want := norm.NFC.String(owner); stored != want {
				t.Fatalf("stored %q; want the NFC form %q", stored, want)
			}
		}
	})
}

// fuzzBank builds a bank for a database-backed fuzz target, then returns to the package directory
// (the harness works in a temp dir) so the fuzzer saves failing inputs under testdata/fuzz.
func fuzzBank(f *testing.F) *bdLedger {
	f.Helper()
	wd, err := os.Getwd()
	must(f, err)
	b := newBDBank(f, bdOpts{noRun: true, maxOpen: 4})
	must(f, os.Chdir(wd))
	return b
}
