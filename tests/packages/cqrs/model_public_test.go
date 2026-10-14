//go:build integration

package cqrs_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cqrs"
)

// What a model may look like, and how keys and versions behave, tested through the
// public constructor and a running service. A model the replicator cannot handle
// must be refused when the service is built, not on its first change.

type (
	noIDModel struct {
		bun.BaseModel `bun:"table:no_ids"`
		Name          string `bun:"name"`
	}
	compositeModel struct {
		bun.BaseModel `bun:"table:composites"`
		A             int64 `bun:"a,pk"`
		B             int64 `bun:"b,pk"`
	}
	naiveTimeModel struct {
		bun.BaseModel `bun:"table:naive"`
		ID            int64     `bun:"id,pk"`
		At            time.Time `bun:"at,type:timestamp"`
	}
	badVersionModel struct {
		bun.BaseModel `bun:"table:bad_versions"`
		ID            int64  `bun:"id,pk"`
		V             string `bun:"v"`
	}
	arrayModel struct {
		bun.BaseModel `bun:"table:arrays"`
		ID            int64    `bun:"id,pk"`
		Tags          []string `bun:"tags,array"`
	}
	intervalModel struct {
		bun.BaseModel `bun:"table:intervals"`
		ID            int64  `bun:"id,pk"`
		Every         string `bun:"every,type:interval"`
	}
	clockModel struct {
		bun.BaseModel `bun:"table:clocks"`
		ID            int64     `bun:"id,pk"`
		At            time.Time `bun:"at,type:time"`
	}
	secondKeyModel struct {
		bun.BaseModel `bun:"table:second_keys"`
		Code          string `bun:"code,pk"`
		N             int64  `bun:"n"`
	}
	stringKeyModel struct {
		bun.BaseModel `bun:"table:string_keys"`
		ID            string `bun:"id,pk"`
		N             int64  `bun:"n"`
	}
	onlyKeyModel struct {
		bun.BaseModel `bun:"table:only_keys"`
		ID            int64 `bun:"id,pk"`
	}
	schemaModel struct {
		bun.BaseModel `bun:"table:billing.invoices"`
		ID            int64 `bun:"id,pk"`
	}
	intKeyModel struct {
		bun.BaseModel `bun:"table:int_keys"`
		ID            int32 `bun:"id,pk"`
	}
	ptrKeyModel struct {
		bun.BaseModel `bun:"table:ptr_keys"`
		ID            *int64 `bun:"id,pk"`
	}
	timeVersionModel struct {
		bun.BaseModel `bun:"table:tv"`
		ID            int64      `bun:"id,pk"`
		V             *time.Time `bun:"v,type:timestamptz"`
	}
)

// refuses reports whether building a service for T panics, and what it says.
func refuses[T any](t *testing.T, p *pair, col, version string) (msg string, panicked bool) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			msg, panicked = fmt.Sprint(r), true
		}
	}()
	f := newFakeCDC("g-model")
	newTyped[T](t, p, f, version, func(c *cqrs.CQRSService[T, AccountResponse, struct{}, int64]) {
		c.ColumnDefaultID = col
	})
	return "", false
}

func TestModelPublic_UnusableModelsAreRefusedAtConstruction(t *testing.T) {
	t.Parallel()
	p := newPair(t, 4)
	cases := []struct {
		name    string
		refuse  func() (string, bool)
		mention string // a word the message must contain
	}{
		{"no id column", func() (string, bool) { return refuses[noIDModel](t, p, "id", "") }, "key"},
		{"composite primary key", func() (string, bool) { return refuses[compositeModel](t, p, "a", "") }, "composite"},
		{"timestamp without time zone", func() (string, bool) { return refuses[naiveTimeModel](t, p, "id", "") }, "timestamptz"},
		{"time of day", func() (string, bool) { return refuses[clockModel](t, p, "id", "") }, "timestamptz"},
		{"interval", func() (string, bool) { return refuses[intervalModel](t, p, "id", "") }, "timestamptz"},
		{"array column", func() (string, bool) { return refuses[arrayModel](t, p, "id", "") }, "array"},
		{"a string version column", func() (string, bool) { return refuses[badVersionModel](t, p, "id", "v") }, "version"},
		{"a missing version column", func() (string, bool) { return refuses[Account](t, p, "id", "nope") }, "version"},
		{"a non-key column as the key", func() (string, bool) { return refuses[secondKeyModel](t, p, "n", "") }, "keyed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msg, panicked := tc.refuse()
			if !panicked {
				t.Fatal("NewCQRS accepted a model replication cannot handle")
			}
			if !strings.Contains(strings.ToLower(msg), tc.mention) {
				t.Fatalf("the message should mention %q so the fix is obvious: %q", tc.mention, msg)
			}
		})
	}
}

func TestModelPublic_UsableShapesAreAccepted(t *testing.T) {
	t.Parallel()
	p := newPair(t, 4)
	cases := map[string]func() (string, bool){
		"key column named differently": func() (string, bool) { return refuses[secondKeyModel](t, p, "code", "") },
		"string key":                   func() (string, bool) { return refuses[stringKeyModel](t, p, "id", "") },
		"int32 key":                    func() (string, bool) { return refuses[intKeyModel](t, p, "id", "") },
		"pointer key":                  func() (string, bool) { return refuses[ptrKeyModel](t, p, "id", "") },
		"a model with only a key":      func() (string, bool) { return refuses[onlyKeyModel](t, p, "id", "") },
		"schema in the table tag":      func() (string, bool) { return refuses[schemaModel](t, p, "id", "") },
		"a time version behind a pointer": func() (string, bool) {
			return refuses[timeVersionModel](t, p, "id", "v")
		},
		"an integer version": func() (string, bool) { return refuses[Account](t, p, "id", "version") },
		"no version at all":  func() (string, bool) { return refuses[Account](t, p, "id", "") },
	}
	for name, try := range cases {
		t.Run(name, func(t *testing.T) {
			if msg, panicked := try(); panicked {
				t.Fatalf("a usable model was refused: %s", msg)
			}
		})
	}
}

// A model without a version column replicates by stream order alone.
func TestModelPublic_NoVersionColumnStillReplicates(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	// Plain is the accounts table without the version column.
	f := newFakeCDC("g-plain")
	svc := newTyped[Plain](t, r.p, f, "")
	start(t, svc)
	if err := f.deliver(ctxTimeout(t, time.Minute), []cdc.Record{
		create(0, 0, 1, "a", 5, 1), update(0, 1, 1, "b", 6, 2),
	}); err != nil {
		t.Fatal(err)
	}
	got := r.p.readAccounts()[1]
	if got.Owner != "b" || got.Balance != 6 {
		t.Fatalf("stream order must win without a version: %+v", got)
	}
}

// A model with only a key has nothing to update: a repeated create must not fail.
func TestModelPublic_KeyOnlyTableUpsertsWithoutError(t *testing.T) {
	t.Parallel()
	p := newPair(t, 4)
	p.exec(p.write, `CREATE TABLE only_keys (id bigint PRIMARY KEY)`)
	p.exec(p.write, `ALTER TABLE only_keys REPLICA IDENTITY FULL`)
	p.exec(p.read, `CREATE TABLE only_keys (id bigint PRIMARY KEY)`)
	f := newFakeCDC("g-onlykeys")
	svc := newTyped[onlyKeyModel](t, p, f, "", func(c *cqrs.CQRSService[onlyKeyModel, AccountResponse, struct{}, int64]) {
		c.Topic = "test.public.only_keys"
	})
	start(t, svc)
	mk := func(off int64) cdc.Record {
		ev := &cdc.ChangeEvent{Op: cdc.OpCreate, Before: []byte(`null`), After: []byte(`{"id":5}`)}
		ev.Source.Table = "only_keys"
		return cdc.Record{Topic: "test.public.only_keys", Offset: off, Key: []byte(`{"id":5}`), Event: ev,
			Timestamp: baseTime.Add(time.Duration(off) * time.Millisecond)}
	}
	if err := f.deliver(ctxTimeout(t, time.Minute), []cdc.Record{mk(0), mk(1)}); err != nil {
		t.Fatal(err)
	}
	if n := p.count(p.read, "only_keys"); n != 1 {
		t.Fatalf("rows = %d, want 1", n)
	}
	if len(f.deadLettered()) != 0 {
		t.Fatal("a key-only upsert is not poison")
	}
}

// Keys: a zero key is refused as poison, never written as a row with an empty key.
func TestModelPublic_ZeroKeyIsPoisonNotARow(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	if err := r.f.deliver(ctxTimeout(t, time.Minute), []cdc.Record{
		event(0, 0, cdc.OpCreate, 0, "null", accountJSON(0, "zero", 1, 1)),
		create(0, 1, 7, "fine", 1, 1),
	}); err != nil {
		t.Fatal(err)
	}
	if _, bad := r.p.readAccounts()[0]; bad {
		t.Fatal("a row with a zero key was written")
	}
	if len(r.f.deadLettered()) != 1 {
		t.Fatalf("the zero-key record must be parked, parked %d", len(r.f.deadLettered()))
	}
	if r.p.readAccounts()[7].Owner != "fine" {
		t.Fatal("the record behind the poison must still be applied")
	}
}

// Versions: the guard decides by the version, whatever the stream order.
func TestModelPublic_VersionGuardTurnsAwayTheOlderChange(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	ctx := ctxTimeout(t, time.Minute)
	if err := r.f.deliver(ctx, []cdc.Record{create(0, 0, 1, "v5", 50, 5)}); err != nil {
		t.Fatal(err)
	}
	// An older version arrives later (a replay): it must not overwrite.
	if err := r.f.deliver(ctx, []cdc.Record{update(0, 1, 1, "v2", 20, 2)}); err != nil {
		t.Fatal(err)
	}
	if got := r.p.readAccounts()[1]; got.Owner != "v5" || got.Balance != 50 {
		t.Fatalf("an older version overwrote a newer one: %+v", got)
	}
	// An equal version: the later change of the stream wins.
	if err := r.f.deliver(ctx, []cdc.Record{update(0, 2, 1, "v5-again", 51, 5)}); err != nil {
		t.Fatal(err)
	}
	if got := r.p.readAccounts()[1]; got.Owner != "v5-again" {
		t.Fatalf("equal versions: the later change must win: %+v", got)
	}
	if r.stats().Stale == 0 {
		t.Fatal("the turned-away change must be counted")
	}
}

// A delete that names an older version than the stored row does not remove it.
func TestModelPublic_OlderDeleteDoesNotRemoveANewerRow(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	ctx := ctxTimeout(t, time.Minute)
	if err := r.f.deliver(ctx, []cdc.Record{create(0, 0, 1, "v5", 50, 5)}); err != nil {
		t.Fatal(err)
	}
	old := event(0, 1, cdc.OpDelete, 1, accountJSON(1, "v2", 20, 2), "null")
	if err := r.f.deliver(ctx, []cdc.Record{old}); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.p.readAccounts()[1]; !ok {
		t.Fatal("a delete of an older version removed a newer row")
	}
	// A key-only delete is ordered by the stream alone and does remove it.
	if err := r.f.deliver(ctx, []cdc.Record{remove(0, 2, 1)}); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.p.readAccounts()[1]; ok {
		t.Fatal("a key-only delete must remove the row")
	}
}

// Time versions: two timestamps a microsecond apart still order, and a zero time is
// the oldest version.
func TestModelPublic_TimeVersionOrdersByMicrosecond(t *testing.T) {
	t.Parallel()
	p := newPair(t, 4)
	f := newFakeCDC("g-timever")
	svc := newTyped[Account](t, p, f, "updated_at")
	start(t, svc)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	at := func(d time.Duration) string { return base.Add(d).Format("2006-01-02T15:04:05.000000Z") }
	row := func(owner string, d time.Duration) string {
		return fmt.Sprintf(`{"id":1,"owner":%q,"balance":1,"version":1,"updated_at":%q}`, owner, at(d))
	}
	mk := func(off int64, owner string, d time.Duration) cdc.Record {
		return event(0, off, cdc.OpUpdate, 1, "null", row(owner, d))
	}
	ctx := ctxTimeout(t, time.Minute)
	if err := f.deliver(ctx, []cdc.Record{mk(0, "newer", time.Microsecond)}); err != nil {
		t.Fatal(err)
	}
	if err := f.deliver(ctx, []cdc.Record{mk(1, "older", 0)}); err != nil {
		t.Fatal(err)
	}
	if got := p.readAccounts()[1].Owner; got != "newer" {
		t.Fatalf("a change one microsecond older overwrote the newer one: %q", got)
	}
}

// Skipping: every position a checkpoint can stand at, against batches with gaps in
// their offsets (compaction and other transactions leave gaps).
func TestModelPublic_ReplayedPrefixIsSkippedWhateverTheGaps(t *testing.T) {
	t.Parallel()
	for _, gap := range []int64{1, 2, 5} {
		t.Run(fmt.Sprintf("gap=%d", gap), func(t *testing.T) {
			r := newRig(t, 1)
			ctx := ctxTimeout(t, time.Minute)
			var batch []cdc.Record
			for i := range 6 {
				batch = append(batch, create(0, 10+int64(i)*gap, int64(i+1), "first", 1, 1))
			}
			if err := r.f.deliver(ctx, batch); err != nil {
				t.Fatal(err)
			}
			// Replay a window that starts inside what was applied and runs past it.
			var again []cdc.Record
			for i := 3; i < 9; i++ {
				again = append(again, update(0, 10+int64(i)*gap, int64(i+1), "second", 2, 2))
			}
			if err := r.f.call(ctx, again); err != nil {
				t.Fatal(err)
			}
			got := r.p.readAccounts()
			for i := 1; i <= 3; i++ {
				if got[int64(i)].Owner != "first" {
					t.Fatalf("row %d was rewritten by a replay the checkpoint covered: %+v", i, got[int64(i)])
				}
			}
			for i := 4; i <= 6; i++ {
				if got[int64(i)].Owner != "first" && got[int64(i)].Owner != "second" {
					t.Fatalf("row %d = %+v", i, got[int64(i)])
				}
			}
			if r.stats().Duplicates == 0 {
				t.Fatal("covered records must be counted as duplicates")
			}
		})
	}
}

// Error text for refusals never quotes data: a panic message from construction is
// about the model's shape, not any value.
func TestModelPublic_ConstructionMessagesDoNotQuoteValues(t *testing.T) {
	t.Parallel()
	p := newPair(t, 4)
	msg, panicked := refuses[badVersionModel](t, p, "id", "v")
	if !panicked || strings.Contains(msg, context.Canceled.Error()) {
		t.Fatalf("unexpected: %v %q", panicked, msg)
	}
}
