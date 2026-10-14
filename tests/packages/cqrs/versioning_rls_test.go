//go:build integration

package cqrs_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
)

// ---- clock skew & timestamp collision on versioning ------------------------

func stampedRow(id int64, owner, ts string) string {
	return fmt.Sprintf(`{"id":%d,"owner":%q,"balance":1,"version":1,"updated_at":%q}`, id, owner, ts)
}

func stampedUpdate(offset, id int64, owner, ts string) cdc.Record {
	return event(0, offset, cdc.OpUpdate, id, "null", stampedRow(id, owner, ts))
}

// With a timestamp as the version: an older timestamp that arrives after a newer
// one (a skewed clock, a late redelivery) never overwrites it; equal timestamps
// (a collision) are won by the later change; instants a microsecond apart still
// order; and the same instant written in two time zones is the same version.
func TestVersioning_ClockSkewAndCollision(t *testing.T) {
	t.Parallel()
	p := newPair(t, 8)
	f := newFakeCDC("g-clock")
	svc := newTyped[Account](t, p, f, "updated_at")
	start(t, svc)
	ctx := ctxTimeout(t, time.Minute)
	owner := func() string { return p.readAccounts()[1].Owner }

	if err := f.deliver(ctx, []cdc.Record{stampedUpdate(0, 1, "t1", "2026-01-01T10:00:00.000001Z")}); err != nil {
		t.Fatal(err)
	}
	// A skewed older timestamp, in a later batch: rejected by the database guard.
	if err := f.deliver(ctx, []cdc.Record{stampedUpdate(1, 1, "skewed-old", "2026-01-01T10:00:00.000000Z")}); err != nil {
		t.Fatal(err)
	}
	if owner() != "t1" {
		t.Fatalf("an older timestamp overwrote a newer one: %q", owner())
	}
	// A collision: the same timestamp, later in the stream, wins.
	if err := f.deliver(ctx, []cdc.Record{stampedUpdate(2, 1, "collision", "2026-01-01T10:00:00.000001Z")}); err != nil {
		t.Fatal(err)
	}
	if owner() != "collision" {
		t.Fatalf("equal versions: the later change must win, got %q", owner())
	}
	// A microsecond later wins; the same instant in another zone is a collision, not newer.
	if err := f.deliver(ctx, []cdc.Record{stampedUpdate(3, 1, "t2", "2026-01-01T10:00:00.000002Z")}); err != nil {
		t.Fatal(err)
	}
	if err := f.deliver(ctx, []cdc.Record{stampedUpdate(4, 1, "same-instant", "2026-01-01T12:00:00.000002+02:00")}); err != nil {
		t.Fatal(err)
	}
	if owner() != "same-instant" {
		t.Fatalf("the same instant in another zone is an equal version: got %q", owner())
	}
	// Within one batch the same rules hold.
	if err := f.deliver(ctx, []cdc.Record{
		stampedUpdate(5, 1, "batch-new", "2026-02-01T00:00:00Z"),
		stampedUpdate(6, 1, "batch-late-old", "2026-01-15T00:00:00Z"),
	}); err != nil {
		t.Fatal(err)
	}
	if owner() != "batch-new" {
		t.Fatalf("inside a batch an older version must not win: %q", owner())
	}
	if s := svc.Stats(); s.DeadLettered != 0 {
		t.Fatalf("clock skew is not poison: %+v", s)
	}
}

// ---- null or missing version field with versioning enabled -----------------

// An event with no version, or a null one, is the oldest version there can be:
// it creates a row that does not exist yet, but never overwrites or deletes one
// that has a version. A stored NULL version is older than anything.
func TestVersioning_NullOrMissingVersion(t *testing.T) {
	t.Parallel()
	p := newPair(t, 8)
	f := newFakeCDC("g-nullver")
	svc := newService(t, p, f)
	start(t, svc)
	ctx := ctxTimeout(t, time.Minute)

	if err := f.deliver(ctx, []cdc.Record{create(0, 0, 1, "versioned", 10, 5)}); err != nil {
		t.Fatal(err)
	}
	noVersion := event(0, 1, cdc.OpUpdate, 1, "null", `{"id":1,"owner":"no-version","balance":99}`)
	nullVersion := event(0, 2, cdc.OpUpdate, 1, "null", `{"id":1,"owner":"null-version","balance":98,"version":null}`)
	if err := f.deliver(ctx, []cdc.Record{noVersion, nullVersion}); err != nil {
		t.Fatal(err)
	}
	if got := p.readAccounts()[1]; got.Owner != "versioned" || got.Balance != 10 || got.Version != 5 {
		t.Fatalf("a versionless change overwrote a versioned row: %+v", got)
	}
	// ...but it may create a row, and a versionless row is then replaced by any version.
	if err := f.deliver(ctx, []cdc.Record{event(0, 3, cdc.OpCreate, 2, "null", `{"id":2,"owner":"first","balance":1}`)}); err != nil {
		t.Fatal(err)
	}
	if err := f.deliver(ctx, []cdc.Record{update(0, 4, 2, "second", 2, 1)}); err != nil {
		t.Fatal(err)
	}
	if got := p.readAccounts()[2]; got.Owner != "second" {
		t.Fatalf("a version must replace a versionless row: %+v", got)
	}
	// A stored NULL (the read column allows it) is older than any version.
	p.exec(p.read, `ALTER TABLE accounts ALTER COLUMN version DROP NOT NULL`)
	p.exec(p.read, `UPDATE accounts SET version = NULL WHERE id = 2`)
	if err := f.deliver(ctx, []cdc.Record{update(0, 5, 2, "over-null", 3, 1)}); err != nil {
		t.Fatal(err)
	}
	if got := p.readAccounts()[2]; got.Owner != "over-null" {
		t.Fatalf("a stored NULL version must lose to any version: %+v", got)
	}
}

// A delete that names an older version than the stored row's does not remove
// it; one that names the current version, or none, does.
func TestVersioning_DeleteRespectsVersions(t *testing.T) {
	t.Parallel()
	p := newPair(t, 8)
	f := newFakeCDC("g-delver")
	start(t, newService(t, p, f))
	ctx := ctxTimeout(t, time.Minute)
	if err := f.deliver(ctx, []cdc.Record{create(0, 0, 1, "keep", 1, 5), create(0, 1, 2, "drop", 1, 5), create(0, 2, 3, "drop-unversioned", 1, 5)}); err != nil {
		t.Fatal(err)
	}
	del := func(offset, id int64, version int64) cdc.Record {
		return event(0, offset, cdc.OpDelete, id, accountJSON(id, "x", 1, version), "null")
	}
	if err := f.deliver(ctx, []cdc.Record{del(3, 1, 3), del(4, 2, 5), remove(0, 5, 3)}); err != nil {
		t.Fatal(err)
	}
	got := p.readAccounts()
	if _, ok := got[1]; !ok {
		t.Fatal("a delete of an older version removed newer data")
	}
	if _, ok := got[2]; ok {
		t.Fatal("a delete of the current version must remove the row")
	}
	if _, ok := got[3]; ok {
		t.Fatal("a delete that names no version must remove the row")
	}
}

// ---- RLS context execution -------------------------------------------------

// The read model mirrors every tenant, so with row-level security on, the
// replicator must be let through, and only inside the transaction that writes:
// the setting never lingers on a pooled connection.
func TestRLS_ReplicatorWritesThroughRowLevelSecurity(t *testing.T) {
	t.Parallel()
	skipUnlessPostgres(t)
	role := fmt.Sprintf("cqrs_rls_%d_%d", runID%1_000_000_000, seqNo.Add(1))
	if _, err := readAdmin.Exec(fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD 'pw'`, role)); err != nil {
		t.Fatal(err)
	}
	// Registered before the pair's own cleanup, so it runs after the database is gone.
	t.Cleanup(func() { _, _ = readAdmin.Exec(fmt.Sprintf(`DROP ROLE IF EXISTS %q`, role)) })

	p := newPair(t, 8)
	p.exec(p.read, `ALTER TABLE accounts ENABLE ROW LEVEL SECURITY`)
	p.exec(p.read, `ALTER TABLE accounts FORCE ROW LEVEL SECURITY`)
	p.exec(p.read, `CREATE POLICY replicator_only ON accounts
		USING (current_setting('app.replicator', true) = 'on')
		WITH CHECK (current_setting('app.replicator', true) = 'on')`)
	p.exec(p.read, fmt.Sprintf(`GRANT ALL ON ALL TABLES IN SCHEMA public TO %q`, role))

	srv := p.readSrv
	srv.user, srv.password = role, "pw"
	roleSvc := startSQL(t, srv.dsn(p.readDB), 4)
	rp := *p
	rp.read = roleSvc

	f := newFakeCDC("g-rls")
	start(t, newService(t, &rp, f))
	if err := f.deliver(ctxTimeout(t, time.Minute), seq(0, 0, 1, 25)); err != nil {
		t.Fatal(err)
	}
	if n := p.count(p.read, "accounts"); n != 25 {
		t.Fatalf("rows = %d: the replicator was not let through", n)
	}

	// RLS really is on: the same role, outside the replicator transaction, sees nothing.
	var visible int
	if err := roleSvc.Client().NewRaw(`SELECT count(*) FROM accounts`).Scan(context.Background(), &visible); err != nil {
		t.Fatal(err)
	}
	if visible != 0 {
		t.Fatalf("the role sees %d rows without being the replicator: RLS is not enforced", visible)
	}
	if _, err := roleSvc.Client().ExecContext(context.Background(),
		`INSERT INTO accounts (id, owner) VALUES (999, 'intruder')`); err == nil {
		t.Fatal("a plain write by the role must be refused by row-level security")
	}
	// And the replicator flag does not stay behind on any pooled connection.
	for range 12 {
		var v sql.NullString
		if err := roleSvc.Client().NewRaw(`SELECT current_setting('app.replicator', true)`).Scan(context.Background(), &v); err != nil {
			t.Fatal(err)
		}
		if v.Valid && v.String == "on" {
			t.Fatal("app.replicator leaked onto a pooled connection")
		}
	}
}
