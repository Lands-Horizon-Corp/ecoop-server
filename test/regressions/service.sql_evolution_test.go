package regressions

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/uptrace/bun"
)

// Long-running schema evolution: one cooperative-banking schema changes across twelve releases, mixing
// models Diff can generate (columns, types, foreign keys, unique constraints, drops, renames) with
// hand-written migrations for what it cannot express (indexes, CHECKs, data backfills, seeds, string
// defaults, CONCURRENTLY). Real rows are present from release 1, so every step proves data survives.

// ---- models, one struct per release of a table ------------------------------

type evBranch struct {
	bun.BaseModel `bun:"table:branches"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Code          string `bun:"code,notnull,unique"`
	Name          string `bun:"name,notnull"`
}

type evStatus struct {
	bun.BaseModel `bun:"table:member_statuses"`
	Code          int32  `bun:"code,pk"`
	Label         string `bun:"label,notnull,unique"`
}

type evMemberV1 struct {
	bun.BaseModel `bun:"table:members"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Name          string `bun:"name,notnull,type:varchar(50)"`
	Email         string `bun:"email,notnull"`
}

type evMemberV2 struct { // joins a branch (nullable until backfilled)
	bun.BaseModel `bun:"table:members"`
	ID            int64     `bun:"id,pk,autoincrement"`
	Name          string    `bun:"name,notnull,type:varchar(50)"`
	Email         string    `bun:"email,notnull"`
	BranchID      int64     `bun:"branch_id"`
	Branch        *evBranch `bun:"rel:belongs-to,join:branch_id=id"`
}

type evMemberV3 struct { // phone, a status code and loyalty points with integer defaults
	bun.BaseModel `bun:"table:members"`
	ID            int64     `bun:"id,pk,autoincrement"`
	Name          string    `bun:"name,notnull,type:varchar(50)"`
	Email         string    `bun:"email,notnull"`
	BranchID      int64     `bun:"branch_id"`
	Branch        *evBranch `bun:"rel:belongs-to,join:branch_id=id"`
	Phone         string    `bun:"phone"`
	Status        int32     `bun:"status,notnull,default:1"`
	Points        int64     `bun:"points,notnull,default:0"`
}

type evMemberV4 struct { // status becomes a foreign key to the lookup table
	bun.BaseModel `bun:"table:members"`
	ID            int64     `bun:"id,pk,autoincrement"`
	Name          string    `bun:"name,notnull,type:varchar(50)"`
	Email         string    `bun:"email,notnull"`
	BranchID      int64     `bun:"branch_id"`
	Branch        *evBranch `bun:"rel:belongs-to,join:branch_id=id"`
	Phone         string    `bun:"phone"`
	Status        int32     `bun:"status,notnull,default:1"`
	StatusRef     *evStatus `bun:"rel:belongs-to,join:status=code"`
	Points        int64     `bun:"points,notnull,default:0"`
}

type evMemberV5 struct { // name widened to 200
	bun.BaseModel `bun:"table:members"`
	ID            int64     `bun:"id,pk,autoincrement"`
	Name          string    `bun:"name,notnull,type:varchar(200)"`
	Email         string    `bun:"email,notnull"`
	BranchID      int64     `bun:"branch_id"`
	Branch        *evBranch `bun:"rel:belongs-to,join:branch_id=id"`
	Phone         string    `bun:"phone"`
	Status        int32     `bun:"status,notnull,default:1"`
	StatusRef     *evStatus `bun:"rel:belongs-to,join:status=code"`
	Points        int64     `bun:"points,notnull,default:0"`
}

type evMemberV6 struct { // name renamed to full_name
	bun.BaseModel `bun:"table:members"`
	ID            int64     `bun:"id,pk,autoincrement"`
	FullName      string    `bun:"full_name,notnull,type:varchar(200)"`
	Email         string    `bun:"email,notnull"`
	BranchID      int64     `bun:"branch_id"`
	Branch        *evBranch `bun:"rel:belongs-to,join:branch_id=id"`
	Phone         string    `bun:"phone"`
	Status        int32     `bun:"status,notnull,default:1"`
	StatusRef     *evStatus `bun:"rel:belongs-to,join:status=code"`
	Points        int64     `bun:"points,notnull,default:0"`
}

type evMemberV7 struct { // email becomes unique
	bun.BaseModel `bun:"table:members"`
	ID            int64     `bun:"id,pk,autoincrement"`
	FullName      string    `bun:"full_name,notnull,type:varchar(200)"`
	Email         string    `bun:"email,notnull,unique"`
	BranchID      int64     `bun:"branch_id"`
	Branch        *evBranch `bun:"rel:belongs-to,join:branch_id=id"`
	Phone         string    `bun:"phone"`
	Status        int32     `bun:"status,notnull,default:1"`
	StatusRef     *evStatus `bun:"rel:belongs-to,join:status=code"`
	Points        int64     `bun:"points,notnull,default:0"`
}

type evMemberV8 struct { // phone dropped
	bun.BaseModel `bun:"table:members"`
	ID            int64     `bun:"id,pk,autoincrement"`
	FullName      string    `bun:"full_name,notnull,type:varchar(200)"`
	Email         string    `bun:"email,notnull,unique"`
	BranchID      int64     `bun:"branch_id"`
	Branch        *evBranch `bun:"rel:belongs-to,join:branch_id=id"`
	Status        int32     `bun:"status,notnull,default:1"`
	StatusRef     *evStatus `bun:"rel:belongs-to,join:status=code"`
	Points        int64     `bun:"points,notnull,default:0"`
}

type evMemberV9 struct { // a tier whose string default was added by a hand-written migration
	bun.BaseModel `bun:"table:members"`
	ID            int64     `bun:"id,pk,autoincrement"`
	FullName      string    `bun:"full_name,notnull,type:varchar(200)"`
	Email         string    `bun:"email,notnull,unique"`
	BranchID      int64     `bun:"branch_id"`
	Branch        *evBranch `bun:"rel:belongs-to,join:branch_id=id"`
	Status        int32     `bun:"status,notnull,default:1"`
	StatusRef     *evStatus `bun:"rel:belongs-to,join:status=code"`
	Points        int64     `bun:"points,notnull,default:0"`
	Tier          string    `bun:"tier,notnull,default:'basic'"`
}

// Relations from other tables point at the newest members struct; only its table name and id matter.
type evMemberHead = evMemberV9

type evLoan struct {
	bun.BaseModel `bun:"table:loans"`
	ID            int64         `bun:"id,pk,autoincrement"`
	MemberID      int64         `bun:"member_id,notnull,unique:member_number"`
	Number        string        `bun:"number,notnull,unique:member_number"`
	Principal     int64         `bun:"principal,notnull"`
	Status        int32         `bun:"status,notnull,default:0"`
	Member        *evMemberHead `bun:"rel:belongs-to,join:member_id=id"`
}

type evNote struct { // short-lived: added in release 9, dropped in release 10
	bun.BaseModel `bun:"table:member_notes"`
	ID            int64         `bun:"id,pk,autoincrement"`
	MemberID      int64         `bun:"member_id,notnull"`
	Note          string        `bun:"note,notnull"`
	Member        *evMemberHead `bun:"rel:belongs-to,join:member_id=id"`
}

type evAudit struct {
	bun.BaseModel `bun:"table:audit_events"`
	ID            int64         `bun:"id,pk,autoincrement"`
	MemberID      int64         `bun:"member_id"`
	Kind          string        `bun:"kind,notnull"`
	Payload       string        `bun:"payload,type:jsonb"`
	CreatedAt     time.Time     `bun:"created_at,notnull,default:current_timestamp"`
	Member        *evMemberHead `bun:"rel:belongs-to,join:member_id=id"`
}

// ---- harness ----------------------------------------------------------------

// evolution applies releases to one database and remembers what the schema looked like after each migration.
type evolution struct {
	t     *testing.T
	e     *sqlEnv
	order []int64          // applied versions, oldest first
	snaps map[int64]string // version -> fingerprint right after it was applied
}

func (v *evolution) record() {
	v.t.Helper()
	ver := v.e.appliedVersion()
	v.order = append(v.order, ver)
	v.snaps[ver] = fingerprint(v.e)
}

// generate lets Diff write however many migrations the model change needs, then proves the models are satisfied.
func (v *evolution) generate(name string, models ...any) {
	v.t.Helper()
	settleModels(v.t, v.e, name, models, v.record)
	requireConverged(v.t, v.e, models...)
}

// hand applies a hand-written migration.
func (v *evolution) hand(name, up, down string) {
	v.t.Helper()
	v.e.writeNext(name, up, down)
	v.applyHand(name)
}

// handNoTx applies a hand-written migration that must run outside a transaction.
func (v *evolution) handNoTx(name, up, down string) {
	v.t.Helper()
	v.e.writeNextBody(name, "-- +goose NO TRANSACTION\n"+gooseBody(up, down))
	v.applyHand(name)
}

func (v *evolution) applyHand(name string) {
	v.t.Helper()
	if err := v.e.migrate(); err != nil {
		v.t.Fatalf("applying hand-written migration %s: %v", name, err)
	}
	v.record()
}

// fingerprint covers what Diff does not show: columns, indexes and constraints, but never row data.
func fingerprint(e *sqlEnv) string {
	e.t.Helper()
	return e.schema() +
		"\n--- indexes\n" + listing(e, `SELECT indexname || ' ' || indexdef FROM pg_indexes
			WHERE schemaname = 'public' AND tablename <> 'goose_db_version' ORDER BY 1`) +
		"\n--- constraints\n" + listing(e, `SELECT conrelid::regclass::text || ' ' || conname || ' ' || pg_get_constraintdef(oid)
			FROM pg_constraint WHERE connamespace = 'public'::regnamespace AND conrelid::regclass::text <> 'goose_db_version' ORDER BY 1`)
}

func listing(e *sqlEnv, query string) string {
	e.t.Helper()
	rows, err := e.db.Query(query)
	if err != nil {
		e.t.Fatalf("query %q: %v", query, err)
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			e.t.Fatal(err)
		}
		lines = append(lines, line)
	}
	if err := rows.Err(); err != nil {
		e.t.Fatal(err)
	}
	return strings.Join(lines, "\n")
}

func mustFail(t *testing.T, e *sqlEnv, what, query string, args ...any) {
	t.Helper()
	if _, err := e.db.Exec(query, args...); err == nil {
		t.Fatalf("%s: the database accepted %q", what, query)
	}
}

func requireIndexes(t *testing.T, e *sqlEnv, names ...string) {
	t.Helper()
	for _, name := range names {
		if e.scanInt(`SELECT count(*) FROM pg_indexes WHERE schemaname = 'public' AND indexname = $1`, name) != 1 {
			t.Fatalf("index %s is missing", name)
		}
	}
}

// buildReleases walks a development database through twelve releases with data in place and returns the
// history plus the migration count and version right after release 3, the "production is still here" point.
func buildReleases(t *testing.T) (dev *evolution, lagFiles int, lagVersion int64) {
	t.Helper()
	e := newSQLEnv(t)
	dev = &evolution{t: t, e: e, snaps: map[int64]string{}}

	branch, status := (*evBranch)(nil), (*evStatus)(nil)
	var members int64
	sameMembers := func(stage string) {
		t.Helper()
		if got := e.scanInt(`SELECT count(*) FROM members`); got != members {
			t.Fatalf("%s: %d members; want %d", stage, got, members)
		}
	}

	// Release 1: the first table, then real rows, two of which share an email.
	dev.generate("members", (*evMemberV1)(nil))
	e.exec(`INSERT INTO members (name, email) VALUES
		('Ann', 'ann@coop.io'), ('Ann Again', 'ann@coop.io'), ('Bob', 'bob@coop.io'),
		('Cara', 'cara@coop.io'), ('Dan', 'dan@coop.io'), ('Eve', 'eve@coop.io')`)
	members = 6

	// Release 2: branches and a nullable foreign key, then a backfill that seeds the defaults existing rows need.
	dev.generate("branches", (*evMemberV2)(nil), branch)
	dev.hand("seed_branches_and_backfill",
		`INSERT INTO branches (code, name) VALUES ('HQ', 'Head Office'), ('NORTH', 'North Branch') ON CONFLICT (code) DO NOTHING;
UPDATE members SET branch_id = (SELECT id FROM branches WHERE code = 'HQ') WHERE branch_id IS NULL;`,
		`UPDATE members SET branch_id = NULL;
DELETE FROM branches WHERE code IN ('HQ', 'NORTH');`)
	if n := e.scanInt(`SELECT count(*) FROM members WHERE branch_id IS NULL`); n != 0 {
		t.Fatalf("%d members were not backfilled to a branch", n)
	}
	if e.scanInt(`SELECT count(*) FROM branches`) != 2 {
		t.Fatal("the branch seed did not run")
	}
	e.exec(`UPDATE members SET branch_id = (SELECT id FROM branches WHERE code = 'NORTH') WHERE name IN ('Dan', 'Eve')`)
	sameMembers("release 2")

	// Release 3: indexes Diff cannot express (functional, partial, composite). Models are unchanged, so Diff must stay quiet.
	dev.hand("member_indexes",
		`CREATE INDEX members_email_lower_idx ON members (lower(email));
CREATE INDEX members_branch_partial_idx ON members (branch_id) WHERE branch_id IS NOT NULL;
CREATE INDEX members_branch_email_idx ON members (branch_id, email);`,
		`DROP INDEX members_branch_email_idx;
DROP INDEX members_branch_partial_idx;
DROP INDEX members_email_lower_idx;`)
	requireIndexes(t, e, "members_email_lower_idx", "members_branch_partial_idx", "members_branch_email_idx")
	requireConverged(t, e, (*evMemberV2)(nil), branch)
	lagFiles, lagVersion = len(e.migrationFiles()), e.appliedVersion()

	// Release 4: columns with integer defaults on a populated table, a lookup table, and its seed rows.
	dev.generate("status_and_points", (*evMemberV3)(nil), branch, status)
	if n := e.scanInt(`SELECT count(*) FROM members WHERE status = 1 AND points = 0 AND phone IS NULL`); n != members {
		t.Fatalf("existing rows got defaults for %d of %d members", n, members)
	}
	dev.hand("seed_member_statuses",
		`INSERT INTO member_statuses (code, label) VALUES (1, 'active'), (2, 'suspended'), (3, 'closed') ON CONFLICT (code) DO NOTHING;`,
		`DELETE FROM member_statuses WHERE code IN (1, 2, 3);`)
	e.exec(`UPDATE members SET points = id * 300`)
	sameMembers("release 4")

	// Release 5: the status becomes a real foreign key; the seed from release 4 is what lets it apply.
	dev.generate("status_foreign_key", (*evMemberV4)(nil), branch, status)
	mustFail(t, e, "foreign key on status", `UPDATE members SET status = 99 WHERE name = 'Bob'`)
	e.exec(`UPDATE members SET status = 2 WHERE name = 'Dan'`)
	mustFail(t, e, "name still limited to 50 characters", `INSERT INTO members (name, email) VALUES ($1, 'long@coop.io')`, strings.Repeat("n", 150))
	sameMembers("release 5")

	// Release 6: widen the name; the same insert that failed a release ago now works.
	dev.generate("widen_name", (*evMemberV5)(nil), branch, status)
	e.exec(`INSERT INTO members (name, email, branch_id) VALUES ($1, 'long@coop.io', (SELECT id FROM branches WHERE code = 'HQ'))`, strings.Repeat("n", 150))
	members++
	sameMembers("release 6")

	// Release 7: rename name to full_name; it must be a rename, so the values come along.
	dev.generate("rename_name", (*evMemberV6)(nil), branch, status)
	if e.hasColumn("members", "name") || e.scanInt(`SELECT count(*) FROM members WHERE full_name IN ('Ann', 'Bob', 'Eve') OR length(full_name) = 150`) != 4 {
		t.Fatal("the rename lost or kept data")
	}
	sameMembers("release 7")

	// Release 8: clean up duplicate emails by hand, then let Diff add the unique constraint that would have failed before.
	if e.scanInt(`SELECT count(*) - count(DISTINCT email) FROM members`) == 0 {
		t.Fatal("the seed data should contain duplicate emails")
	}
	dev.hand("dedupe_member_emails",
		`UPDATE members m SET email = m.email || '+' || m.id
FROM (SELECT id, row_number() OVER (PARTITION BY email ORDER BY id) AS rn FROM members) d
WHERE d.id = m.id AND d.rn > 1;`,
		`SELECT 1;`)
	dev.generate("unique_email", (*evMemberV7)(nil), branch, status)
	mustFail(t, e, "unique email", `INSERT INTO members (full_name, email) VALUES ('Clone', 'bob@coop.io')`)
	requireIndexes(t, e, "members_email_lower_idx") // the hand-made index survives generated DDL
	sameMembers("release 8")

	// Release 9: loans with a composite unique key, a short-lived notes table, and two more indexes.
	dev.generate("loans_and_notes", (*evMemberV7)(nil), branch, status, (*evLoan)(nil), (*evNote)(nil))
	e.exec(`INSERT INTO loans (member_id, number, principal, status) VALUES
		((SELECT id FROM members WHERE email = 'bob@coop.io'), 'L-1', 1000, 0),
		((SELECT id FROM members WHERE email = 'bob@coop.io'), 'L-2', 2500, 1),
		((SELECT id FROM members WHERE email = 'cara@coop.io'), 'L-1', 4000, 0)`)
	mustFail(t, e, "composite unique (member, number)", `INSERT INTO loans (member_id, number, principal) VALUES ((SELECT id FROM members WHERE email = 'bob@coop.io'), 'L-1', 1)`)
	e.exec(`INSERT INTO member_notes (member_id, note) VALUES ((SELECT id FROM members WHERE email = 'bob@coop.io'), 'called'), ((SELECT id FROM members WHERE email = 'cara@coop.io'), 'visited')`)
	dev.hand("loan_indexes",
		`CREATE INDEX loans_active_idx ON loans (member_id) WHERE status = 0;
CREATE INDEX loans_member_status_idx ON loans (member_id, status DESC) INCLUDE (principal);`,
		`DROP INDEX loans_member_status_idx;
DROP INDEX loans_active_idx;`)
	requireIndexes(t, e, "loans_active_idx", "loans_member_status_idx")

	// Release 10: drop a whole table and a column; everything else must be untouched. Diff cannot write a
	// valid Down for a dropped table that has foreign keys, so the table goes by hand and Diff handles the column.
	loanSum := e.scanInt(`SELECT sum(principal) FROM loans`)
	dev.hand("drop_member_notes",
		`DROP TABLE member_notes;`,
		`CREATE TABLE member_notes (id bigserial PRIMARY KEY, member_id bigint NOT NULL, note varchar NOT NULL);
ALTER TABLE member_notes ADD CONSTRAINT member_notes_member_id_fkey FOREIGN KEY (member_id) REFERENCES members (id);`)
	dev.generate("drop_phone", (*evMemberV8)(nil), branch, status, (*evLoan)(nil))
	if e.hasColumn("members", "phone") || e.hasTable("member_notes") {
		t.Fatal("the dropped column or table is still there")
	}
	if e.scanInt(`SELECT sum(principal) FROM loans`) != loanSum || e.scanInt(`SELECT count(*) FROM loans`) != 3 {
		t.Fatal("dropping unrelated objects changed the loans")
	}
	requireIndexes(t, e, "loans_active_idx", "loans_member_status_idx")
	sameMembers("release 10")

	// Release 11: what bun cannot do. A string default and backfill, a CHECK, and an index built without blocking writes.
	// Hand-written columns must use the type bun maps the Go field to (varchar for string), or Diff sees drift.
	dev.hand("member_tier_and_loan_check",
		`ALTER TABLE members ADD COLUMN tier varchar NOT NULL DEFAULT 'basic';
UPDATE members SET tier = 'gold' WHERE points >= 1000;
UPDATE members SET tier = 'silver' WHERE points >= 400 AND points < 1000;
ALTER TABLE loans ADD CONSTRAINT loans_principal_positive CHECK (principal > 0);`,
		`ALTER TABLE loans DROP CONSTRAINT loans_principal_positive;
ALTER TABLE members DROP COLUMN tier;`)
	dev.handNoTx("big_loan_index_concurrently",
		`CREATE INDEX CONCURRENTLY loans_big_idx ON loans (principal) WHERE principal >= 3000;`,
		`DROP INDEX CONCURRENTLY loans_big_idx;`)
	requireConverged(t, e, (*evMemberV9)(nil), branch, status, (*evLoan)(nil)) // models catch up with the by-hand column
	if e.scanInt(`SELECT count(*) FROM members WHERE tier = 'gold'`) == 0 || e.scanInt(`SELECT count(*) FROM members WHERE tier = 'gold'`) != e.scanInt(`SELECT count(*) FROM members WHERE points >= 1000`) {
		t.Fatal("the tier backfill did not follow the points")
	}
	if e.scanInt(`SELECT indisvalid::int FROM pg_index WHERE indexrelid = 'loans_big_idx'::regclass`) != 1 {
		t.Fatal("the concurrently built index is invalid")
	}
	mustFail(t, e, "loan CHECK", `INSERT INTO loans (member_id, number, principal) VALUES ((SELECT id FROM members WHERE email = 'dan@coop.io'), 'L-0', 0)`)
	e.exec(`INSERT INTO members (full_name, email) VALUES ('Fay', 'fay@coop.io')`)
	members++
	if e.scanInt(`SELECT count(*) FROM members WHERE full_name = 'Fay' AND tier = 'basic'`) != 1 {
		t.Fatal("a new member did not get the default tier")
	}

	// Release 12: an audit table with jsonb and timestamp defaults, and the indexes that serve its queries.
	dev.generate("audit_events", (*evMemberV9)(nil), branch, status, (*evLoan)(nil), (*evAudit)(nil))
	dev.hand("audit_indexes",
		`CREATE INDEX audit_events_payload_idx ON audit_events USING gin (payload jsonb_path_ops);
CREATE INDEX audit_events_created_idx ON audit_events (created_at DESC);`,
		`DROP INDEX audit_events_created_idx;
DROP INDEX audit_events_payload_idx;`)
	e.exec(`INSERT INTO audit_events (member_id, kind, payload) VALUES ((SELECT id FROM members WHERE email = 'bob@coop.io'), 'login', '{"ip": "10.0.0.1", "ok": true}')`)
	if e.scanInt(`SELECT count(*) FROM audit_events WHERE payload @> '{"ok": true}' AND created_at <= now()`) != 1 {
		t.Fatal("the jsonb payload or the created_at default does not work")
	}
	sameMembers("release 12")

	if got := len(dev.order); got < 18 {
		t.Fatalf("only %d migrations were applied across twelve releases", got)
	}
	return dev, lagFiles, lagVersion
}

// ---- tests ------------------------------------------------------------------

// bun emits its statements in map order, so a foreign key could come before the column it uses. Diff
// must order them itself; thirty generations of the same change all have to produce a valid migration.
func TestSQLDiff_StatementOrderDoesNotDependOnMapIteration(t *testing.T) {
	e := newSQLEnv(t)
	diffApply(t, e, "members", (*evMemberV1)(nil))

	for i := range 30 {
		path, err := e.diff(fmt.Sprintf("branches %d", i), (*evMemberV2)(nil), (*evBranch)(nil))
		if err != nil || path == "" {
			t.Fatalf("generation %d: %q, %v", i, path, err)
		}
		if err := os.Remove(path); err != nil { // keep the directory clean so the next Diff starts from the same state
			t.Fatal(err)
		}
	}
}

// Happy: twelve releases of one schema, with rows present throughout, all apply and converge with the models.
func TestSQLEvolution_TwelveReleasesWithDataAndHandWrittenIndexes(t *testing.T) {
	dev, _, _ := buildReleases(t)

	for _, want := range []string{"members_email_lower_idx", "loans_big_idx", "audit_events_payload_idx"} {
		if !strings.Contains(fingerprint(dev.e), want) {
			t.Errorf("final schema is missing %s", want)
		}
	}
}

// Happy: a production database still on release 3 is upgraded through the nine later releases with its own data.
func TestSQLEvolution_ProductionBehindByNineReleasesUpgradesWithItsData(t *testing.T) {
	dev, lagFiles, lagVersion := buildReleases(t)

	prod := dev.e.sibling()
	svc := prod.newServiceWith(false)
	if err := svc.Run(bg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Stop(bg) })
	if err := svc.UpSteps(bg, lagFiles); err != nil {
		t.Fatalf("bringing production to release 3: %v", err)
	}
	if prod.appliedVersion() != lagVersion || fingerprint(prod) != dev.snaps[lagVersion] {
		t.Fatal("production at release 3 does not match development at release 3")
	}

	// Real production rows: 300 members, 50 of which reuse another member's email.
	prod.exec(`INSERT INTO members (name, email, branch_id)
		SELECT 'Member ' || g, 'm' || (g % 250) || '@coop.io',
		       (SELECT id FROM branches WHERE code = CASE WHEN g % 2 = 0 THEN 'HQ' ELSE 'NORTH' END)
		FROM generate_series(1, 300) g`)
	namesBefore := prod.scanInt(`SELECT sum(length(name)) FROM members`)

	if err := svc.Migrate(bg); err != nil {
		t.Fatalf("upgrading production: %v", err)
	}
	if prod.appliedVersion() != dev.e.appliedVersion() || fingerprint(prod) != fingerprint(dev.e) {
		t.Fatal("production and development diverged after the upgrade")
	}
	if got := prod.scanInt(`SELECT count(*) FROM members`); got != 300 {
		t.Fatalf("members after the upgrade = %d; want 300", got)
	}
	if got := prod.scanInt(`SELECT count(DISTINCT email) FROM members`); got != 300 {
		t.Fatalf("distinct emails = %d; want 300 after the dedupe", got)
	}
	if got := prod.scanInt(`SELECT count(*) FROM members WHERE email LIKE '%+%'`); got != 50 {
		t.Fatalf("%d emails were rewritten; want exactly the 50 duplicates", got)
	}
	if got := prod.scanInt(`SELECT sum(length(full_name)) FROM members`); got != namesBefore {
		t.Fatalf("names changed across the rename: %d characters before, %d after", namesBefore, got)
	}
	if got := prod.scanInt(`SELECT count(*) FROM members WHERE status = 1 AND points = 0 AND tier = 'basic' AND branch_id IS NOT NULL`); got != 300 {
		t.Fatalf("%d of 300 production members have the expected defaults", got)
	}

	// The upgraded production database enforces everything the later releases added.
	mustFail(t, prod, "unique email", `INSERT INTO members (full_name, email) VALUES ('Clone', 'm1@coop.io')`)
	mustFail(t, prod, "status foreign key", `UPDATE members SET status = 99 WHERE id = (SELECT min(id) FROM members)`)
	mustFail(t, prod, "loan CHECK", `INSERT INTO loans (member_id, number, principal) VALUES ((SELECT min(id) FROM members), 'L-0', -5)`)
	prod.exec(`INSERT INTO audit_events (member_id, kind, payload) VALUES ((SELECT min(id) FROM members), 'upgrade', '{"ok": true}')`)

	if err := svc.Migrate(bg); err != nil {
		t.Fatalf("a second Migrate must be a no-op: %v", err)
	}
	requireConverged(t, prod, (*evMemberV9)(nil), (*evBranch)(nil), (*evStatus)(nil), (*evLoan)(nil), (*evAudit)(nil))
}

// Happy: the whole history rebuilds a new database from nothing, and rolling back one migration at a
// time passes through exactly the schema (columns, indexes, constraints) each release had going forward.
func TestSQLEvolution_ReplayFromScratchThenRollBackEveryMigration(t *testing.T) {
	dev, _, _ := buildReleases(t)
	final := fingerprint(dev.e)

	fresh := dev.e.sibling()
	svc := fresh.running() // Run applies the whole history
	if fingerprint(fresh) != final {
		t.Fatal("a database built from the migration files differs from the evolved one")
	}

	for i := len(dev.order) - 1; i >= 1; i-- {
		if err := svc.Rollback(bg); err != nil {
			t.Fatalf("rolling back migration %d of %d: %v", i+1, len(dev.order), err)
		}
		want := dev.order[i-1]
		if got := fresh.appliedVersion(); got != want {
			t.Fatalf("after rollback #%d the version is %d; want %d", len(dev.order)-i, got, want)
		}
		if got := fingerprint(fresh); got != dev.snaps[want] {
			t.Fatalf("after rolling back to version %d the schema differs from the original:\n--- got\n%s\n--- want\n%s", want, got, dev.snaps[want])
		}
	}
	if err := svc.Rollback(bg); err != nil {
		t.Fatalf("rolling back the first migration: %v", err)
	}
	if fresh.schema() != "" || listing(fresh, `SELECT indexname FROM pg_indexes WHERE schemaname = 'public' AND tablename <> 'goose_db_version'`) != "" {
		t.Fatalf("objects survived a full rollback:\n%s", fingerprint(fresh))
	}

	if err := svc.Migrate(bg); err != nil {
		t.Fatalf("re-applying the whole history: %v", err)
	}
	if fingerprint(fresh) != final {
		t.Fatal("re-applying after a full rollback did not reproduce the final schema")
	}
}
