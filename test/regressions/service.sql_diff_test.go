package regressions

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	sqlsvc "github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"
	"github.com/uptrace/bun"
)

// ---- models -----------------------------------------------------------------

type dOrg struct {
	bun.BaseModel `bun:"table:orgs"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Name          string `bun:"name,notnull"`
}

type dUser struct {
	bun.BaseModel `bun:"table:users"`
	ID            int64     `bun:"id,pk,autoincrement"`
	OrgID         int64     `bun:"org_id,notnull"`
	Email         string    `bun:"email,notnull"`
	Active        bool      `bun:"active,notnull,default:true"`
	Profile       string    `bun:"profile,type:jsonb"`
	CreatedAt     time.Time `bun:"created_at,notnull,default:current_timestamp"`
	Org           *dOrg     `bun:"rel:belongs-to,join:org_id=id"`
}

type dAccount struct {
	bun.BaseModel `bun:"table:accounts"`
	ID            int64   `bun:"id,pk,autoincrement"`
	UserID        int64   `bun:"user_id,notnull"`
	Balance       float64 `bun:"balance,notnull,type:numeric"`
	User          *dUser  `bun:"rel:belongs-to,join:user_id=id"`
}

type dTransaction struct {
	bun.BaseModel `bun:"table:transactions"`
	ID            int64     `bun:"id,pk,autoincrement"`
	AccountID     int64     `bun:"account_id,notnull"`
	Amount        float64   `bun:"amount,notnull,type:numeric"`
	Memo          string    `bun:"memo"`
	PostedAt      time.Time `bun:"posted_at,notnull,default:current_timestamp"`
	Account       *dAccount `bun:"rel:belongs-to,join:account_id=id"`
}

// Versions of one table, as it would evolve in a real codebase.
type dItemV1 struct {
	bun.BaseModel `bun:"table:items"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Name          string `bun:"name,notnull,type:varchar(20)"`
	Qty           int32  `bun:"qty,notnull"`
	Legacy        string `bun:"legacy"`
}

type dItemV2 struct { // adds nullable + NOT NULL-with-default columns
	bun.BaseModel `bun:"table:items"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Name          string `bun:"name,notnull,type:varchar(20)"`
	Qty           int32  `bun:"qty,notnull"`
	Legacy        string `bun:"legacy"`
	Note          string `bun:"note"`
	Priority      int32  `bun:"priority,notnull,default:0"`
}

type dItemV3 struct { // widens varchar, widens int, drops a column
	bun.BaseModel `bun:"table:items"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Name          string `bun:"name,notnull,type:varchar(200)"`
	Qty           int64  `bun:"qty,notnull"`
	Note          string `bun:"note"`
	Priority      int32  `bun:"priority,notnull,default:0"`
}

type dItemV4 struct { // relaxes NOT NULL and adds a unique constraint
	bun.BaseModel `bun:"table:items"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Name          string `bun:"name,type:varchar(200),unique"`
	Qty           int64  `bun:"qty,notnull"`
	Note          string `bun:"note"`
	Priority      int32  `bun:"priority,notnull,default:0"`
}

type dPersonV1 struct {
	bun.BaseModel `bun:"table:people"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Name          string `bun:"name,notnull"`
}

type dPersonV2 struct { // column renamed, same type
	bun.BaseModel `bun:"table:people"`
	ID            int64  `bun:"id,pk,autoincrement"`
	FullName      string `bun:"full_name,notnull"`
}

// ---- tests ------------------------------------------------------------------

func TestSQLDiff_NewSchemaWithRelations(t *testing.T) {
	e := newSQLEnv(t)
	svc := e.running()
	models := []any{(*dTransaction)(nil), (*dAccount)(nil), (*dUser)(nil), (*dOrg)(nil)} // deliberately child-first

	content := diffApply(t, e, "core schema", models...)
	for _, table := range []string{"orgs", "users", "accounts", "transactions"} {
		if !e.hasTable(table) {
			t.Fatalf("table %s missing after applying:\n%s", table, content)
		}
	}
	if n := e.constraintCount("transactions", "f"); n != 1 {
		t.Errorf("transactions has %d foreign keys; want 1\n%s", n, content)
	}
	if n := e.constraintCount("users", "p"); n != 1 {
		t.Errorf("users has %d primary keys; want 1", n)
	}
	requireConverged(t, e, models...)

	built := e.schema()

	// Down must tear it all down in dependency order (children before parents), and Up must restore it.
	if err := svc.Rollback(bg); err != nil {
		t.Fatalf("Rollback: %v\n%s", err, gooseDown(content))
	}
	if e.schema() != "" {
		t.Fatalf("schema not empty after rollback:\n%s", e.schema())
	}
	if err := svc.Migrate(bg); err != nil {
		t.Fatalf("re-Migrate: %v", err)
	}
	if e.schema() != built {
		t.Fatalf("schema after rollback+migrate differs from the original")
	}

	// Real rows survive and the FK is enforced.
	e.exec(`INSERT INTO orgs (name) VALUES ('acme')`)
	e.exec(`INSERT INTO users (org_id, email) VALUES (1, 'a@x.io')`)
	if _, err := e.db.Exec(`INSERT INTO users (org_id, email) VALUES (999, 'b@x.io')`); err == nil {
		t.Error("foreign key to orgs is not enforced")
	}
}

func TestSQLDiff_EvolutionHistoryReplaysToTheSameSchema(t *testing.T) {
	e := newSQLEnv(t)
	svc := e.running()

	steps := []struct {
		name   string
		model  any
		expect []string // fragments that must appear in the Up SQL
	}{
		{"items v1", (*dItemV1)(nil), []string{"CREATE TABLE"}},
		{"items v2 add columns", (*dItemV2)(nil), []string{"ADD COLUMN"}},
		{"items v3 widen and drop", (*dItemV3)(nil), []string{"DROP COLUMN"}},
		{"items v4 relax and unique", (*dItemV4)(nil), []string{"DROP NOT NULL"}},
	}

	var snapshots []string
	for i, step := range steps {
		if i > 0 {
			// Rows written under earlier versions must survive every later migration.
			e.exec(`INSERT INTO items (name, qty) VALUES ($1, $2)`, fmt.Sprintf("row-%d", i), i)
		}
		contents := settleModels(t, e, step.name, []any{step.model}, func() { snapshots = append(snapshots, e.schema()) })
		allUp := gooseUp(strings.Join(contents, "\n"))
		for _, frag := range step.expect {
			if !strings.Contains(allUp, frag) {
				t.Errorf("%s: Up SQL lacks %q:\n%s", step.name, frag, strings.Join(contents, "\n---\n"))
			}
		}
		requireConverged(t, e, step.model)
		if i == 0 {
			e.exec(`INSERT INTO items (name, qty, legacy) VALUES ('seed', 7, 'old')`)
		}
	}
	finalRows := e.scanInt(`SELECT count(*) FROM items`)
	if finalRows < 4 {
		t.Fatalf("rows were lost across migrations: %d", finalRows)
	}

	// Specific end-state facts the evolution must have produced.
	if e.hasColumn("items", "legacy") {
		t.Error("dropped column legacy still exists")
	}
	if got := e.scanInt(`SELECT character_maximum_length FROM information_schema.columns WHERE table_name='items' AND column_name='name'`); got != 200 {
		t.Errorf("name length = %d; want 200", got)
	}
	if got := e.scanInt(`SELECT count(*) FROM information_schema.columns WHERE table_name='items' AND column_name='qty' AND data_type='bigint'`); got != 1 {
		t.Error("qty was not widened to bigint")
	}
	if n := e.constraintCount("items", "u"); n != 1 {
		t.Errorf("items has %d unique constraints; want 1", n)
	}
	final := e.schema()

	// Walk back one migration at a time: every intermediate schema must match what it was going forward.
	for i := len(snapshots) - 2; i >= 0; i-- {
		if err := svc.Rollback(bg); err != nil {
			t.Fatalf("Rollback to migration %d: %v", i+1, err)
		}
		if got := e.schema(); got != snapshots[i] {
			t.Fatalf("schema after rolling back to migration %d differs from the original:\n--- got\n%s\n--- want\n%s", i+1, got, snapshots[i])
		}
	}
	if err := svc.RollbackTo(bg, 0); err != nil {
		t.Fatalf("RollbackTo(0): %v", err)
	}
	if e.schema() != "" {
		t.Fatalf("schema not empty after rolling back everything:\n%s", e.schema())
	}

	// Replaying the whole history from nothing must land on the identical schema.
	if err := svc.Migrate(bg); err != nil {
		t.Fatalf("replay Migrate: %v", err)
	}
	if got := e.schema(); got != final {
		t.Fatalf("replayed schema differs:\n--- got\n%s\n--- want\n%s", got, final)
	}
	requireConverged(t, e, (*dItemV4)(nil))

	// And a brand-new database built purely from the migration files agrees with the models.
	history := map[string][]byte{}
	for _, f := range e.migrationFiles() {
		history[filepath.Base(f)] = []byte(readFile(t, f))
	}
	if len(history) != len(snapshots) {
		t.Fatalf("history has %d files; want %d", len(history), len(snapshots))
	}
	e2 := newSQLEnv(t) // also moves the working directory to e2's own migrations directory
	for name, data := range history {
		if err := os.WriteFile(filepath.Join(e2.dir, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	e2.running() // Run applies the whole history
	if got := e2.schema(); got != final {
		t.Fatalf("a fresh database built from the files differs:\n--- got\n%s\n--- want\n%s", got, final)
	}
	requireConverged(t, e2, (*dItemV4)(nil))
}

func TestSQLDiff_RenamedColumn(t *testing.T) {
	e := newSQLEnv(t)
	svc := e.running()

	diffApply(t, e, "people", (*dPersonV1)(nil))
	e.exec(`INSERT INTO people (name) VALUES ('Ada'), ('Grace')`)

	path, err := e.diff("rename name to full name", (*dPersonV2)(nil))
	if err != nil || path == "" {
		t.Fatalf("Diff = %q, %v", path, err)
	}
	content := readFile(t, path)
	upSQL := strings.ToUpper(gooseUp(content))

	// A same-type column swap must be a rename, never drop+add, which would lose the data.
	if !strings.Contains(upSQL, "RENAME COLUMN") || strings.Contains(upSQL, "DROP COLUMN") {
		t.Fatalf("expected a RENAME COLUMN and no DROP COLUMN:\n%s", content)
	}
	if err := svc.Migrate(bg); err != nil {
		t.Fatalf("Migrate: %v\n%s", err, content)
	}

	if e.scanInt(`SELECT count(*) FROM people WHERE full_name IN ('Ada','Grace')`) != 2 {
		t.Fatalf("renamed column lost its data:\n%s", content)
	}
	requireConverged(t, e, (*dPersonV2)(nil))

	if err := svc.Rollback(bg); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if e.scanInt(`SELECT count(*) FROM people WHERE name IN ('Ada','Grace')`) != 2 {
		t.Fatal("rolling back the rename lost data")
	}
}

func TestSQLDiff_ModelsOmittedFromDiffAreDropped(t *testing.T) {
	e := newSQLEnv(t)

	diffApply(t, e, "both", (*dOrg)(nil), (*dPersonV1)(nil))

	path, err := e.diff("forgot a model", (*dOrg)(nil))
	if err != nil || path == "" {
		t.Fatalf("Diff = %q, %v", path, err)
	}
	content := readFile(t, path)
	if !strings.Contains(gooseUp(content), "DROP TABLE") || !strings.Contains(gooseUp(content), "people") {
		t.Fatalf("an omitted model must show up as DROP TABLE (so reviewers can catch it):\n%s", content)
	}
	if !strings.Contains(gooseDown(content), "people") {
		t.Fatalf("Down for a dropped table should mention it:\n%s", content)
	}
	// Nothing is applied by Diff itself.
	if !e.hasTable("people") {
		t.Fatal("Diff dropped a table; only Migrate may change the database")
	}
}

func TestSQLDiff_DetectsDriftFromManualChanges(t *testing.T) {
	e := newSQLEnv(t)
	diffApply(t, e, "orgs", (*dOrg)(nil))

	e.exec(`ALTER TABLE orgs ADD COLUMN hotfix text`)          // someone patched prod by hand
	e.exec(`ALTER TABLE orgs ALTER COLUMN name DROP NOT NULL`) // and loosened a constraint

	content := diffApply(t, e, "undo manual changes", (*dOrg)(nil))
	if !strings.Contains(gooseUp(content), "DROP COLUMN") || !strings.Contains(gooseUp(content), "SET NOT NULL") {
		t.Fatalf("drift was not reverted by the generated migration:\n%s", content)
	}
	if e.hasColumn("orgs", "hotfix") {
		t.Fatal("manual column still present")
	}
	requireConverged(t, e, (*dOrg)(nil))
}

func TestSQLDiff_NoChangeWritesNothing(t *testing.T) {
	e := newSQLEnv(t)
	diffApply(t, e, "orgs", (*dOrg)(nil))

	tmpBefore, _ := filepath.Glob(filepath.Join(os.TempDir(), "goose-diff-*"))
	for range 3 {
		requireConverged(t, e, (*dOrg)(nil))
	}
	tmpAfter, _ := filepath.Glob(filepath.Join(os.TempDir(), "goose-diff-*"))
	if len(tmpAfter) > len(tmpBefore) {
		t.Fatalf("Diff leaked scratch directories: %v", tmpAfter)
	}
}

func TestSQLDiff_BackToBackMigrationsStayOrdered(t *testing.T) {
	e := newSQLEnv(t)

	// Generated within the same second: versions must still be strictly increasing and apply cleanly.
	diffApply(t, e, "one", (*dItemV1)(nil))
	diffApply(t, e, "two", (*dItemV2)(nil))
	diffApply(t, e, "three", (*dItemV3)(nil))

	files := e.migrationFiles()
	if len(files) != 3 {
		t.Fatalf("files = %v; want 3", files)
	}
	var last int64
	for _, f := range files {
		prefix, _, _ := strings.Cut(filepath.Base(f), "_")
		v, err := strconv.ParseInt(prefix, 10, 64)
		if err != nil {
			t.Fatalf("file %q has no numeric version", f)
		}
		if v <= last {
			t.Fatalf("versions not strictly increasing: %v", files)
		}
		last = v
	}
	if e.appliedVersion() != last {
		t.Fatalf("applied version = %d; want %d", e.appliedVersion(), last)
	}
}

func TestSQLDiff_StaysAheadOfExistingVersions(t *testing.T) {
	e := newSQLEnv(t)
	// A hand-written migration with a version far in the future (e.g. from a branch with a skewed clock).
	e.write(99999999999999, "future", `CREATE TABLE future_t (id int);`, `DROP TABLE future_t;`)
	svc := e.running()

	path, err := e.diff("orgs", (*dOrg)(nil), (*futureT)(nil))
	if err != nil || path == "" {
		t.Fatalf("Diff = %q, %v", path, err)
	}
	prefix, _, _ := strings.Cut(filepath.Base(path), "_")
	v, _ := strconv.ParseInt(prefix, 10, 64)
	if v <= 99999999999999 {
		t.Fatalf("new version %d is not after the existing 99999999999999; goose would reject it as out of order", v)
	}
	if err := svc.Migrate(bg); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
}

type futureT struct {
	bun.BaseModel `bun:"table:future_t"`
	ID            int32 `bun:"id"`
}

func TestSQLDiff_ManyTablesWithForeignKeyChain(t *testing.T) {
	e := newSQLEnv(t)
	svc := e.running()

	// Twelve tables is too many to spell out, so use eight where each references the previous one.
	models := []any{(*c8)(nil), (*c7)(nil), (*c6)(nil), (*c5)(nil), (*c4)(nil), (*c3)(nil), (*c2)(nil), (*c1)(nil)}
	content := diffApply(t, e, "chain", models...)
	for i := 1; i <= 8; i++ {
		if !e.hasTable(fmt.Sprintf("chain_%d", i)) {
			t.Fatalf("chain_%d missing\n%s", i, content)
		}
	}
	if n := e.scanInt(`SELECT count(*) FROM information_schema.table_constraints WHERE constraint_type='FOREIGN KEY' AND table_name LIKE 'chain_%'`); n != 7 {
		t.Errorf("foreign keys = %d; want 7\n%s", n, content)
	}
	requireConverged(t, e, models...)
	if err := svc.Rollback(bg); err != nil {
		t.Fatalf("Rollback of an FK chain: %v", err)
	}
	if e.schema() != "" {
		t.Fatal("tables left after rolling back the chain")
	}
}

type c1 struct {
	bun.BaseModel `bun:"table:chain_1"`
	ID            int64 `bun:"id,pk,autoincrement"`
}
type c2 struct {
	bun.BaseModel `bun:"table:chain_2"`
	ID            int64 `bun:"id,pk,autoincrement"`
	Prev          int64 `bun:"prev,notnull"`
	P             *c1   `bun:"rel:belongs-to,join:prev=id"`
}
type c3 struct {
	bun.BaseModel `bun:"table:chain_3"`
	ID            int64 `bun:"id,pk,autoincrement"`
	Prev          int64 `bun:"prev,notnull"`
	P             *c2   `bun:"rel:belongs-to,join:prev=id"`
}
type c4 struct {
	bun.BaseModel `bun:"table:chain_4"`
	ID            int64 `bun:"id,pk,autoincrement"`
	Prev          int64 `bun:"prev,notnull"`
	P             *c3   `bun:"rel:belongs-to,join:prev=id"`
}
type c5 struct {
	bun.BaseModel `bun:"table:chain_5"`
	ID            int64 `bun:"id,pk,autoincrement"`
	Prev          int64 `bun:"prev,notnull"`
	P             *c4   `bun:"rel:belongs-to,join:prev=id"`
}
type c6 struct {
	bun.BaseModel `bun:"table:chain_6"`
	ID            int64 `bun:"id,pk,autoincrement"`
	Prev          int64 `bun:"prev,notnull"`
	P             *c5   `bun:"rel:belongs-to,join:prev=id"`
}
type c7 struct {
	bun.BaseModel `bun:"table:chain_7"`
	ID            int64 `bun:"id,pk,autoincrement"`
	Prev          int64 `bun:"prev,notnull"`
	P             *c6   `bun:"rel:belongs-to,join:prev=id"`
}
type c8 struct {
	bun.BaseModel `bun:"table:chain_8"`
	ID            int64 `bun:"id,pk,autoincrement"`
	Prev          int64 `bun:"prev,notnull"`
	P             *c7   `bun:"rel:belongs-to,join:prev=id"`
}

func TestSQLDiff_RejectsBadInput(t *testing.T) {
	e := newSQLEnv(t)

	if _, err := e.diff("x"); !errors.Is(err, sqlsvc.ErrNoModels) {
		t.Errorf("no models = %v; want ErrNoModels", err)
	}
	for _, name := range []string{"", "   ", "!!!", "___"} {
		if _, err := e.diff(name, (*dOrg)(nil)); !errors.Is(err, sqlsvc.ErrInvalidName) {
			t.Errorf("Diff(%q) = %v; want ErrInvalidName", name, err)
		}
	}

	// Path separators and dots are flattened into a safe file name, never followed.
	path, err := e.diff("../../etc/passwd", (*dOrg)(nil))
	if err != nil {
		t.Fatalf("Diff with traversal-looking name: %v", err)
	}
	if filepath.Dir(path) != e.dir || strings.Contains(filepath.Base(path), "..") || strings.ContainsAny(filepath.Base(path), `/\`) {
		t.Fatalf("unsafe migration path %q", path)
	}
	if len(e.migrationFiles()) != 1 {
		t.Fatalf("files = %v", e.migrationFiles())
	}
}

func TestSQLDiff_RefusesWhileMigrationsArePending(t *testing.T) {
	e := newSQLEnv(t)
	svc := e.running()

	if _, err := e.diff("first", (*dOrg)(nil)); err != nil {
		t.Fatalf("first Diff: %v", err)
	}
	// Written but not applied: a second diff would regenerate the same CREATE TABLE.
	if _, err := e.diff("second", (*dOrg)(nil)); !errors.Is(err, sqlsvc.ErrPendingMigrations) {
		t.Fatalf("Diff with a pending migration = %v; want ErrPendingMigrations", err)
	}
	if err := svc.Migrate(bg); err != nil {
		t.Fatal(err)
	}
	if path, err := e.diff("third", (*dOrg)(nil)); err != nil || path != "" {
		t.Fatalf("Diff after applying = %q, %v; want no file", path, err)
	}
}

func TestSQLDiff_GeneratedMigrationsAreEditable(t *testing.T) {
	e := newSQLEnv(t)
	svc := e.running()

	path, err := e.diff("orgs", (*dOrg)(nil))
	if err != nil || path == "" {
		t.Fatal(err)
	}
	// Developers add data backfills and seed rows by hand; goose must run whatever the file says.
	extra := "\nINSERT INTO orgs (name) VALUES ('seeded');\n"
	content := readFile(t, path)
	edited := strings.Replace(content, "\n-- +goose Down", extra+"\n-- +goose Down", 1)
	if err := os.WriteFile(path, []byte(edited), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := svc.Migrate(bg); err != nil {
		t.Fatalf("Migrate of the edited file: %v\n%s", err, edited)
	}
	if e.scanInt(`SELECT count(*) FROM orgs WHERE name = 'seeded'`) != 1 {
		t.Fatal("hand-written statements in a generated migration were not run")
	}
}
