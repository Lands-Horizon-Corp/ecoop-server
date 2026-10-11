package sql_test

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	ecsql "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sql"
)

// Schema diff: Migrate generating migrations by comparing the models with
// the live schema.

func versionOf(file string) int64 {
	v, _ := strconv.ParseInt(strings.SplitN(filepath.Base(file), "_", 2)[0], 10, 64)
	return v
}

func readFile(t testing.TB, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestSQLDiff_NewSchemaWithRelations(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s := e.started(t, (*DiffCustomer)(nil), (*DiffAccount)(nil))
	ctx := ctxT(t)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Client().ExecContext(ctx, "INSERT INTO diff_accounts (customer_id) VALUES (999)"); err == nil {
		t.Fatal("foreign key not enforced")
	}
	// Down tears it down children first; Up restores it.
	if err := s.Rollback(ctx); err != nil {
		t.Fatalf("Rollback (generated Down): %v", err)
	}
	if got := tables(t, s); len(got) != 0 {
		t.Fatalf("tables after Rollback = %v", got)
	}
	if err := s.UpSteps(ctx, 1); err != nil {
		t.Fatal(err)
	}
	execSQL(t, s, "INSERT INTO diff_customers (name) VALUES ('a')")
	execSQL(t, s, "INSERT INTO diff_accounts (customer_id) SELECT id FROM diff_customers")
	if _, err := s.Client().ExecContext(ctx, "DELETE FROM diff_customers"); err == nil {
		t.Fatal("foreign key not enforced after Up")
	}
	if n := count(t, s, "SELECT count(*) FROM diff_accounts"); n != 1 {
		t.Fatalf("rows = %d", n)
	}
}

func TestSQLDiff_EvolutionHistoryReplaysToTheSameSchema(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := ctxT(t)
	var last string
	for i, model := range []any{(*EvoV1)(nil), (*EvoV2)(nil), (*EvoV3)(nil), (*EvoV4)(nil)} {
		s := e.started(t, model)
		if err := s.Migrate(ctx); err != nil {
			t.Fatalf("release %d: %v", i+1, err)
		}
		execSQL(t, s, "INSERT INTO evo (name) VALUES (?)", "row"+strconv.Itoa(i))
		if n := count(t, s, "SELECT count(*) FROM evo"); n != i+1 {
			t.Fatalf("release %d: rows = %d", i+1, n)
		}
		last = snapshot(t, s)
		_ = s.Stop(ctx)
	}
	if got := replayAndRollBack(t, e); got != last {
		t.Fatalf("replay from nothing differs:\n%s", diffLines(last, got))
	}
}

func TestSQLDiff_RenamedColumn(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := ctxT(t)
	v1 := e.started(t, (*RenameV1)(nil))
	if err := v1.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	execSQL(t, v1, "INSERT INTO renames (nick) VALUES ('ana')")
	_ = v1.Stop(ctx)
	v2 := e.started(t, (*RenameV2)(nil))
	if err := v2.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	files := e.files("*.sql")
	body := readFile(t, files[len(files)-1])
	if !strings.Contains(body, "RENAME COLUMN") || strings.Contains(body, "DROP COLUMN") {
		t.Fatalf("rename generated as:\n%s", body)
	}
	if n := count(t, v2, "SELECT count(*) FROM renames WHERE handle = 'ana'"); n != 1 {
		t.Fatal("data lost in the rename")
	}
}

func TestSQLDiff_ModelsOmittedFromDiffAreDropped(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := ctxT(t)
	both := e.started(t, (*DiffCustomer)(nil), (*DiffAccount)(nil))
	if err := both.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	execSQL(t, both, "INSERT INTO diff_customers (name) VALUES ('a')")
	execSQL(t, both, "INSERT INTO diff_accounts (customer_id) SELECT id FROM diff_customers")
	_ = both.Stop(ctx)
	one := e.started(t, (*DiffCustomer)(nil))
	if err := one.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	review := e.files("*" + ecsql.ReviewSuffix)
	if len(review) != 1 || !strings.Contains(readFile(t, review[0]), `DROP TABLE "public"."diff_accounts"`) {
		t.Fatalf("review files = %v", review)
	}
	if n := count(t, one, "SELECT count(*) FROM diff_accounts"); n != 1 {
		t.Fatal("the omitted table was dropped without review")
	}
}

func TestSQLDiff_DetectsDriftFromManualChanges(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s := e.started(t, (*EvoV2)(nil))
	ctx := ctxT(t)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	execSQL(t, s, "ALTER TABLE evo ADD COLUMN extra text")
	execSQL(t, s, "ALTER TABLE evo ALTER COLUMN name DROP NOT NULL")
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	review := e.files("*" + ecsql.ReviewSuffix)
	if len(review) != 1 {
		t.Fatalf("drift not detected: files %v", e.files("*"))
	}
	body := readFile(t, review[0])
	if !strings.Contains(body, `DROP COLUMN "extra"`) || !strings.Contains(body, `SET NOT NULL`) {
		t.Fatalf("drift migration:\n%s", body)
	}
	if err := os.Rename(review[0], strings.TrimSuffix(review[0], ecsql.ReviewSuffix)); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if slices.Contains(columns(t, s, "evo"), "extra") {
		t.Fatal("extra column not reverted")
	}
	if _, err := s.Client().ExecContext(ctx, "INSERT INTO evo (name) VALUES (NULL)"); err == nil {
		t.Fatal("loosened NOT NULL not reverted")
	}
}

func TestSQLDiff_NoChangeWritesNothing(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s := e.started(t, (*EvoV4)(nil), (*DiffCustomer)(nil), (*DiffAccount)(nil))
	ctx := ctxT(t)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	files := e.files("*")
	for range 3 {
		if err := s.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if got := e.files("*"); !slices.Equal(got, files) {
		t.Fatalf("files changed with no model change: %v → %v", files, got)
	}
	// Scratch folders are checked for leaks once for the whole run, in TestMain.
}

func TestSQLDiff_BackToBackMigrationsStayOrdered(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := ctxT(t)
	for i, model := range []any{(*EvoV1)(nil), (*EvoV2)(nil), (*EvoV3)(nil)} {
		s := e.started(t, model)
		if err := s.Create(ctx, "note "+strconv.Itoa(i)); err != nil {
			t.Fatal(err)
		}
		if err := s.Migrate(ctx); err != nil {
			t.Fatalf("release %d: %v", i, err)
		}
		_ = s.Stop(ctx)
	}
	files := e.files("*.sql")
	for i := 1; i < len(files); i++ {
		if versionOf(files[i]) <= versionOf(files[i-1]) {
			t.Fatalf("versions not strictly increasing: %v", files)
		}
	}
	if len(files) != 6 {
		t.Fatalf("files = %v", files)
	}
}

func TestSQLDiff_StaysAheadOfExistingVersions(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	const future = int64(99990101000000) // a clock far ahead wrote this
	e.write(t, future, "from_the_future", "SELECT 1;", "SELECT 1;")
	s := e.started(t, (*EvoV1)(nil))
	if err := s.Migrate(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	files := e.files("*_auto.sql")
	if len(files) != 1 || versionOf(files[0]) <= future {
		t.Fatalf("generated %v, want a version after %d", files, future)
	}
}

func TestSQLDiff_ManyTablesWithForeignKeyChain(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s := e.started(t, chainModels...)
	ctx := ctxT(t)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if n := len(tables(t, s)); n != 8 {
		t.Fatalf("tables = %d", n)
	}
	execSQL(t, s, "INSERT INTO chain1 DEFAULT VALUES")
	for i := 2; i <= 8; i++ {
		execSQL(t, s, "INSERT INTO chain"+strconv.Itoa(i)+" (prev_id) SELECT max(id) FROM chain"+strconv.Itoa(i-1))
	}
	if err := s.Rollback(ctx); err != nil {
		t.Fatalf("dropping the chain: %v", err)
	}
	if n := len(tables(t, s)); n != 0 {
		t.Fatalf("tables left = %d", n)
	}
}

func TestSQLDiff_RejectsBadInput(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := ctxT(t)
	none := e.started(t)
	if err := none.Migrate(ctx); err != nil || len(e.files("*")) != 0 {
		t.Fatalf("Migrate without models = %v, files %v", err, e.files("*"))
	}
	bad := ecsql.NewSQLService(e.dsn, 1, 1, e.dir, false, e.out, []any{EvoV1{}}, nil)
	if err := bad.Migrate(ctx); !errors.Is(err, ecsql.ErrInvalidModel) {
		t.Fatalf("struct value model = %v, want ErrInvalidModel", err)
	}
	for _, name := range []string{"", "  ", "!!"} {
		if err := none.Create(ctx, name); !errors.Is(err, ecsql.ErrEmptyName) {
			t.Fatalf("Create(%q) = %v", name, err)
		}
	}
	if err := none.Create(ctx, "a/b\\c"); err != nil {
		t.Fatal(err)
	}
	if f := e.files("*"); len(f) != 1 || !strings.HasSuffix(f[0], "_a_b_c.sql") {
		t.Fatalf("separators not flattened: %v", f)
	}
}

// A pending migration (a teammate's file not yet applied here) is applied
// before diffing, so the same change is never generated twice.
func TestSQLDiff_PendingMigrationsAreAppliedBeforeDiffing(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.write(t, 1, "teammate_evo", `CREATE TABLE evo ("id" BIGSERIAL NOT NULL, "name" VARCHAR NOT NULL, PRIMARY KEY ("id"));`, "DROP TABLE evo;")
	s := e.started(t, (*EvoV1)(nil))
	if err := s.Migrate(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	if got := e.files("*_auto.sql*"); len(got) != 0 {
		t.Fatalf("generated a duplicate of the pending migration: %v", got)
	}
}

func TestSQLDiff_GeneratedMigrationsAreEditable(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s := e.started(t, (*EvoV1)(nil))
	ctx := ctxT(t)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	file := e.files("*_auto.sql")[0]
	body := strings.Replace(readFile(t, file), "\n\n-- +goose Down",
		"\nINSERT INTO evo (name) VALUES ('seed-1'), ('seed-2');\n\n-- +goose Down", 1)
	if err := os.WriteFile(file, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if n := count(t, s, "SELECT count(*) FROM evo WHERE name LIKE 'seed-%'"); n != 2 {
		t.Fatalf("seed rows = %d", n)
	}
	if len(e.files("*")) != 1 {
		t.Fatalf("edited migration regenerated: %v", e.files("*"))
	}
}

// Diff limits: a change the generator cannot express must either apply and
// converge, or fail with nothing written and nothing changed.

func checkSafe(t *testing.T, from, to []any) {
	t.Helper()
	e := newEnv(t)
	ctx := ctxT(t)
	if from != nil {
		s := e.started(t, from...)
		if err := s.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
		execSQL(t, s, "INSERT INTO "+tables(t, s)[0]+" DEFAULT VALUES")
		_ = s.Stop(ctx)
	}
	s := e.started(t, to...)
	files, snap := e.files("*"), snapshot(t, s)
	err := s.Migrate(ctx)
	if err != nil {
		if got := e.files("*"); !slices.Equal(got, files) {
			t.Fatalf("refused but left files: %v", got)
		}
		if snapshot(t, s) != snap {
			t.Fatal("refused but changed the schema")
		}
		t.Logf("refused cleanly: %v", err)
		return
	}
	n := len(e.files("*"))
	if err := s.Migrate(ctx); err != nil || len(e.files("*")) != n {
		t.Fatalf("applied but does not converge: %v, files %v", err, e.files("*"))
	}
}

func TestSQLDiff_StringDefaultOnExistingTableIsSafe(t *testing.T) {
	t.Parallel()
	checkSafe(t, []any{(*DefaultV1)(nil)}, []any{(*DefaultV2)(nil)})
}

func TestSQLDiff_NumericWithPrecisionIsSafe(t *testing.T) {
	t.Parallel()
	checkSafe(t, nil, []any{(*Money)(nil)})
}

func TestSQLDiff_InvalidMigrationErrorIsDistinguishable(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := ctxT(t)
	v1 := e.started(t, (*ViewedV1)(nil))
	if err := v1.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	execSQL(t, v1, "CREATE VIEW viewed_codes AS SELECT code FROM viewed")
	_ = v1.Stop(ctx)
	v2 := e.started(t, (*ViewedV2)(nil))
	files, snap := e.files("*"), snapshot(t, v2)
	err := v2.Migrate(ctx)
	if !errors.Is(err, ecsql.ErrInvalidMigration) {
		t.Fatalf("Migrate = %v, want ErrInvalidMigration", err)
	}
	if got := e.files("*"); !slices.Equal(got, files) {
		t.Fatalf("invalid migration saved: %v", got)
	}
	if snapshot(t, v2) != snap {
		t.Fatal("invalid migration changed the database")
	}
}

// Statement order must not depend on Go map iteration: regenerating the same
// change many times always gives a migration that applies.
func TestSQLDiff_StatementOrderDoesNotDependOnMapIteration(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s := e.started(t, chainModels...)
	ctx := ctxT(t)
	for i := range 30 {
		if err := s.Migrate(ctx); err != nil {
			t.Fatalf("generation %d: %v", i, err)
		}
		if err := s.Rollback(ctx); err != nil {
			t.Fatalf("generation %d rollback: %v", i, err)
		}
		for _, f := range e.files("*") {
			_ = os.Remove(f)
		}
	}
}
