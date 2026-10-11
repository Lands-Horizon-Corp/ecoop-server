package sql_test

import (
	"os"
	"slices"
	"strings"
	"testing"

	ecsql "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sql"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Happy path: every command doing what it says on a healthy database.

func TestHappy_MigrateGeneratesAndApplies(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s := e.started(t, (*MemberV1)(nil), (*Loan)(nil))
	if err := s.Migrate(ctxT(t)); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if got := e.files("*_auto.sql"); len(got) != 1 {
		t.Fatalf("auto files = %v, want 1", got)
	}
	if got := tables(t, s); !slices.Equal(got, []string{"loans", "members"}) {
		t.Fatalf("tables = %v", got)
	}
}

func TestHappy_MigrateIsIdempotent(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s := e.started(t, (*MemberV1)(nil))
	for range 3 {
		if err := s.Migrate(ctxT(t)); err != nil {
			t.Fatal(err)
		}
	}
	if got := e.files("*.sql*"); len(got) != 1 {
		t.Fatalf("files after 3 Migrates = %v, want 1", got)
	}
}

func TestHappy_ModelEvolutionAddsColumnAndKeepsRows(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	v1 := e.started(t, (*MemberV1)(nil))
	ctx := ctxT(t)
	if err := v1.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := v1.Client().NewInsert().Model(&MemberV1{Name: "Ana"}).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	_ = v1.Stop(ctx)

	v2 := e.started(t, (*MemberV2)(nil))
	if err := v2.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if got := columns(t, v2, "members"); !slices.Equal(got, []string{"email", "id", "name"}) {
		t.Fatalf("columns = %v", got)
	}
	n, err := v2.Client().NewSelect().Model((*MemberV2)(nil)).Count(ctx)
	if err != nil || n != 1 {
		t.Fatalf("rows = %d, %v; want the row to survive", n, err)
	}
	if got := e.files("*_auto.sql"); len(got) != 2 {
		t.Fatalf("auto files = %v, want 2", got)
	}
}

func TestHappy_CreateWritesOrderedTemplates(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s := e.service()
	ctx := ctxT(t)
	for _, name := range []string{"Add Members", "add-loans", "seed"} {
		if err := s.Create(ctx, name); err != nil { // Create needs no Start
			t.Fatalf("Create(%q): %v", name, err)
		}
	}
	got := e.files("*.sql")
	if len(got) != 3 {
		t.Fatalf("files = %v", got)
	}
	for i, want := range []string{"_add_members.sql", "_add_loans.sql", "_seed.sql"} {
		if !strings.HasSuffix(got[i], want) {
			t.Fatalf("file %d = %s, want suffix %s (in creation order)", i, got[i], want)
		}
	}
	// The scaffold applies as is.
	started := e.started(t)
	if err := started.Migrate(ctx); err != nil {
		t.Fatalf("applying scaffolds: %v", err)
	}
}

func TestHappy_RunAppliesOnlyWhenAutoMigrate(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.write(t, 1, "members", "CREATE TABLE members (id int);", "DROP TABLE members;")
	ctx := ctxT(t)

	off := ecsql.NewSQLService(e.dsn, 2, 4, e.dir, false, e.out, []any{(*Loan)(nil)}, nil)
	if err := off.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer off.Stop(ctx)
	if err := off.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if hasTable(t, off, "members") {
		t.Fatal("Run applied migrations with autoMigrate off")
	}

	on := ecsql.NewSQLService(e.dsn, 2, 4, e.dir, true, e.out, []any{(*Loan)(nil)}, nil)
	if err := on.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer on.Stop(ctx)
	if err := on.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if !hasTable(t, on, "members") {
		t.Fatal("Run did not apply the pending migration")
	}
	if hasTable(t, on, "loans") || len(e.files("*_auto.sql")) != 0 {
		t.Fatal("Run generated from models; only Migrate may")
	}
}

func TestHappy_RollbackFamily(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	for v := int64(1); v <= 4; v++ {
		name := "t" + string(rune('0'+v))
		e.write(t, v, name, "CREATE TABLE "+name+" (id int);", "DROP TABLE "+name+";")
	}
	s := e.started(t)
	ctx := ctxT(t)
	step := func(name string, err error, want string) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got := version(t, s, e); got != want {
			t.Fatalf("after %s version = %s, want %s", name, got, want)
		}
	}
	step("UpSteps(2)", s.UpSteps(ctx, 2), "2")
	step("Migrate", s.Migrate(ctx), "4")
	step("Rollback", s.Rollback(ctx), "3")
	step("RollbackSteps(2)", s.RollbackSteps(ctx, 2), "1")
	step("UpSteps(3)", s.UpSteps(ctx, 3), "4")
	step("RollbackTo(2)", s.RollbackTo(ctx, 2), "2")
	step("Redo", s.Redo(ctx), "2")
	step("RollbackSteps(2)", s.RollbackSteps(ctx, 2), "0")
	if got := tables(t, s); len(got) != 0 {
		t.Fatalf("tables after full rollback = %v", got)
	}
	step("Rollback at 0", s.Rollback(ctx), "0")
}

func TestHappy_StatusAndVersion(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.write(t, 1, "a", "CREATE TABLE a (id int);", "DROP TABLE a;")
	e.write(t, 2, "b", "CREATE TABLE b (id int);", "DROP TABLE b;")
	s := e.started(t)
	ctx := ctxT(t)
	if err := s.UpSteps(ctx, 1); err != nil {
		t.Fatal(err)
	}
	e.out.Reset()
	if err := s.Status(ctx); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(e.out.String()), "\n")
	if len(lines) != 3 || !strings.Contains(lines[1], "applied") || !strings.Contains(lines[1], "1_a.sql") ||
		!strings.Contains(lines[2], "pending") || !strings.Contains(lines[2], "2_b.sql") {
		t.Fatalf("Status output:\n%s", e.out.String())
	}
	if v := version(t, s, e); v != "1" {
		t.Fatalf("Version = %s", v)
	}
}

func TestHappy_FreshWipesAndReapplies(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s := e.started(t, (*MemberV1)(nil))
	ctx := ctxT(t)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Client().NewInsert().Model(&MemberV1{Name: "x"}).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Client().ExecContext(ctx, "CREATE TABLE stray (id int)"); err != nil {
		t.Fatal(err)
	}
	if err := s.Fresh(ctx); err != nil {
		t.Fatalf("Fresh: %v", err)
	}
	if got := tables(t, s); !slices.Equal(got, []string{"members"}) {
		t.Fatalf("tables after Fresh = %v", got)
	}
	if n, _ := s.Client().NewSelect().Model((*MemberV1)(nil)).Count(ctx); n != 0 {
		t.Fatalf("rows after Fresh = %d", n)
	}
}

func TestHappy_DestructiveChangeWaitsForReview(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := ctxT(t)
	v2 := e.started(t, (*MemberV2)(nil))
	if err := v2.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	_ = v2.Stop(ctx)

	v1 := e.started(t, (*MemberV1)(nil)) // drops the email column
	for range 2 {
		if err := v1.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
	}
	review := e.files("*" + ecsql.ReviewSuffix)
	if len(review) != 1 {
		t.Fatalf("review files = %v, want exactly 1 after two Migrates", review)
	}
	if !slices.Contains(columns(t, v1, "members"), "email") {
		t.Fatal("destructive change was applied without review")
	}
	if !strings.Contains(e.out.String(), "for review") {
		t.Fatalf("no review note in output: %q", e.out.String())
	}
	if err := os.Rename(review[0], strings.TrimSuffix(review[0], ecsql.ReviewSuffix)); err != nil {
		t.Fatal(err)
	}
	if err := v1.Migrate(ctx); err != nil {
		t.Fatalf("Migrate after review: %v", err)
	}
	if slices.Contains(columns(t, v1, "members"), "email") {
		t.Fatal("reviewed migration did not drop the column")
	}
}

func TestHappy_RestartAfterStop(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s := e.service()
	ctx := ctxT(t)
	for i := range 3 {
		if err := s.Start(ctx); err != nil {
			t.Fatalf("Start %d: %v", i, err)
		}
		if err := s.Ping(ctx); err != nil {
			t.Fatalf("Ping %d: %v", i, err)
		}
		if err := s.Stop(ctx); err != nil {
			t.Fatalf("Stop %d: %v", i, err)
		}
	}
}

func TestHappy_SharedInflightAcrossServices(t *testing.T) {
	t.Parallel()
	shared := utils.NewInflight()
	a, b := newEnv(t), newEnv(t)
	ctx := ctxT(t)
	sa := ecsql.NewSQLService(a.dsn, 2, 4, a.dir, false, a.out, nil, shared)
	sb := ecsql.NewSQLService(b.dsn, 2, 4, b.dir, false, b.out, nil, shared)
	for _, s := range []ecsql.SQLServices{sa, sb} {
		if err := s.Start(ctx); err != nil {
			t.Fatal(err)
		}
		if err := s.Ping(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := sa.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := sb.Ping(ctx); err != nil {
		t.Fatalf("stopping a broke b: %v", err)
	}
	_ = sb.Stop(ctx)
	if shared.Count() != 0 {
		t.Fatalf("inflight count = %d after both stopped", shared.Count())
	}
}
