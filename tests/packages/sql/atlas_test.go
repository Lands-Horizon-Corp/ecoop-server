//go:build integration

package sql_test

import (
	"context"
	"errors"
	"net/url"
	"slices"
	"strings"
	"testing"

	ecsql "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sql"
)

// Planning with Atlas: what the Bun + Atlas + goose arrangement guarantees
// beyond generating the right SQL.

// Each database has its own service, folder and models; one process can
// migrate several without them seeing each other.
func TestAtlas_MultipleDatabasesKeepSeparateMigrations(t *testing.T) {
	t.Parallel()
	core, loans := newEnv(t), newEnv(t)
	ctx := ctxT(t)
	a := core.started(t, (*MemberV1)(nil))
	b := loans.started(t, (*Loan)(nil), (*Branch)(nil))
	errs := make(chan error, 2)
	go func() { errs <- a.Migrate(ctx) }()
	go func() { errs <- b.Migrate(ctx) }()
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if got := tables(t, a); !slices.Equal(got, []string{"members"}) {
		t.Fatalf("core tables = %v", got)
	}
	if got := tables(t, b); !slices.Equal(got, []string{"branches", "loans"}) {
		t.Fatalf("loans tables = %v", got)
	}
	if len(core.files("*")) != 1 || len(loans.files("*")) != 1 {
		t.Fatalf("files: core %v, loans %v", core.files("*"), loans.files("*"))
	}
}

// An index added by a hand-written migration but not declared on a model is
// not dropped unnoticed: the plan to drop it waits for review.
func TestAtlas_UndeclaredIndexWaitsForReview(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s := e.started(t, (*MemberV1)(nil))
	ctx := ctxT(t)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	e.write(t, nextFileVersion(e), "hand_index", "CREATE INDEX members_name_idx ON members (name);", "DROP INDEX members_name_idx;")
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	review := e.files("*" + ecsql.ReviewSuffix)
	if len(review) != 1 || !strings.Contains(readFile(t, review[0]), `DROP INDEX "members_name_idx"`) {
		t.Fatalf("review files = %v", review)
	}
	if count(t, s, "SELECT count(*) FROM pg_indexes WHERE indexname = 'members_name_idx'") != 1 {
		t.Fatal("the undeclared index was dropped")
	}
}

// Without CREATEDB there is nowhere to plan: Migrate says so and changes
// nothing; applying migrations (Run) needs no such right.
func TestAtlas_PlanningNeedsCreateDB(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	role := "plain_" + dbName(e.dsn)
	execAdmin(t, "CREATE ROLE "+role+" LOGIN PASSWORD 'pw'")
	t.Cleanup(func() { _, _ = admin.Exec("DROP ROLE IF EXISTS " + role) })
	execAdmin(t, "ALTER DATABASE "+dbName(e.dsn)+" OWNER TO "+role)
	dsn := strings.Replace(e.dsn, url.UserPassword(pgUser, pgPassword).String(), role+":pw", 1)
	s := ecsql.NewSQLService(dsn, 1, 2, e.dir, true, e.out, []any{(*MemberV1)(nil)}, nil)
	ctx := ctxT(t)
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer s.Stop(context.Background())
	if err := s.Migrate(ctx); !errors.Is(err, ecsql.ErrScratchDatabase) {
		t.Fatalf("Migrate = %v, want ErrScratchDatabase", err)
	}
	if len(e.files("*")) != 0 || len(tables(t, s)) != 0 {
		t.Fatal("Migrate changed something")
	}
	e.write(t, 1, "members", "CREATE TABLE members (id int);", "DROP TABLE members;")
	if err := s.Run(ctx); err != nil {
		t.Fatalf("Run without CREATEDB: %v", err)
	}
}

// A planning failure (a model Bun cannot create) still drops the scratch
// databases; TestMain checks none are left after the whole run.
func TestAtlas_FailedPlanningLeavesNoScratchDatabase(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s := e.started(t, (*Broken)(nil))
	if err := s.Migrate(ctxT(t)); !errors.Is(err, ecsql.ErrInvalidModel) {
		t.Fatalf("Migrate = %v, want ErrInvalidModel", err)
	}
	if len(e.files("*")) != 0 {
		t.Fatalf("files = %v", e.files("*"))
	}
}

func execAdmin(t *testing.T, q string) {
	t.Helper()
	if _, err := admin.Exec(q); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}
