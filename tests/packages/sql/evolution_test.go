package sql_test

import (
	"strconv"
	"testing"
)

// Schema evolution: a product's twelve releases, mixing migrations
// generated from models with hand-written indexes, backfills and seed data.

// evoStep is one release: models to Migrate to, or a hand-written migration.
type evoStep struct {
	models   []any
	up, down string
}

var evoReleases = []evoStep{
	{models: []any{(*EvoV1)(nil)}},
	{up: "CREATE INDEX evo_name_idx ON evo (name);", down: "DROP INDEX evo_name_idx;"},
	{models: []any{(*EvoV2)(nil)}},
	{up: "UPDATE evo SET email = lower(name) || '@evo.test' WHERE email IS NULL;", down: "SELECT 1;"},
	{models: []any{(*EvoV3)(nil)}},
	{up: "CREATE INDEX evo_joined_idx ON evo (joined_at) WHERE joined_at IS NOT NULL;", down: "DROP INDEX evo_joined_idx;"},
	{models: []any{(*EvoV4)(nil)}},
	{up: "UPDATE evo SET score = length(name);", down: "SELECT 1;"},
	{up: "CREATE INDEX evo_score_idx ON evo (score DESC);", down: "DROP INDEX evo_score_idx;"},
	{models: []any{(*EvoV4)(nil), (*DiffCustomer)(nil), (*DiffAccount)(nil)}},
	{up: "INSERT INTO diff_customers (name) SELECT name FROM evo;", down: "DELETE FROM diff_customers;"},
	{up: "CREATE INDEX diff_accounts_customer_idx ON diff_accounts (customer_id);", down: "DROP INDEX diff_accounts_customer_idx;"},
}

var finalEvoModels = evoReleases[9].models

// evolve applies releases [from, to) to e (1-based count of releases done is
// from), inserting rows into evo after each, and returns the rows inserted.
func evolve(t *testing.T, e *env, from, to int) int {
	t.Helper()
	ctx := ctxT(t)
	rows := 0
	for i := from; i < to; i++ {
		r := evoReleases[i]
		s := e.started(t, r.models...)
		if r.models == nil {
			e.write(t, nextFileVersion(e), "release_"+strconv.Itoa(i+1), r.up, r.down)
		}
		if err := s.Migrate(ctx); err != nil {
			t.Fatalf("release %d: %v", i+1, err)
		}
		execSQL(t, s, "INSERT INTO evo (name) VALUES (?)", "Row"+strconv.Itoa(i))
		rows++
		_ = s.Stop(ctx)
	}
	return rows
}

// nextFileVersion is a version after every file in e.
func nextFileVersion(e *env) int64 {
	var v int64 = 1
	for _, f := range e.files("*") {
		if fv := versionOf(f); fv >= v {
			v = fv + 1
		}
	}
	return v
}

func TestSQLEvolution_TwelveReleasesWithDataAndHandWrittenIndexes(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	rows := evolve(t, e, 0, len(evoReleases))
	s := e.started(t, finalEvoModels...)
	if n := count(t, s, "SELECT count(*) FROM evo"); n != rows {
		t.Fatalf("rows = %d, want %d", n, rows)
	}
	if n := count(t, s, "SELECT count(*) FROM pg_indexes WHERE indexname IN ('evo_name_idx','evo_joined_idx','evo_score_idx','diff_accounts_customer_idx')"); n != 4 {
		t.Fatalf("hand-written indexes = %d, want 4", n)
	}
	// Converged: the models match, so Migrate writes nothing (and keeps the
	// hand-written indexes).
	files := len(e.files("*"))
	if err := s.Migrate(ctxT(t)); err != nil || len(e.files("*")) != files {
		t.Fatalf("not converged: %v, files %d → %d", err, files, len(e.files("*")))
	}
}

func TestSQLEvolution_ProductionBehindByNineReleasesUpgradesWithItsData(t *testing.T) {
	t.Parallel()
	dev := newEnv(t)
	evolve(t, dev, 0, len(evoReleases))

	// Production deployed release 3's files and has its own data.
	prod := newEnv(t)
	copySome(t, dev, prod, releaseFiles(t, dev, 3))
	p := prod.started(t)
	ctx := ctxT(t)
	if err := p.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	for i := range 50 {
		execSQL(t, p, "INSERT INTO evo (name, email) VALUES (?, ?)", "Prod"+strconv.Itoa(i), "p"+strconv.Itoa(i)+"@prod.test")
	}
	copyMigrations(t, dev, prod)
	if err := p.Migrate(ctx); err != nil {
		t.Fatalf("upgrading nine releases: %v", err)
	}
	if n := count(t, p, "SELECT count(*) FROM evo"); n != 50 {
		t.Fatalf("rows = %d", n)
	}
	if n := count(t, p, "SELECT count(*) FROM evo WHERE email LIKE 'p%@prod.test' AND score = length(name)"); n != 50 {
		t.Fatalf("backfills did not run on production's data: %d", n)
	}
	if n := count(t, p, "SELECT count(*) FROM diff_customers"); n != 50 {
		t.Fatalf("seed copied %d customers", n)
	}
	devSvc := dev.started(t)
	if a, b := snapshot(t, devSvc), snapshot(t, p); a != b {
		t.Fatalf("production schema differs from development:\n%s", diffLines(a, b))
	}
}

func TestSQLEvolution_ReplayFromScratchThenRollBackEveryMigration(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	evolve(t, e, 0, len(evoReleases))
	s := e.started(t)
	want := snapshot(t, s)
	if got := replayAndRollBack(t, e); got != want {
		t.Fatalf("replay differs:\n%s", diffLines(want, got))
	}
}

// releaseFiles is how many of e's files the first n evolution releases
// wrote: one per hand-written release, and the generated files of model
// releases (two for EvoV4, whose NOT NULL column needs a second pass).
func releaseFiles(t *testing.T, e *env, n int) int {
	t.Helper()
	probe := newEnv(t)
	evolve(t, probe, 0, n)
	return len(probe.files("*.sql"))
}
