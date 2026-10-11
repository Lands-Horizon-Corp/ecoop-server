//go:build integration

package sql_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	ecsql "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sql"
)

// Poison pill: hostile names, broken migration files and bad models must
// fail loudly without crashing and leave the database as it was.

func TestPoison_CreateCannotEscapeTheDirectory(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s := e.service()
	for _, name := range []string{"../../etc/passwd", `..\..\win`, "/abs/path", "a/../../b", "x\x00y", "drop table; --", "<script>"} {
		if err := s.Create(ctxT(t), name); err != nil {
			t.Fatalf("Create(%q): %v", name, err)
		}
	}
	for _, f := range e.files("*") {
		if filepath.Dir(f) != e.path {
			t.Fatalf("file escaped: %s", f)
		}
		base := strings.TrimSuffix(filepath.Base(f), ".sql")
		if strings.ContainsAny(base, `/\. <>;`+"\x00") {
			t.Fatalf("unsafe file name %q", base)
		}
	}
	if n := len(e.files("*")); n != 7 {
		t.Fatalf("files = %d, want 7", n)
	}
	if entries, _ := os.ReadDir(filepath.Dir(e.path)); len(entries) != 1 {
		t.Fatalf("parent of the migrations dir changed: %v", entries)
	}
}

func TestPoison_VeryLongNameFailsCleanly(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	err := e.service().Create(ctxT(t), strings.Repeat("a", 5000))
	if err == nil {
		t.Fatal("Create with a 5000-char name succeeded")
	}
}

// A migration whose second statement fails must roll back the first, keep
// the version, and apply once fixed.
func TestPoison_FailedMigrationIsAtomic(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.write(t, 1, "ok", "CREATE TABLE a (id int);", "DROP TABLE a;")
	e.write(t, 2, "bad", "CREATE TABLE b (id int);\nINSERT INTO nope VALUES (1);", "DROP TABLE b;")
	s := e.started(t)
	ctx := ctxT(t)
	if err := s.Migrate(ctx); err == nil {
		t.Fatal("Migrate with a broken migration succeeded")
	}
	if hasTable(t, s, "b") {
		t.Fatal("half of the failed migration was kept")
	}
	if v := version(t, s, e); v != "1" {
		t.Fatalf("version = %s, want 1", v)
	}
	e.write(t, 2, "bad", "CREATE TABLE b (id int);", "DROP TABLE b;")
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate after fix: %v", err)
	}
	if !hasTable(t, s, "b") {
		t.Fatal("fixed migration not applied")
	}
}

func TestPoison_MalformedFilesAreRejected(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"1_no_annotation.sql":  "CREATE TABLE a (id int);",
		"1_unclosed_block.sql": "-- +goose Up\n-- +goose StatementBegin\nCREATE TABLE a (id int);\n",
		"1_garbage.sql":        "\x00\xff\xfe binary junk",
		"1_only_down.sql":      "-- +goose Down\nDROP TABLE a;",
	}
	for file, body := range cases {
		t.Run(file, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			if err := os.WriteFile(filepath.Join(e.path, file), []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
			s := e.started(t)
			var err error
			mustNotPanic(t, "Migrate", func() { err = s.Migrate(ctxT(t)) })
			if err == nil && hasTable(t, s, "a") {
				t.Fatal("malformed migration created a table")
			}
			if len(tables(t, s)) != 0 {
				t.Fatalf("tables = %v, want none", tables(t, s))
			}
		})
	}
}

func TestPoison_DuplicateVersionsRefusedBeforeRunning(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.write(t, 5, "a", "CREATE TABLE a (id int);", "DROP TABLE a;")
	e.write(t, 5, "b", "CREATE TABLE b (id int);", "DROP TABLE b;")
	s := e.started(t)
	if err := s.Migrate(ctxT(t)); err == nil {
		t.Fatal("duplicate versions accepted")
	}
	if got := tables(t, s); len(got) != 0 {
		t.Fatalf("tables = %v; nothing may run", got)
	}
}

// An older-numbered migration arriving after a newer one was applied (a
// merge problem) is refused, then accepted once renumbered.
func TestPoison_OutOfOrderMigrationRefused(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.write(t, 10, "new", "CREATE TABLE n (id int);", "DROP TABLE n;")
	s := e.started(t)
	ctx := ctxT(t)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	e.write(t, 5, "old", "CREATE TABLE o (id int);", "DROP TABLE o;")
	if err := s.Migrate(ctx); err == nil {
		t.Fatal("out-of-order migration accepted")
	}
	if hasTable(t, s, "o") {
		t.Fatal("out-of-order migration ran")
	}
	if err := os.Rename(filepath.Join(e.path, "5_old.sql"), filepath.Join(e.path, "11_old.sql")); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err != nil || !hasTable(t, s, "o") {
		t.Fatalf("renumbered migration: %v", err)
	}
}

func TestPoison_StrayFilesAreIgnored(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.write(t, 1, "a", "CREATE TABLE a (id int);", "DROP TABLE a;")
	for name, body := range map[string]string{
		"README.md": "# notes", ".DS_Store": "\x00", "2_x.sql" + ecsql.ReviewSuffix: "-- +goose Up\nCREATE TABLE r (id int);",
	} {
		_ = os.WriteFile(filepath.Join(e.path, name), []byte(body), 0o644)
	}
	s := e.started(t)
	if err := s.Migrate(ctxT(t)); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if got := tables(t, s); len(got) != 1 || got[0] != "a" {
		t.Fatalf("tables = %v, want [a]", got)
	}
}

func TestPoison_BadModels(t *testing.T) {
	t.Parallel()
	for name, models := range map[string][]any{
		"nil interface": {nil},
		"int":           {42},
		"string":        {"members"},
		"nil map":       {map[string]any(nil)},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			s := e.started(t, models...)
			var err error
			mustNotPanic(t, "Migrate", func() { err = s.Migrate(ctxT(t)) })
			if err == nil {
				t.Fatal("Migrate with a bad model succeeded")
			}
			if got := e.files("*"); len(got) != 0 {
				t.Fatalf("files written: %v", got)
			}
		})
	}
}

func TestPoison_HostileDSN(t *testing.T) {
	t.Parallel()
	for _, dsn := range []string{"not a dsn", "postgres://", "mysql://u:p@h/db", "postgres://u:p@[::1:5432/db", "\x00"} {
		s := ecsql.NewSQLService(dsn, 1, 1, nil, false, nil, nil, nil)
		var err error
		mustNotPanic(t, "Start("+dsn+")", func() { err = s.Start(ctxShort(t)) })
		if err == nil {
			_ = s.Stop(ctxT(t))
			t.Errorf("Start(%q) succeeded", dsn)
		}
	}
}

// A directory named like a migration is refused by goose, without a panic
// and without applying anything.
func TestPoison_DirectoryNamedLikeAMigration(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	_ = os.Mkdir(filepath.Join(e.path, "1_dir.sql"), 0o755)
	s := e.started(t)
	var err error
	mustNotPanic(t, "Migrate", func() { err = s.Migrate(ctxT(t)) })
	if err == nil {
		t.Fatal("directory accepted as a migration")
	}
}
