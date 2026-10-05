package regressions

import (
	"errors"
	"os"
	"strings"
	"sync"
	"testing"

	sqlsvc "github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"
)

func TestSQLService_Lifecycle(t *testing.T) {
	e := newSQLEnv(t)

	t.Run("everything fails cleanly before Run", func(t *testing.T) {
		svc := e.newService()
		if svc.Client() != nil {
			t.Error("Client() before Run should be nil")
		}
		calls := map[string]error{
			"Ping":          svc.Ping(bg),
			"Migrate":       svc.Migrate(bg),
			"Rollback":      svc.Rollback(bg),
			"RollbackTo":    svc.RollbackTo(bg, 0),
			"Redo":          svc.Redo(bg),
			"Status":        svc.Status(bg),
			"Version":       svc.Version(bg),
			"Fresh":         svc.Fresh(bg),
			"RollbackSteps": svc.RollbackSteps(bg, 1),
			"UpSteps":       svc.UpSteps(bg, 1),
		}
		_, calls["Diff"] = svc.Diff(bg, "x")
		for name, err := range calls {
			if !errors.Is(err, sqlsvc.ErrNotInitialized) {
				t.Errorf("%s before Run = %v; want ErrNotInitialized", name, err)
			}
		}
		if err := svc.Stop(bg); err != nil {
			t.Errorf("Stop before Run = %v; want nil", err)
		}
	})

	t.Run("Run connects and Stop makes it unusable again", func(t *testing.T) {
		svc := e.newService()
		if err := svc.Run(bg); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if svc.Client() == nil {
			t.Fatal("Client() after Run is nil")
		}
		if err := svc.Ping(bg); err != nil {
			t.Fatalf("Ping: %v", err)
		}
		if err := svc.Stop(bg); err != nil {
			t.Fatalf("Stop: %v", err)
		}
		if err := svc.Ping(bg); !errors.Is(err, sqlsvc.ErrNotInitialized) {
			t.Errorf("Ping after Stop = %v; want ErrNotInitialized", err)
		}
		if err := svc.Migrate(bg); !errors.Is(err, sqlsvc.ErrNotInitialized) {
			t.Errorf("Migrate after Stop = %v; want ErrNotInitialized", err)
		}
		if err := svc.Stop(bg); err != nil {
			t.Errorf("second Stop = %v; want nil", err)
		}
	})

	t.Run("Run can be repeated after Stop", func(t *testing.T) {
		svc := e.newService()
		for i := range 3 {
			if err := svc.Run(bg); err != nil {
				t.Fatalf("Run #%d: %v", i+1, err)
			}
			if err := svc.Ping(bg); err != nil {
				t.Fatalf("Ping #%d: %v", i+1, err)
			}
			if err := svc.Stop(bg); err != nil {
				t.Fatalf("Stop #%d: %v", i+1, err)
			}
		}
	})

	t.Run("Run fails when the database is unreachable", func(t *testing.T) {
		svc := sqlsvc.NewSQLService("postgres://u:p@"+closedAddr(t)+"/db?sslmode=disable&connect_timeout=2", 1, 1, nil, true, nil, nil)
		if err := svc.Run(bg); err == nil {
			_ = svc.Stop(bg)
			t.Fatal("Run succeeded against a closed port")
		}
		if err := svc.Ping(bg); !errors.Is(err, sqlsvc.ErrNotInitialized) {
			t.Errorf("Ping after failed Run = %v; want ErrNotInitialized", err)
		}
	})

	t.Run("Run fails on a malformed DSN", func(t *testing.T) {
		svc := sqlsvc.NewSQLService("not a dsn ://", 1, 1, nil, true, nil, nil)
		if err := svc.Run(bg); err == nil {
			_ = svc.Stop(bg)
			t.Fatal("Run succeeded with a malformed DSN")
		}
	})
}

func TestSQLService_RunAppliesPendingMigrations(t *testing.T) {
	e := newSQLEnv(t)
	e.write(1, "users", `CREATE TABLE users (id bigserial PRIMARY KEY, email text NOT NULL);`, `DROP TABLE users;`)
	e.write(2, "posts", `CREATE TABLE posts (id bigserial PRIMARY KEY, user_id bigint NOT NULL REFERENCES users(id));`, `DROP TABLE posts;`)

	e.running()

	if !e.hasTable("users") || !e.hasTable("posts") {
		t.Fatal("Run did not apply the pending migrations")
	}
	if v := e.appliedVersion(); v != 2 {
		t.Fatalf("applied version = %d; want 2", v)
	}
}

func TestSQLService_WithAutoMigrateDisabledLeavesMigrationsPending(t *testing.T) {
	e := newSQLEnv(t)
	e.write(1, "users", `CREATE TABLE users (id bigserial PRIMARY KEY);`, `DROP TABLE users;`)

	svc := e.newServiceWith(false)
	if err := svc.Run(bg); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Cleanup(func() { _ = svc.Stop(bg) })

	if e.hasTable("users") || e.appliedVersion() != 0 {
		t.Fatal("Run applied migrations even though auto-migrate is off")
	}
	e.resetStatusOutput()
	if err := svc.Status(bg); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(e.statusOutput(), "Pending") {
		t.Fatalf("Status should list the migration as pending:\n%s", e.statusOutput())
	}
	if err := svc.Migrate(bg); err != nil {
		t.Fatal(err)
	}
	if !e.hasTable("users") {
		t.Fatal("explicit Migrate did not apply the migration")
	}
}

func TestSQLService_RunWithNoMigrationsIsFine(t *testing.T) {
	e := newSQLEnv(t)
	svc := e.running()
	if err := svc.Migrate(bg); err != nil {
		t.Fatalf("Migrate with no migrations = %v; want nil", err)
	}
	e.resetStatusOutput()
	if err := svc.Status(bg); err != nil {
		t.Fatalf("Status with no migrations = %v; want nil", err)
	}
	if err := svc.Version(bg); err != nil {
		t.Fatalf("Version with no migrations = %v; want nil", err)
	}
	if out := e.statusOutput(); !strings.Contains(out, "no migrations found") || !strings.Contains(out, "database version: 0") {
		t.Fatalf("unexpected output for an empty project:\n%s", out)
	}
	if err := svc.Fresh(bg); err != nil {
		t.Fatalf("Fresh with no migrations = %v; want nil", err)
	}
}

func TestSQLService_RunWithMissingMigrationsDirectory(t *testing.T) {
	e := newSQLEnv(t)
	svc := e.newService()
	if err := os.RemoveAll("src"); err != nil { // the directory vanishes after the service was built
		t.Fatal(err)
	}
	if err := svc.Run(bg); err != nil {
		t.Fatalf("Run without a migrations directory = %v; want it treated as no migrations", err)
	}
	_ = svc.Stop(bg)
}

// A service built without a migrations directory is a plain database connection.
func TestSQLService_WithoutMigrationsDirectoryOnlyConnects(t *testing.T) {
	e := newSQLEnv(t)
	e.write(1, "users", `CREATE TABLE users (id int);`, `DROP TABLE users;`)

	svc := sqlsvc.NewSQLService(e.dsn, 2, 5, nil, true, nil, []any{(*dOrg)(nil)})
	if err := svc.Run(bg); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Cleanup(func() { _ = svc.Stop(bg) })
	if err := svc.Ping(bg); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if e.hasTable("users") {
		t.Fatal("Run applied migrations without a migrations directory")
	}

	_, diffErr := svc.Diff(bg, "x")
	calls := map[string]error{
		"Migrate":    svc.Migrate(bg),
		"Status":     svc.Status(bg),
		"Version":    svc.Version(bg),
		"Rollback":   svc.Rollback(bg),
		"UpSteps":    svc.UpSteps(bg, 1),
		"Fresh":      svc.Fresh(bg),
		"Create":     svc.Create(bg, "x"),
		"Diff":       diffErr,
		"RollbackTo": svc.RollbackTo(bg, 0),
	}
	for name, err := range calls {
		if !errors.Is(err, sqlsvc.ErrNoMigrationsDir) {
			t.Errorf("%s = %v; want ErrNoMigrationsDir", name, err)
		}
	}
}

// The migrations argument must be a directory.
func TestSQLService_MigrationsPathMustBeADirectory(t *testing.T) {
	e := newSQLEnv(t)
	notADir, err := os.CreateTemp(t.TempDir(), "file-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = notADir.Close() })

	svc := sqlsvc.NewSQLService(e.dsn, 2, 5, notADir, true, nil, nil)
	if err := svc.Run(bg); !errors.Is(err, sqlsvc.ErrInvalidMigrationsDir) {
		_ = svc.Stop(bg)
		t.Fatalf("Run with a regular file = %v; want ErrInvalidMigrationsDir", err)
	}
	if err := svc.Create(bg, "x"); !errors.Is(err, sqlsvc.ErrInvalidMigrationsDir) {
		t.Fatalf("Create with a regular file = %v; want ErrInvalidMigrationsDir", err)
	}
}

func TestSQLService_MigrateAndRollbackOperations(t *testing.T) {
	e := newSQLEnv(t)
	e.write(1, "users", `CREATE TABLE users (id bigserial PRIMARY KEY, email text NOT NULL);`, `DROP TABLE users;`)
	e.write(2, "posts", `CREATE TABLE posts (id bigserial PRIMARY KEY, user_id bigint NOT NULL REFERENCES users(id));`, `DROP TABLE posts;`)
	e.write(3, "users_name", `ALTER TABLE users ADD COLUMN name text;`, `ALTER TABLE users DROP COLUMN name;`)
	e.write(4, "posts_index", `CREATE INDEX posts_user_idx ON posts (user_id);`, `DROP INDEX posts_user_idx;`)

	// Start without applying anything: UpSteps drives it one migration at a time.
	svc := e.newService()
	if err := svc.Run(bg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Stop(bg) })
	if err := svc.Fresh(bg); err != nil { // Run already migrated; reset to empty then step manually
		t.Fatal(err)
	}
	if err := svc.RollbackTo(bg, 0); err != nil {
		t.Fatal(err)
	}
	if v := e.appliedVersion(); v != 0 {
		t.Fatalf("version after RollbackTo(0) = %d; want 0", v)
	}
	if e.hasTable("users") || e.hasTable("posts") {
		t.Fatal("tables remain after rolling everything back")
	}

	t.Run("UpSteps applies exactly n migrations", func(t *testing.T) {
		if err := svc.UpSteps(bg, 2); err != nil {
			t.Fatalf("UpSteps(2): %v", err)
		}
		if v := e.appliedVersion(); v != 2 {
			t.Fatalf("version = %d; want 2", v)
		}
		if !e.hasTable("posts") || e.hasColumn("users", "name") {
			t.Fatal("UpSteps(2) applied the wrong set")
		}
	})

	t.Run("Migrate applies the rest and is idempotent", func(t *testing.T) {
		if err := svc.Migrate(bg); err != nil {
			t.Fatalf("Migrate: %v", err)
		}
		if v := e.appliedVersion(); v != 4 {
			t.Fatalf("version = %d; want 4", v)
		}
		if err := svc.Migrate(bg); err != nil {
			t.Fatalf("second Migrate: %v", err)
		}
		if v := e.appliedVersion(); v != 4 {
			t.Fatalf("version after repeat Migrate = %d; want 4", v)
		}
	})

	t.Run("Rollback reverts only the newest", func(t *testing.T) {
		if err := svc.Rollback(bg); err != nil {
			t.Fatalf("Rollback: %v", err)
		}
		if v := e.appliedVersion(); v != 3 {
			t.Fatalf("version = %d; want 3", v)
		}
		if e.scanInt(`SELECT count(*) FROM pg_indexes WHERE indexname = 'posts_user_idx'`) != 0 {
			t.Fatal("index survived its rollback")
		}
		if !e.hasColumn("users", "name") {
			t.Fatal("Rollback reverted more than one migration")
		}
	})

	t.Run("RollbackSteps reverts n migrations", func(t *testing.T) {
		if err := svc.RollbackSteps(bg, 2); err != nil {
			t.Fatalf("RollbackSteps(2): %v", err)
		}
		if v := e.appliedVersion(); v != 1 {
			t.Fatalf("version = %d; want 1", v)
		}
		if e.hasTable("posts") || e.hasColumn("users", "name") {
			t.Fatal("RollbackSteps(2) left later changes behind")
		}
		if !e.hasTable("users") {
			t.Fatal("RollbackSteps(2) reverted too much")
		}
	})

	t.Run("RollbackTo stops at the requested version", func(t *testing.T) {
		if err := svc.Migrate(bg); err != nil {
			t.Fatal(err)
		}
		if err := svc.RollbackTo(bg, 2); err != nil {
			t.Fatalf("RollbackTo(2): %v", err)
		}
		if v := e.appliedVersion(); v != 2 {
			t.Fatalf("version = %d; want 2", v)
		}
		if !e.hasTable("posts") || e.hasColumn("users", "name") {
			t.Fatal("RollbackTo(2) left the wrong schema")
		}
	})

	t.Run("step counts must be positive", func(t *testing.T) {
		for _, n := range []int{0, -1, -100} {
			if err := svc.UpSteps(bg, n); !errors.Is(err, sqlsvc.ErrInvalidSteps) {
				t.Errorf("UpSteps(%d) = %v; want ErrInvalidSteps", n, err)
			}
			if err := svc.RollbackSteps(bg, n); !errors.Is(err, sqlsvc.ErrInvalidSteps) {
				t.Errorf("RollbackSteps(%d) = %v; want ErrInvalidSteps", n, err)
			}
		}
	})

	t.Run("asking for more steps than exist fails before changing anything", func(t *testing.T) {
		if err := svc.Migrate(bg); err != nil {
			t.Fatal(err)
		}
		if err := svc.UpSteps(bg, 1); !errors.Is(err, sqlsvc.ErrTooManySteps) {
			t.Errorf("UpSteps with nothing pending = %v; want ErrTooManySteps", err)
		}
		if err := svc.RollbackSteps(bg, 99); !errors.Is(err, sqlsvc.ErrTooManySteps) {
			t.Errorf("RollbackSteps(99) with 4 applied = %v; want ErrTooManySteps", err)
		}
		if v := e.appliedVersion(); v != 4 {
			t.Fatalf("version = %d; want 4: a refused request must not roll anything back", v)
		}
		if err := svc.RollbackSteps(bg, 4); err != nil {
			t.Fatalf("RollbackSteps(4) with exactly 4 applied: %v", err)
		}
		if v := e.appliedVersion(); v != 0 {
			t.Fatalf("version = %d; want 0", v)
		}
		if err := svc.UpSteps(bg, 5); !errors.Is(err, sqlsvc.ErrTooManySteps) {
			t.Errorf("UpSteps(5) with 4 pending = %v; want ErrTooManySteps", err)
		}
		if v := e.appliedVersion(); v != 0 {
			t.Fatalf("version = %d; want 0 after a refused UpSteps", v)
		}
	})

	t.Run("Redo reverts and re-applies the newest migration", func(t *testing.T) {
		if err := svc.Migrate(bg); err != nil {
			t.Fatal(err)
		}
		e.exec(`INSERT INTO users (email) VALUES ('a@x.io')`)
		e.exec(`INSERT INTO posts (user_id) VALUES (1)`)
		e.exec(`DROP INDEX posts_user_idx`) // proves Redo's Down+Up recreated it, not that it was left alone
		e.exec(`CREATE INDEX posts_user_idx ON posts (user_id)`)

		if err := svc.Redo(bg); err != nil {
			t.Fatalf("Redo: %v", err)
		}
		if v := e.appliedVersion(); v != 4 {
			t.Fatalf("version after Redo = %d; want 4", v)
		}
		if e.scanInt(`SELECT count(*) FROM pg_indexes WHERE indexname = 'posts_user_idx'`) != 1 {
			t.Fatal("index missing after Redo")
		}
		if e.scanInt(`SELECT count(*) FROM posts`) != 1 {
			t.Fatal("Redo of an index migration lost table data")
		}
	})

	t.Run("Fresh rebuilds from zero and discards data", func(t *testing.T) {
		e.exec(`INSERT INTO users (email) VALUES ('b@x.io')`)
		if err := svc.Fresh(bg); err != nil {
			t.Fatalf("Fresh: %v", err)
		}
		if v := e.appliedVersion(); v != 4 {
			t.Fatalf("version after Fresh = %d; want 4", v)
		}
		if e.scanInt(`SELECT count(*) FROM users`) != 0 {
			t.Fatal("Fresh kept data; it must rebuild from migrations")
		}
	})
}

func TestSQLService_StatusAndVersionOutput(t *testing.T) {
	e := newSQLEnv(t)
	e.write(1, "first", `CREATE TABLE a (id int);`, `DROP TABLE a;`)
	e.write(2, "second", `CREATE TABLE b (id int);`, `DROP TABLE b;`)
	svc := e.running()

	e.resetStatusOutput()
	if err := svc.Version(bg); err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(e.statusOutput()); got != "database version: 2" {
		t.Fatalf("Version output = %q", got)
	}

	if err := svc.RollbackSteps(bg, 1); err != nil {
		t.Fatal(err)
	}
	e.resetStatusOutput()
	if err := svc.Status(bg); err != nil {
		t.Fatal(err)
	}
	out := e.statusOutput()
	for _, want := range []string{"Applied At", "00001_first.sql", "00002_second.sql"} {
		if !strings.Contains(out, want) {
			t.Errorf("Status output missing %q:\n%s", want, out)
		}
	}
	lines := strings.Split(out, "\n")
	var first, second string
	for _, l := range lines {
		switch {
		case strings.Contains(l, "00001_first.sql"):
			first = l
		case strings.Contains(l, "00002_second.sql"):
			second = l
		}
	}
	if strings.Contains(first, "Pending") {
		t.Errorf("applied migration reported as pending: %q", first)
	}
	if !strings.Contains(second, "Pending") {
		t.Errorf("rolled-back migration not reported as pending: %q", second)
	}
}

func TestSQLService_Create(t *testing.T) {
	e := newSQLEnv(t)
	svc := e.running()

	if err := svc.Create(bg, "add_orders"); err != nil {
		t.Fatalf("Create: %v", err)
	}
	files := e.migrationFiles()
	if len(files) != 1 || !strings.HasSuffix(files[0], "_add_orders.sql") {
		t.Fatalf("migration files = %v; want one *_add_orders.sql", files)
	}
	content := readFile(t, files[0])
	if !strings.Contains(content, "-- +goose Up") || !strings.Contains(content, "-- +goose Down") {
		t.Fatalf("scaffold lacks goose Up/Down markers:\n%s", content)
	}

	// The scaffold must be applicable as-is, and visible to a service that is already running.
	if err := svc.Migrate(bg); err != nil {
		t.Fatalf("Migrate of the scaffold: %v", err)
	}
	if e.appliedVersion() == 0 {
		t.Fatal("scaffold was not applied")
	}

	t.Run("creates the directory when missing", func(t *testing.T) {
		if err := os.RemoveAll("src"); err != nil {
			t.Fatal(err)
		}
		if err := svc.Create(bg, "again"); err != nil {
			t.Fatalf("Create without a directory: %v", err)
		}
		if len(e.migrationFiles()) != 1 {
			t.Fatalf("migration files = %v", e.migrationFiles())
		}
	})

	t.Run("a hostile name cannot write outside the migrations directory", func(t *testing.T) {
		_ = svc.Create(bg, "../../escape")
		var escaped []string
		_ = walkFiles(".", func(path string) {
			if strings.Contains(path, "escape") && !strings.HasPrefix(path, e.dir) {
				escaped = append(escaped, path)
			}
		})
		if len(escaped) != 0 {
			t.Fatalf("files created outside the migrations directory: %v", escaped)
		}
	})
}

func TestSQLService_FailedMigrationsAreAtomic(t *testing.T) {
	e := newSQLEnv(t)
	e.write(1, "ok", `CREATE TABLE ok_table (id int);`, `DROP TABLE ok_table;`)
	// The first statement is valid; the second fails, so the whole migration must roll back.
	e.write(2, "broken", "CREATE TABLE half_done (id int);\nINSERT INTO does_not_exist VALUES (1);", `DROP TABLE half_done;`)

	svc := e.newService()
	if err := svc.Run(bg); err == nil {
		_ = svc.Stop(bg)
		t.Fatal("Run succeeded with a broken migration; want an error")
	}
	if !e.hasTable("ok_table") || e.appliedVersion() != 1 {
		t.Fatalf("migration 1 should stay applied (version=%d)", e.appliedVersion())
	}
	if e.hasTable("half_done") {
		t.Fatal("a half-applied migration left a table behind; migrations must be transactional")
	}

	// Fixing the file lets the same service path succeed afterwards.
	e.write(2, "broken", `CREATE TABLE half_done (id int);`, `DROP TABLE half_done;`)
	svc = e.running()
	if !e.hasTable("half_done") || e.appliedVersion() != 2 {
		t.Fatalf("fixed migration not applied (version=%d)", e.appliedVersion())
	}
}

func TestSQLService_RefusesOutOfOrderMigrations(t *testing.T) {
	e := newSQLEnv(t)
	e.write(1, "a", `CREATE TABLE a (id int);`, `DROP TABLE a;`)
	e.write(3, "c", `CREATE TABLE c (id int);`, `DROP TABLE c;`)
	svc := e.running()

	// A teammate's older migration arrives after a newer one was applied (a classic merge problem).
	e.write(2, "b", `CREATE TABLE b (id int);`, `DROP TABLE b;`)
	if err := svc.Migrate(bg); err == nil {
		t.Fatal("Migrate applied a migration older than the current version; want an error")
	}
	if e.hasTable("b") {
		t.Fatal("the out-of-order migration was applied despite the error")
	}
	if v := e.appliedVersion(); v != 3 {
		t.Fatalf("version = %d; want it unchanged at 3", v)
	}
}

func TestSQLService_ConcurrentStartupIsSafe(t *testing.T) {
	e := newSQLEnv(t)
	for i := int64(1); i <= 6; i++ {
		e.write(i, "t"+string(rune('a'+i)), `CREATE TABLE t`+string(rune('a'+i))+` (id int);`, `DROP TABLE t`+string(rune('a'+i))+`;`)
	}

	// Several replicas booting at once all call Run, which migrates.
	const replicas = 6
	var wg sync.WaitGroup
	errs := make([]error, replicas)
	svcs := make([]sqlsvc.SQLServices, replicas)
	start := make(chan struct{})
	for i := range replicas {
		svcs[i] = e.newService()
		wg.Go(func() {
			<-start
			errs[i] = svcs[i].Run(bg)
		})
	}
	close(start)
	wg.Wait()
	for _, s := range svcs {
		defer func() { _ = s.Stop(bg) }()
	}

	for i, err := range errs {
		if err != nil {
			t.Errorf("replica %d failed to start: %v", i, err)
		}
	}
	if v := e.appliedVersion(); v != 6 {
		t.Fatalf("version = %d; want 6", v)
	}
	if n := e.scanInt(`SELECT count(*) FROM goose_db_version WHERE version_id > 0`); n != 6 {
		t.Fatalf("goose recorded %d applied versions; want exactly 6 (no duplicates)", n)
	}
}
