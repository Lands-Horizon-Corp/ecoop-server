//go:build integration

package sql_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/uptrace/bun/driver/pgdriver"

	ecsql "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sql"
)

// The PostgreSQL foundation: 100 hand-written goose migrations
// (testdata/pg100) evolving users, accounts and transactions from a
// prototype to a partitioned, zero-downtime production schema, in four
// phases: foundations (1-20), growth and normalization (21-50), performance
// and advanced indexing (51-80), enterprise scale and zero downtime
// (81-100). The tests prove the history is reproducible from a blank
// database, reversible step by step, safe over 100,000 rows per table,
// lock-safe in its late phase, and appliable under live traffic.

var updatePG100Sums = flag.Bool("update-pg100-sums", false, "rewrite testdata/pg100/SHA256SUMS after an intended change")

const (
	pg100Count = 100
	pg100Seed  = 100_000 // rows per table, loaded at v010
)

// pg100Source is the migrations folder, as an env to copy from.
func pg100Source(t testing.TB) *env {
	t.Helper()
	path, err := filepath.Abs(filepath.Join("testdata", "pg100"))
	if err != nil {
		t.Fatal(err)
	}
	return &env{path: path}
}

// heavy lets one 100,000-row test load the shared server at a time, so they
// do not starve each other or the rest of the suite.
var heavy = make(chan struct{}, 1)

func oneHeavyTest(t testing.TB) {
	t.Helper()
	heavy <- struct{}{}
	t.Cleanup(func() { <-heavy })
}

// pg100Env is a new database with the 100 migrations in its folder.
func pg100Env(t testing.TB) (*env, ecsql.SQLServices) {
	t.Helper()
	e := newEnv(t)
	copyMigrations(t, pg100Source(t), e)
	return e, e.started(t)
}

func versionInt(t testing.TB, s ecsql.SQLServices, e *env) int {
	t.Helper()
	v, err := strconv.Atoi(version(t, s, e))
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// upTo applies migrations one by one until version v, retrying a step that
// lost a lock wait (lock_timeout) or a deadlock, as a deploy would.
func upTo(t testing.TB, s ecsql.SQLServices, e *env, v int) {
	t.Helper()
	for cur := versionInt(t, s, e); cur < v; cur++ {
		var err error
		for attempt := range 5 {
			if err = s.UpSteps(ctxT(t), 1); err == nil || !retryable(err) {
				break
			}
			time.Sleep(time.Duration(attempt+1) * 200 * time.Millisecond)
		}
		if err != nil {
			t.Fatalf("v%03d: %v", cur+1, err)
		}
	}
}

func retryable(err error) bool {
	pg, ok := errors.AsType[pgdriver.Error](err)
	return ok && (pg.Field('C') == "55P03" || pg.Field('C') == "40P01")
}

// Seeding at v010: 100,000 users, each with one account and one deposit,
// spread over 2023-2025 so the Phase 3 partitions all get rows.
const pg100SeedSQL = `
INSERT INTO users (email, full_name, phone, created_at)
  SELECT 'user' || g || '@ecoop.test', 'First' || g || ' Last' || g, '+63917' || lpad(g::text, 7, '0'),
         timestamptz '2023-01-01 00:00:00+00' + g * interval '15 minutes'
    FROM generate_series(1, 100000) g;
INSERT INTO accounts (user_id, number, balance, currency, created_at)
  SELECT g, 'AC' || lpad(g::text, 10, '0'), (g % 5000 + 1) + 0.25, 'PHP',
         timestamptz '2023-01-01 00:01:00+00' + g * interval '15 minutes'
    FROM generate_series(1, 100000) g;
INSERT INTO transactions (account_id, amount, kind, created_at, description)
  SELECT g, (g % 5000 + 1) + 0.25, 'deposit', timestamptz '2023-01-01 00:00:00+00' + g * interval '13 minutes', 'Seed deposit ' || g
    FROM generate_series(1, 100000) g;`

// pg100SeedCents is the money the seed puts in, in cents.
var pg100SeedCents = func() int64 {
	var sum int64
	for g := int64(1); g <= pg100Seed; g++ {
		sum += (g%5000+1)*100 + 25
	}
	return sum
}()

// pg100Digest fingerprints every v010 column of every row.
const pg100Digest = `SELECT md5(
  (SELECT string_agg(concat_ws('|', id, email, full_name, phone, created_at), ',' ORDER BY id) FROM users) ||
  (SELECT string_agg(concat_ws('|', id, user_id, number, balance, currency, created_at), ',' ORDER BY id) FROM accounts) ||
  (SELECT string_agg(concat_ws('|', id, account_id, amount, kind, created_at, description), ',' ORDER BY id) FROM transactions))`

func digest(t testing.TB, s ecsql.SQLServices) string {
	t.Helper()
	var d string
	if err := s.Client().NewRaw(pg100Digest).Scan(ctxT(t), &d); err != nil {
		t.Fatal(err)
	}
	return d
}

func sum(t testing.TB, s ecsql.SQLServices, query string) int64 {
	t.Helper()
	var n int64
	if err := s.Client().NewRaw(query).Scan(ctxT(t), &n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

// pg100Money checks the books balance at version v, whichever money
// columns exist then: accounts hold exactly what their transactions add up
// to, and both hold what was put in (plus, under load, the net of the
// workload, which keeps both sides equal).
func pg100Money(t testing.TB, s ecsql.SQLServices, v int, exact bool) {
	t.Helper()
	acct, tx := "round(balance * 100)", "round(amount * 100)"
	if v >= 40 {
		acct = "balance_cents"
	}
	if v >= 38 {
		tx = "amount_cents"
	}
	a := sum(t, s, "SELECT coalesce(sum("+acct+"), 0)::bigint FROM accounts")
	b := sum(t, s, "SELECT coalesce(sum("+tx+"), 0)::bigint FROM transactions")
	if a != b {
		t.Fatalf("v%03d: accounts hold %d cents, transactions add up to %d", v, a, b)
	}
	if exact && a != pg100SeedCents {
		t.Fatalf("v%03d: money = %d cents, want %d", v, a, pg100SeedCents)
	}
}

// --- Files: sequence, format, immutability ---------------------------------

func TestPG100_FilesAreSequentialAndImmutable(t *testing.T) {
	t.Parallel()
	src := pg100Source(t)
	files := src.files("*.sql")
	if len(files) != pg100Count {
		t.Fatalf("%d migrations, want %d", len(files), pg100Count)
	}
	var sums strings.Builder
	for i, f := range files {
		base := filepath.Base(f)
		if want := fmt.Sprintf("%03d_", i+1); !strings.HasPrefix(base, want) {
			t.Fatalf("file %d is %s: versions must run 001..100 with no gaps", i+1, base)
		}
		body := readFile(t, f)
		up, down, ok := strings.Cut(body, "-- +goose Down")
		if !ok || !strings.Contains(up, "-- +goose Up") || strings.TrimSpace(down) == "" {
			t.Errorf("%s: needs an Up and a non-empty Down", base)
		}
		notx := strings.Contains(body, "-- +goose NO TRANSACTION")
		needsNotx := strings.Contains(body, "CONCURRENTLY") || strings.Contains(body, "COMMIT;")
		if notx != needsNotx {
			t.Errorf("%s: NO TRANSACTION = %v, but it is needed only for CONCURRENTLY or batch COMMITs (%v)", base, notx, needsNotx)
		}
		if !strings.HasPrefix(body, fmt.Sprintf("-- v%03d, phase ", i+1)) {
			t.Errorf("%s: missing its architectural note", base)
		}
		fmt.Fprintf(&sums, "%x  %s\n", sha256.Sum256([]byte(body)), base)
	}
	path := filepath.Join(src.path, "SHA256SUMS")
	if *updatePG100Sums {
		if err := os.WriteFile(path, []byte(sums.String()), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	if want := readFile(t, path); want != sums.String() {
		t.Fatalf("a migration file changed after it was released:\n%s\nApplied migrations are history; add a new version instead. "+
			"If the change is intended (nothing has run it yet), rerun with -update-pg100-sums.", diffLines(want, sums.String()))
	}
}

// --- Lock safety of Phase 4 -------------------------------------------------

// lockRules are what a migration on a live, large table must not do. Each
// pattern is checked against the Up section of versions 81-100.
var (
	createIndex  = regexp.MustCompile(`(?is)CREATE\s+(UNIQUE\s+)?INDEX\b[^;]*`)
	addCheckOrFK = regexp.MustCompile(`(?is)ADD\s+CONSTRAINT\s+\w+\s+(CHECK|FOREIGN\s+KEY)\b[^;]*`)
	typeChange   = regexp.MustCompile(`(?is)ALTER\s+COLUMN\s+\w+\s+(SET\s+DATA\s+)?TYPE\b`)
	addNotNull   = regexp.MustCompile(`(?is)ADD\s+COLUMN\s+[^;]*NOT\s+NULL`)
	rewriteDML   = regexp.MustCompile(`(?im)^\s*(UPDATE\s+\w+\s+SET|DELETE\s+FROM)\b`)
	setNotNull   = regexp.MustCompile(`(?is)ALTER\s+COLUMN\s+(\w+)\s+SET\s+NOT\s+NULL`)
	lockTable    = regexp.MustCompile(`(?is)\bLOCK\s+TABLE\b`)
)

// lintLockSafe returns the lock-safety problems of one migration, given the
// Up sections of the late-phase files before it.
func lintLockSafe(body string, earlier []string) []string {
	up, _, _ := strings.Cut(body, "-- +goose Down")
	up = regexp.MustCompile(`(?m)^\s*--.*$`).ReplaceAllString(up, "") // notes may mention anything
	notx := strings.Contains(body, "-- +goose NO TRANSACTION")
	var problems []string
	if !notx && !strings.Contains(up, "SET LOCAL lock_timeout") {
		problems = append(problems, "transactional DDL without SET LOCAL lock_timeout: a blocked lock would queue every query behind it")
	}
	for _, stmt := range createIndex.FindAllString(up, -1) {
		if !strings.Contains(stmt, "CONCURRENTLY") && !regexp.MustCompile(`(?i)\bON\s+ONLY\b`).MatchString(stmt) {
			problems = append(problems, "CREATE INDEX without CONCURRENTLY blocks writes for the whole build")
		}
	}
	for _, stmt := range addCheckOrFK.FindAllString(up, -1) {
		if !strings.Contains(stmt, "NOT VALID") {
			problems = append(problems, "CHECK or FOREIGN KEY without NOT VALID scans the table under lock")
		}
	}
	if typeChange.MatchString(up) {
		problems = append(problems, "ALTER COLUMN TYPE rewrites the table under an exclusive lock")
	}
	if addNotNull.MatchString(up) {
		problems = append(problems, "ADD COLUMN ... NOT NULL: add it nullable, backfill, then validate")
	}
	if rewriteDML.MatchString(up) && !(notx && strings.Contains(up, "COMMIT;")) {
		problems = append(problems, "UPDATE or DELETE in one transaction: backfill in committed batches")
	}
	for _, m := range setNotNull.FindAllStringSubmatch(up, -1) {
		validated := false
		check := regexp.MustCompile(`(?is)CHECK\s*\(\s*` + m[1] + `\s+IS\s+NOT\s+NULL\s*\)\s*NOT\s+VALID`)
		for i, prev := range earlier {
			if check.MatchString(prev) && slices.ContainsFunc(earlier[i:], func(p string) bool { return strings.Contains(p, "VALIDATE CONSTRAINT") }) {
				validated = true
			}
		}
		if !validated {
			problems = append(problems, "SET NOT NULL on "+m[1]+" without a validated CHECK first: it scans the table under an exclusive lock")
		}
	}
	if lockTable.MatchString(up) {
		problems = append(problems, "LOCK TABLE in a late-phase migration")
	}
	if strings.Contains(up, "CONCURRENTLY") && !notx {
		problems = append(problems, "CONCURRENTLY cannot run inside a transaction")
	}
	return problems
}

func TestPG100_LateMigrationsAreLockSafe(t *testing.T) {
	t.Parallel()
	files := pg100Source(t).files("*.sql")
	var earlier []string
	for _, f := range files[80:] {
		body := readFile(t, f)
		for _, p := range lintLockSafe(body, earlier) {
			t.Errorf("%s: %s", filepath.Base(f), p)
		}
		up, _, _ := strings.Cut(body, "-- +goose Down")
		earlier = append(earlier, up)
	}
}

// The linter must catch what it claims to.
func TestPG100_LockLinterCatchesUnsafeMigrations(t *testing.T) {
	t.Parallel()
	for name, up := range map[string]string{
		"no lock_timeout":          "ALTER TABLE users ADD COLUMN x text;",
		"blocking index":           "SET LOCAL lock_timeout = '5s';\nCREATE INDEX x ON users (email);",
		"validated FK at once":     "SET LOCAL lock_timeout = '5s';\nALTER TABLE a ADD CONSTRAINT f FOREIGN KEY (b) REFERENCES c (id);",
		"type rewrite":             "SET LOCAL lock_timeout = '5s';\nALTER TABLE a ALTER COLUMN b TYPE bigint;",
		"NOT NULL column":          "SET LOCAL lock_timeout = '5s';\nALTER TABLE a ADD COLUMN b int NOT NULL DEFAULT 0;",
		"one-shot backfill":        "SET LOCAL lock_timeout = '5s';\nUPDATE users SET x = 1;",
		"unvalidated SET NOT NULL": "SET LOCAL lock_timeout = '5s';\nALTER TABLE a ALTER COLUMN b SET NOT NULL;",
		"LOCK TABLE":               "SET LOCAL lock_timeout = '5s';\nLOCK TABLE a IN ACCESS EXCLUSIVE MODE;",
	} {
		if len(lintLockSafe("-- +goose Up\n"+up+"\n-- +goose Down\nSELECT 1;", nil)) == 0 {
			t.Errorf("%s: not caught", name)
		}
	}
}

// --- Determinism ------------------------------------------------------------

// Two blank databases reach the same schema, whether applied one by one,
// all at once on deploy (Run), or rebuilt with Fresh; goose orders the
// files numerically; nothing is left half-built.
func TestPG100_BlankSlateIsDeterministic(t *testing.T) {
	t.Parallel()
	a, sa := pg100Env(t)
	upTo(t, sa, a, pg100Count)
	b := newEnv(t)
	copyMigrations(t, pg100Source(t), b)
	sb := ecsql.NewSQLService(b.dsn, 2, 4, b.dir, true, b.out, nil, nil)
	ctx := ctxT(t)
	if err := sb.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sb.Stop(context.Background())
	if err := sb.Run(ctx); err != nil {
		t.Fatal(err)
	}
	want := snapshot(t, sa)
	if got := snapshot(t, sb); got != want {
		t.Fatalf("one-by-one and all-at-once differ:\n%s", diffLines(want, got))
	}
	if err := sb.Fresh(ctx); err != nil {
		t.Fatal(err)
	}
	if got := snapshot(t, sb); got != want {
		t.Fatalf("Fresh differs:\n%s", diffLines(want, got))
	}
	if strings.Contains(want, "\ninvalid ") {
		t.Fatal("an index was left INVALID")
	}
	if v := versionInt(t, sb, b); v != pg100Count {
		t.Fatalf("version = %d", v)
	}
	b.out.Reset()
	if err := sb.Status(ctx); err != nil {
		t.Fatal(err)
	}
	var versions []int
	for _, line := range strings.Split(strings.TrimSpace(b.out.String()), "\n")[1:] {
		n, _ := strconv.Atoi(strings.Fields(line)[0])
		versions = append(versions, n)
	}
	for i, v := range versions {
		if v != i+1 {
			t.Fatalf("goose ran the files out of order: %v", versions)
		}
	}
}

// --- Full round trip ----------------------------------------------------------

// Up one version at a time to v100, recording the schema; down one at a
// time to nothing, each Down restoring exactly the schema before its Up;
// then up again to the same v100.
func TestPG100_FullRoundTrip(t *testing.T) {
	t.Parallel()
	e, s := pg100Env(t)
	snaps := []string{snapshot(t, s)}
	for v := 1; v <= pg100Count; v++ {
		upTo(t, s, e, v)
		snaps = append(snaps, snapshot(t, s))
	}
	for v := pg100Count; v >= 1; v-- {
		if err := s.RollbackSteps(ctxT(t), 1); err != nil {
			t.Fatalf("Down of v%03d: %v", v, err)
		}
		if got := snapshot(t, s); got != snaps[v-1] {
			t.Fatalf("Down of v%03d did not restore v%03d:\n%s", v, v-1, diffLines(snaps[v-1], got))
		}
	}
	upTo(t, s, e, pg100Count)
	if got := snapshot(t, s); got != snaps[pg100Count] {
		t.Fatalf("second climb differs:\n%s", diffLines(snaps[pg100Count], got))
	}
}

// --- Data-seeding evolution ---------------------------------------------------

// 100,000 rows per table at v010, then the other 90 versions over live
// data, with the books checked along the way; then all the way back to
// v010, where every row must be exactly as seeded.
func TestPG100_SeededDataSurvivesTheWholeHistory(t *testing.T) {
	t.Parallel()
	oneHeavyTest(t)
	e, s := pg100Env(t)
	ctx := ctxT(t)
	upTo(t, s, e, 10)
	seed(t, e, pg100SeedSQL)
	seeded, schema10 := digest(t, s), snapshot(t, s)
	for _, checkpoint := range []int{20, 37, 39, 40, 50, 64, 66, 67, 68, 80, 85, 90, 97, 100} {
		upTo(t, s, e, checkpoint)
		for _, table := range []string{"users", "accounts", "transactions"} {
			if n := count(t, s, "SELECT count(*) FROM "+table); n != pg100Seed {
				t.Fatalf("v%03d: %s has %d rows, want %d", checkpoint, table, n, pg100Seed)
			}
		}
		pg100Money(t, s, checkpoint, true)
	}
	for q, want := range map[string]int{
		"SELECT count(*) FROM ONLY transactions_default":                                      0, // every row found its year
		"SELECT count(*) FROM users WHERE phone_number IS NULL":                               0, // the expand-contract rename kept every phone
		"SELECT count(DISTINCT iban) FROM accounts":                                           pg100Seed,
		"SELECT count(*) FROM users WHERE first_name || ' ' || last_name LIKE 'First% Last%'": pg100Seed,
	} {
		if n := count(t, s, q); n != want {
			t.Errorf("v100: %s = %d, want %d", q, n, want)
		}
	}
	if err := s.RollbackTo(ctx, 10); err != nil {
		t.Fatalf("down to v010 over live data: %v", err)
	}
	if got := snapshot(t, s); got != schema10 {
		t.Fatalf("v010 schema not restored:\n%s", diffLines(schema10, got))
	}
	if digest(t, s) != seeded {
		t.Fatal("rows changed between seeding at v010 and coming back to v010")
	}
}

// --- Zero downtime under load --------------------------------------------------

type loadStats struct {
	ops, failed atomic.Int64
	slowest     atomic.Int64 // nanoseconds
	applying    atomic.Int64 // the version being applied, for failure reports
	mu          sync.Mutex
	errs        []string
}

func (l *loadStats) record(start time.Time, err error) {
	l.ops.Add(1)
	if d := int64(time.Since(start)); d > l.slowest.Load() {
		l.slowest.Store(d)
	}
	if err != nil {
		l.failed.Add(1)
		l.mu.Lock()
		if len(l.errs) < 10 {
			l.errs = append(l.errs, fmt.Sprintf("while applying v%03d: %v", l.applying.Load(), err))
		}
		l.mu.Unlock()
	}
}

// workload runs one worker's CRUD traffic until stop: deposits (insert a
// transaction and credit the account, atomically), reversals of its own
// deposits, statement reads, logins and sign-ups. Only what every version
// from v050 on supports.
func workload(ctx context.Context, s ecsql.SQLServices, worker int, stop <-chan struct{}, l *loadStats) {
	db := s.Client()
	r := rand.New(rand.NewPCG(uint64(worker), 42))
	type deposit struct{ id, account, cents int64 }
	var mine []deposit
	for n := 0; ; n++ {
		select {
		case <-stop:
			return
		default:
		}
		start := time.Now()
		var err error
		switch op := r.IntN(10); {
		case op < 4:
			d := deposit{account: 1 + r.Int64N(pg100Seed), cents: 1 + r.Int64N(10_000)}
			err = func() error {
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					return err
				}
				defer tx.Rollback()
				if err := tx.QueryRowContext(ctx, "INSERT INTO transactions (account_id, amount_cents, kind, reference) VALUES (?, ?, 'deposit', ?) RETURNING id",
					d.account, d.cents, fmt.Sprintf("LOAD-%d-%d", worker, n)).Scan(&d.id); err != nil {
					return err
				}
				if _, err := tx.ExecContext(ctx, "UPDATE accounts SET balance_cents = balance_cents + ? WHERE id = ?", d.cents, d.account); err != nil {
					return err
				}
				return tx.Commit()
			}()
			if err == nil {
				mine = append(mine, d)
			}
		case op < 5 && len(mine) > 0:
			d := mine[len(mine)-1]
			err = func() error {
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					return err
				}
				defer tx.Rollback()
				if _, err := tx.ExecContext(ctx, "DELETE FROM transactions WHERE id = ?", d.id); err != nil {
					return err
				}
				if _, err := tx.ExecContext(ctx, "UPDATE accounts SET balance_cents = balance_cents - ? WHERE id = ?", d.cents, d.account); err != nil {
					return err
				}
				return tx.Commit()
			}()
			if err == nil {
				mine = mine[:len(mine)-1]
			}
		case op < 7:
			var ids []int64
			err = db.NewRaw("SELECT id FROM transactions WHERE account_id = ? ORDER BY created_at DESC LIMIT 20", 1+r.Int64N(pg100Seed)).Scan(ctx, &ids)
		case op < 9:
			id := 1 + r.Int64N(pg100Seed)
			var found int64
			if err = db.NewRaw("SELECT id FROM users WHERE email = ?", fmt.Sprintf("user%d@ecoop.test", id)).Scan(ctx, &found); err == nil {
				_, err = db.ExecContext(ctx, "UPDATE users SET last_login_at = now() WHERE id = ?", found)
			}
		default:
			_, err = db.ExecContext(ctx, "INSERT INTO users (email, first_name, last_name) VALUES (?, 'Load', ?)",
				fmt.Sprintf("load-%d-%d@ecoop.test", worker, n), strconv.Itoa(n))
		}
		l.record(start, err)
	}
}

// v051-v100 (the indexing, partitioning and zero-downtime phases) applied
// while workers hammer all three tables: no application query may fail,
// none may stall for long, and the books must still balance.
func TestPG100_ZeroDowntimeUnderLoad(t *testing.T) {
	t.Parallel()
	oneHeavyTest(t)
	e, s := pg100Env(t)
	upTo(t, s, e, 10)
	seed(t, e, pg100SeedSQL)
	upTo(t, s, e, 50)

	ctx := ctxT(t)
	var stats loadStats
	stop := make(chan struct{})
	var wg sync.WaitGroup
	const workers = 8
	for w := range workers {
		wg.Go(func() { workload(ctx, s, w, stop, &stats) })
	}
	time.Sleep(200 * time.Millisecond) // traffic is flowing before the first migration
	before := stats.ops.Load()
	for v := 51; v <= pg100Count; v++ {
		stats.applying.Store(int64(v))
		upTo(t, s, e, v)
	}
	during := stats.ops.Load() - before
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()

	t.Logf("%d operations during the migrations, slowest %v", during, time.Duration(stats.slowest.Load()))
	if n := stats.failed.Load(); n > 0 {
		t.Fatalf("%d application operations failed during the migrations, e.g.:\n%s", n, strings.Join(stats.errs, "\n"))
	}
	if during < 100 {
		t.Fatalf("only %d operations ran during the migrations: traffic stalled", during)
	}
	if slowest := time.Duration(stats.slowest.Load()); slowest > 5*time.Second {
		t.Fatalf("an application operation waited %v behind a migration", slowest)
	}
	pg100Money(t, s, pg100Count, false)
	if strings.Contains(snapshot(t, s), "\ninvalid ") {
		t.Fatal("an index was left INVALID")
	}
}

// --- Transactional boundaries ----------------------------------------------------

// A CONCURRENTLY build runs outside a transaction, so a failure leaves an
// INVALID index behind and no version recorded. v091 drops such a leftover
// first, so after the data is fixed the same file just runs again.
func TestPG100_FailedConcurrentIndexIsRetryable(t *testing.T) {
	t.Parallel()
	e, s := pg100Env(t)
	ctx := ctxT(t)
	upTo(t, s, e, 90)
	execSQL(t, s, `INSERT INTO users (email, first_name, last_name) VALUES ('a@ecoop.test', 'A', 'A'), ('b@ecoop.test', 'B', 'B');
INSERT INTO accounts (user_id, number, iban) SELECT id, 'N' || id, 'PH-DUPLICATE' FROM users;`)
	if err := s.UpSteps(ctx, 1); err == nil {
		t.Fatal("unique index built over duplicate IBANs")
	}
	if v := versionInt(t, s, e); v != 90 {
		t.Fatalf("version after the failed build = %d, want 90", v)
	}
	if !strings.Contains(snapshot(t, s), "\ninvalid accounts_iban_key") {
		t.Fatal("expected the failed concurrent build to leave an INVALID index")
	}
	execSQL(t, s, "UPDATE accounts SET iban = 'PH-FIXED-' || id WHERE iban = 'PH-DUPLICATE'")
	if err := s.UpSteps(ctx, 1); err != nil {
		t.Fatalf("rerunning v091 after fixing the data: %v", err)
	}
	if strings.Contains(snapshot(t, s), "\ninvalid ") {
		t.Fatal("the leftover INVALID index survived the rerun")
	}
}
