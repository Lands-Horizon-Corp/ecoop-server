package regressions

import (
	"database/sql"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

// 01 Smoke: the service boots against both databases with healthy, correctly sized pools on the
// expected dialect, and the migrated schema is complete and identical on writer and reader.

func TestBankDBSmoke_PoolsPingAndDialect(t *testing.T) {
	b := newBDBank(t, bdOpts{maxOpen: 6, app: "smoke"})

	for name, tc := range map[string]struct {
		client *bun.DB
		app    string
	}{
		"writer": {b.svc.Writer().Client(), "smoke-w"},
		"reader": {b.svc.Reader().Client(), "smoke-r"},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := withDeadline(t, 5e9)
			if err := tc.client.PingContext(ctx); err != nil {
				t.Fatalf("ping: %v", err)
			}
			if got := tc.client.Dialect().Name(); got != dialect.PG {
				t.Fatalf("dialect = %v; want PostgreSQL", got)
			}
			if got := tc.client.DB.Stats().MaxOpenConnections; got != 6 {
				t.Fatalf("pool max = %d; want 6", got)
			}
			var encoding, app string
			var version int
			must(t, tc.client.QueryRowContext(ctx, `SELECT current_setting('server_encoding'),
				current_setting('application_name'), current_setting('server_version_num')::int`).Scan(&encoding, &app, &version))
			if encoding != "UTF8" || app != tc.app || version < 140000 {
				t.Fatalf("encoding=%s app=%s version=%d; want UTF8, %s, >= 14", encoding, app, version, tc.app)
			}
		})
	}
}

func TestBankDBSmoke_MigrationIntegrityOnBoot(t *testing.T) {
	b := newBDBank(t, bdOpts{})

	snapshot := func(db *sql.DB) string {
		rows, err := db.Query(`SELECT table_name || '.' || column_name || ':' || data_type || ':' || is_nullable
			FROM information_schema.columns WHERE table_schema = 'public' AND table_name LIKE 'bank_%'
			ORDER BY table_name, column_name`)
		must(t, err)
		defer rows.Close()
		var out string
		for rows.Next() {
			var line string
			must(t, rows.Scan(&line))
			out += line + "\n"
		}
		must(t, rows.Err())
		return out
	}
	for name, db := range map[string]*sql.DB{"writer": b.h.writer, "reader": b.h.reader} {
		t.Run(name, func(t *testing.T) {
			checks := []struct {
				what  string
				query string
				want  int64
			}{
				{"all migrations applied", `SELECT COALESCE(MAX(version_id), 0) FROM goose_db_version WHERE is_applied`, 2},
				{"bank tables", `SELECT count(*) FROM information_schema.tables WHERE table_name LIKE 'bank_%'`, 3},
				{"foreign keys", `SELECT count(*) FROM pg_constraint WHERE contype = 'f' AND conrelid::regclass::text LIKE 'bank_%'`, 4},
				{"check constraints", `SELECT count(*) FROM pg_constraint WHERE contype = 'c' AND conrelid::regclass::text LIKE 'bank_%'`, 4},
				{"unique constraints", `SELECT count(*) FROM pg_constraint WHERE contype = 'u' AND conrelid::regclass::text LIKE 'bank_%'`, 2},
				{"append-only trigger", `SELECT count(*) FROM pg_trigger WHERE tgname = 'bank_audit_immutable'`, 1},
				{"enum labels", `SELECT count(*) FROM pg_enum e JOIN pg_type t ON t.oid = e.enumtypid WHERE t.typname = 'bank_txn_kind'`, 3},
				{"processed_events", `SELECT count(*) FROM information_schema.tables WHERE table_name = 'processed_events'`, 1},
			}
			for _, c := range checks {
				if got := count(t, db, c.query); got != c.want {
					t.Errorf("%s = %d; want %d", c.what, got, c.want)
				}
			}
		})
	}
	if w, r := snapshot(b.h.writer), snapshot(b.h.reader); w != r || w == "" {
		t.Fatalf("writer and reader schemas differ:\nwriter:\n%s\nreader:\n%s", w, r)
	}
}

func TestBankDBSmoke_EveryModelRoundTripsThroughAllPackages(t *testing.T) {
	b := newBDBank(t, bdOpts{})
	b.open("a", 100)
	b.open("b", 0)
	if _, _, err := b.Transfer(withDeadline(t, 5e9), transferReq{Key: "k1", From: "a", To: "b", Amount: 40}); err != nil {
		t.Fatalf("Transfer: %v", err)
	}
	b.replicate()

	ctx := withDeadline(t, 5e9)
	for name, n := range map[string]func() (int64, error){
		"accounts":  func() (int64, error) { return b.accounts.Count(ctx, eqFilter("id", "b")) },
		"transfers": func() (int64, error) { return b.transfers.Count(ctx, eqFilter("idempotency_key", "k1")) },
		"audit":     func() (int64, error) { return b.audit.Count(ctx, eqFilter("transfer_id", "t-k1")) },
	} {
		got, err := n()
		if err != nil || got == 0 {
			t.Errorf("%s on the reader = %d, %v; want rows", name, got, err)
		}
	}
	if acc, err := b.accounts.GetByID(ctx, "b"); err != nil || acc.Balance != 40 {
		t.Fatalf("reader account b = %+v, %v; want balance 40", acc, err)
	}
}

func TestBankDBSmoke_RestartIsIdempotent(t *testing.T) {
	b := newBDBank(t, bdOpts{noRun: true})
	b.open("a", 500)
	must(t, b.svc.Stop(bg))

	if err := b.svc.Start(bg); err != nil {
		t.Fatalf("restart: %v", err)
	}
	if got := count(t, b.h.writer, `SELECT count(*) FROM goose_db_version WHERE version_id > 0`); got != 2 {
		t.Fatalf("%d migration records after restart; a migration was re-applied", got)
	}
	accounts, err := database.Get[bdAccount, bdAccountRes, bdNoRequest, string](b.svc)
	if err != nil {
		t.Fatalf("Get after restart: %v", err)
	}
	if _, err := accounts.IncrementByID(withDeadline(t, 5e9), "a", "balance", 1); err != nil {
		t.Fatalf("write after restart: %v", err)
	}
	if got := b.writerBalance("a"); got != 501 {
		t.Fatalf("balance after restart = %d; want 501 (data lost or write failed)", got)
	}
}
