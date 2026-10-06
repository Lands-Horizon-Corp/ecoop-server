package regressions

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// 23 Backup and restore: a logical backup (pg_dump -Fc) restored into a fresh database must carry the
// money, the audit trail, the triggers that guard them and the row-level security policies, and the
// restored ledger must still enforce every rule. Opt-in (DOCKER_CHAOS=1, `make test-chaos`): it runs
// pg_dump and pg_restore inside the Postgres container. Point-in-time recovery needs WAL archiving
// and is an operations procedure, outside the service's tests.

func TestBankDBBackup_DumpAndRestorePreservesMoneyRulesAndPolicies(t *testing.T) {
	if os.Getenv("DOCKER_CHAOS") == "" {
		t.Skip("runs pg_dump/pg_restore in the Postgres container; run with DOCKER_CHAOS=1 (make test-chaos)")
	}
	b, _, _ := tenantBank(t, 4)
	source := strings.TrimPrefix(mustURLPath(t, b.h.writerDSN), "/")
	restored, restoredDSN := createTestDatabase(t)

	script := fmt.Sprintf(`pg_dump -U ecoop -Fc %s | pg_restore -U ecoop --exit-on-error -d %s`, source, restored)
	if out, err := composeCmd("exec", "-T", "postgres", "sh", "-c", script).CombinedOutput(); err != nil {
		t.Fatalf("backup/restore: %v\n%s", err, out)
	}
	orig, copyDB := b.h.writer, openInspect(t, restoredDSN)

	for name, q := range map[string]string{
		"accounts":   `SELECT string_agg(id || '=' || balance || '@' || COALESCE(tenant_id, '-'), ',' ORDER BY id) FROM bank_accounts`,
		"transfers":  `SELECT string_agg(id || ':' || amount, ',' ORDER BY id) FROM bank_transfers`,
		"audit":      `SELECT string_agg(id || ':' || delta || ':' || balance_after, ',' ORDER BY id) FROM bank_audit`,
		"migrations": `SELECT string_agg(version_id::text, ',' ORDER BY version_id) FROM goose_db_version WHERE is_applied`,
	} {
		if a, c := queryLines(t, orig, q), queryLines(t, copyDB, q); a != c || a == "" {
			t.Errorf("%s differ after restore:\noriginal %s\nrestored %s", name, a, c)
		}
	}
	for what, q := range map[string]string{
		"guard triggers":  `SELECT count(*) FROM pg_trigger WHERE tgname IN ('bank_audit_immutable', 'bank_closed_frozen', 'bank_transfer_currency')`,
		"RLS policies":    `SELECT count(*) FROM pg_policies WHERE policyname = 'tenant_isolation'`,
		"forced RLS":      `SELECT count(*) FROM pg_class WHERE relname LIKE 'bank_%' AND relrowsecurity AND relforcerowsecurity`,
		"enum type":       `SELECT count(*) FROM pg_type WHERE typname = 'bank_txn_kind'`,
		"audit reconcile": `SELECT count(*) FROM bank_accounts a WHERE a.balance <> 1000 + COALESCE((SELECT SUM(delta) FROM bank_audit WHERE account_id = a.id), 0)`,
	} {
		want := map[string]int64{"guard triggers": 3, "RLS policies": 3, "forced RLS": 3, "enum type": 1, "audit reconcile": 0}[what]
		if got := count(t, copyDB, q); got != want {
			t.Errorf("restored %s = %d; want %d", what, got, want)
		}
	}
	if _, err := copyDB.Exec(`UPDATE bank_audit SET delta = 0`); err == nil {
		t.Error("the restored audit trail accepts updates")
	}
	// Row-level security still confines the app role in the restored copy.
	app := openInspect(t, asUser(restoredDSN, "ecoop_app", "ecoop-app-pass"))
	tx, err := app.Begin()
	must(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.Exec(`SELECT set_config('app.tenant_id', 'branch-a', true)`)
	must(t, err)
	var visible int64
	must(t, tx.QueryRow(`SELECT count(*) FROM bank_accounts`).Scan(&visible))
	if visible != 2 {
		t.Fatalf("branch A sees %d accounts in the restored copy; want its own 2", visible)
	}
}
