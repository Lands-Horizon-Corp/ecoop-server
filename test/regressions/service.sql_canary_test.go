package regressions

import (
	"context"
	"testing"
	"time"
)

// Migration canary tests: a release is applied to a small canary environment first, and only
// promoted to the rest of the fleet if the canary stays healthy. Each canary here is a separate
// database deployed from the same migration files.

// Happy: the canary takes the release, keeps its data, and the fleet then converges to the same schema.
func TestMigrationCanary_HappyCanaryThenFleetConverge(t *testing.T) {
	prodEnv := newSQLEnv(t)
	prod := bootstrapBank(t, prodEnv)
	prodIDs := seedBank(t, prodEnv, 5, 80_000)
	if err := bankTransfer(bg, prodEnv.db, "prod-1", prodIDs[0], prodIDs[1], 5_000); err != nil {
		t.Fatal(err)
	}

	canaryEnv := prodEnv.sibling()
	canary := canaryEnv.running() // deployed from the same files as production
	canaryIDs := seedBank(t, canaryEnv, 3, 80_000)
	if err := bankTransfer(bg, canaryEnv.db, "canary-1", canaryIDs[0], canaryIDs[1], 1_000); err != nil {
		t.Fatal(err)
	}
	canaryBefore := accountsChecksum(canaryEnv)

	// Release the next version to the canary only.
	settleModels(t, canary, "release v2", bankModelsV2())
	if accountsChecksum(canaryEnv) != canaryBefore {
		t.Fatal("the release changed the canary's data")
	}
	if !canaryEnv.hasColumn("accounts", "risk_score") || !canaryEnv.hasTable("risk_profiles") {
		t.Fatal("the canary did not receive the release")
	}
	requireBankInvariants(t, canaryEnv, 3, 80_000)
	requireConverged(t, canaryEnv, canary, bankModelsV2()...)

	// The canary is healthy, so promote to production.
	prodBefore := accountsChecksum(prodEnv)
	if err := prod.Migrate(bg); err != nil {
		t.Fatalf("promoting the release to production: %v", err)
	}
	if accountsChecksum(prodEnv) != prodBefore {
		t.Fatal("the promotion changed production data")
	}
	if prodEnv.schema() != canaryEnv.schema() || prodEnv.appliedVersion() != canaryEnv.appliedVersion() {
		t.Fatal("production and canary diverged after the promotion")
	}
	requireBankInvariants(t, prodEnv, 5, 80_000)
}

// Sad: a migration that is valid on clean data fails on the canary's real data, so the release stops there.
func TestMigrationCanary_SadDataDependentFailureStopsAtCanary(t *testing.T) {
	canaryEnv := newSQLEnv(t)
	canary := bootstrapBank(t, canaryEnv)
	seedBank(t, canaryEnv, 2, 10_000)
	// Real data has two customers with the same legal name at one branch.
	canaryEnv.exec(`INSERT INTO customers (branch_id, legal_name, tax_id, kyc_status)
		VALUES ((SELECT id FROM branches WHERE code = 'HQ'), 'Twin Name', 'TAX-A', 'verified'),
		       ((SELECT id FROM branches WHERE code = 'HQ'), 'Twin Name', 'TAX-B', 'verified')`)

	cleanEnv := canaryEnv.sibling()
	clean := cleanEnv.running()
	seedBank(t, cleanEnv, 2, 10_000)

	canaryEnv.writeNext("unique_customer_name",
		`CREATE UNIQUE INDEX customers_branch_name_uq ON customers (branch_id, legal_name);`,
		`DROP INDEX customers_branch_name_uq;`)

	good := canaryEnv.appliedVersion()
	before := accountsChecksum(canaryEnv)
	if err := canary.Migrate(bg); err == nil {
		t.Fatal("the canary accepted a unique index that its own data violates")
	}
	if canaryEnv.appliedVersion() != good {
		t.Fatalf("canary version moved to %d; want it to stay at %d", canaryEnv.appliedVersion(), good)
	}
	if canaryEnv.scanInt(`SELECT count(*) FROM pg_indexes WHERE indexname = 'customers_branch_name_uq'`) != 0 {
		t.Fatal("a failed migration left its index behind")
	}
	if accountsChecksum(canaryEnv) != before {
		t.Fatal("the failed migration changed canary data")
	}

	// A clean database would have passed, which is exactly why the canary must run first.
	if err := clean.Migrate(bg); err != nil {
		t.Fatalf("the same migration should pass on clean data: %v", err)
	}
}

// Happy: the canary can be rolled back to exactly its previous schema and data, then re-released.
func TestMigrationCanary_HappyRollbackRestoresSchemaAndData(t *testing.T) {
	e := newSQLEnv(t)
	canary := bootstrapBank(t, e)
	ids := seedBank(t, e, 4, 60_000)
	for i := range 6 {
		if err := bankTransfer(bg, e.db, "canary-"+time.Now().Format("150405.000000")+string(rune('a'+i)), ids[i%4], ids[(i+1)%4], 1_000); err != nil {
			t.Fatal(err)
		}
	}
	schemaBefore, dataBefore := e.schema(), accountsChecksum(e)

	applied := settleModels(t, canary, "release v2", bankModelsV2())
	schemaAfter := e.schema()
	if schemaAfter == schemaBefore {
		t.Fatal("the release did not change the schema")
	}

	if err := canary.RollbackSteps(bg, len(applied)); err != nil {
		t.Fatalf("rolling the canary back: %v", err)
	}
	if e.schema() != schemaBefore || accountsChecksum(e) != dataBefore {
		t.Fatal("the rollback did not restore the previous schema and data")
	}
	requireBankInvariants(t, e, 4, 60_000)

	if err := canary.UpSteps(bg, len(applied)); err != nil {
		t.Fatalf("re-releasing to the canary: %v", err)
	}
	if e.schema() != schemaAfter || accountsChecksum(e) != dataBefore {
		t.Fatal("re-releasing did not reproduce the released schema")
	}
}

// Sad: a long-running transaction on the canary blocks the migration; it times out cleanly, changes
// nothing, and succeeds as soon as the blocker is gone.
func TestMigrationCanary_SadLockContentionTimesOutWithoutChange(t *testing.T) {
	e := newSQLEnv(t)
	canary := bootstrapBank(t, e)
	seedBank(t, e, 2, 10_000)
	good := e.appliedVersion()
	e.writeNext("flag_accounts", `ALTER TABLE accounts ADD COLUMN flagged boolean;`, `ALTER TABLE accounts DROP COLUMN flagged;`)

	blocker, err := e.db.BeginTx(bg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := blocker.Exec(`LOCK TABLE accounts IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(bg, 1500*time.Millisecond)
	defer cancel()
	if err := canary.Migrate(ctx); err == nil {
		_ = blocker.Rollback()
		t.Fatal("Migrate finished while another transaction held an exclusive lock")
	}
	if err := blocker.Rollback(); err != nil {
		t.Fatal(err)
	}
	if e.appliedVersion() != good || e.hasColumn("accounts", "flagged") {
		t.Fatal("the timed-out migration changed the canary")
	}

	if err := canary.Migrate(bg); err != nil {
		t.Fatalf("Migrate after the blocker released its lock: %v", err)
	}
	if !e.hasColumn("accounts", "flagged") || e.appliedVersion() <= good {
		t.Fatal("the migration was not applied once the lock cleared")
	}
}

// Happy: a staged rollout, canary then two tenants, ends with every database on the same schema and version.
func TestMigrationCanary_HappyStagedRolloutAcrossTenants(t *testing.T) {
	canaryEnv := newSQLEnv(t)
	canary := bootstrapBank(t, canaryEnv)

	tenants := []struct {
		env      *sqlEnv
		svc      interface{ Migrate(context.Context) error }
		accounts int
	}{}
	for _, n := range []int{3, 6} {
		env := canaryEnv.sibling()
		svc := env.running()
		seedBank(t, env, n, 20_000)
		tenants = append(tenants, struct {
			env      *sqlEnv
			svc      interface{ Migrate(context.Context) error }
			accounts int
		}{env, svc, n})
	}
	seedBank(t, canaryEnv, 2, 20_000)

	settleModels(t, canary, "release v2", bankModelsV2())
	for i, tenant := range tenants {
		sum := accountsChecksum(tenant.env)
		if tenant.env.hasColumn("accounts", "risk_score") {
			t.Fatalf("tenant %d received the release before it was promoted", i)
		}
		if err := tenant.svc.Migrate(bg); err != nil {
			t.Fatalf("promoting to tenant %d: %v", i, err)
		}
		if accountsChecksum(tenant.env) != sum {
			t.Fatalf("tenant %d lost data during the promotion", i)
		}
		if tenant.env.schema() != canaryEnv.schema() || tenant.env.appliedVersion() != canaryEnv.appliedVersion() {
			t.Fatalf("tenant %d diverged from the canary", i)
		}
		requireBankInvariants(t, tenant.env, tenant.accounts, 20_000)
	}
}
