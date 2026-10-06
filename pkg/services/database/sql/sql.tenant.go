package sql

import (
	"context"
	"database/sql"

	"github.com/uptrace/bun"
)

// Tenant scoping for Postgres row-level security. Policies read the tenant from the transaction-local
// setting app.tenant_id (and let the read-model replicator through via app.role), for example:
//
//	ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;
//	ALTER TABLE accounts FORCE ROW LEVEL SECURITY;
//	CREATE POLICY tenant_isolation ON accounts
//	    USING (tenant_id = current_setting('app.tenant_id', true) OR current_setting('app.role', true) = 'replicator')
//	    WITH CHECK (tenant_id = current_setting('app.tenant_id', true) OR current_setting('app.role', true) = 'replicator');
//
// The settings are transaction-local (set_config(..., true)), so they never leak to the next user of
// a pooled connection and they work through PgBouncer in transaction mode. Without a tenant the
// setting is empty, so a forgotten tenant fails closed: no rows visible, every write rejected.

type tenantKey struct{}

// WithTenant returns a context whose database work is confined to tenant id.
func WithTenant(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, tenantKey{}, id)
}

// TenantFrom returns the tenant carried by ctx.
func TenantFrom(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(tenantKey{}).(string)
	return id, ok && id != ""
}

// SetTenant stamps ctx's tenant on an open transaction. It does nothing when ctx has no tenant.
func SetTenant(ctx context.Context, tx bun.IDB) error {
	id, ok := TenantFrom(ctx)
	if !ok {
		return nil
	}
	_, err := tx.ExecContext(ctx, `SELECT set_config('app.tenant_id', ?, true)`, id)
	return err
}

// SetReplicator marks a transaction as the read-model replicator, which policies let through.
func SetReplicator(ctx context.Context, tx bun.IDB) error {
	_, err := tx.ExecContext(ctx, `SELECT set_config('app.role', 'replicator', true)`)
	return err
}

// Scoped runs fn against db with ctx's tenant applied. On the pool with a tenant it opens a short
// transaction, stamps the tenant and runs fn inside it; without a tenant, or on a transaction (stamped
// when it began), it runs fn directly. Pool-level calls are retried once on a stale session.
func Scoped[T any](ctx context.Context, db bun.IDB, fn func(q bun.IDB) (T, error)) (T, error) {
	pool, pooled := db.(*bun.DB)
	if _, ok := TenantFrom(ctx); !ok || !pooled {
		return RetryStale(db, func() (T, error) { return fn(db) })
	}
	return RetryStale(db, func() (T, error) {
		var out T
		err := pool.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
			if err := SetTenant(ctx, tx); err != nil {
				return err
			}
			var err error
			out, err = fn(tx)
			return err
		})
		return out, err
	})
}

type noLockKey struct{}

// WithoutRowLocks makes reads inside a transaction plain snapshots instead of SELECT ... FOR UPDATE:
// they then work in read-only transactions and never block writers. Use it for reports and checks
// that do not write back what they read; keep the default (locking) for read-modify-write.
func WithoutRowLocks(ctx context.Context) context.Context {
	return context.WithValue(ctx, noLockKey{}, true)
}

// RowLocksDisabled reports whether ctx came from WithoutRowLocks.
func RowLocksDisabled(ctx context.Context) bool {
	off, _ := ctx.Value(noLockKey{}).(bool)
	return off
}
