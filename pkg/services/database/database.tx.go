package database

import (
	"context"
	"database/sql"

	dbsql "github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"
	"github.com/uptrace/bun"
)

// RunInTx runs fn in a transaction on the writer, scoped to ctx's tenant (WithTenant), committing when fn returns nil and rolling back
// when it returns an error or panics (the panic is re-raised). Unlike calling Writer().Client()
// directly it is safe while the service is stopped or restarting: it then returns a mapped
// ErrUnavailable instead of dereferencing a closed pool.
func RunInTx(ctx context.Context, db *DatabaseService, opts *sql.TxOptions, fn func(ctx context.Context, tx bun.Tx) error) error {
	if db == nil {
		return ErrNilService
	}
	w := db.Writer()
	if w == nil || !db.started.Load() {
		return &MappedError{Kind: ErrUnavailable, Cause: ErrNotStarted}
	}
	client := w.Client()
	if client == nil {
		return &MappedError{Kind: ErrUnavailable, Cause: ErrNotStarted}
	}
	return client.RunInTx(ctx, opts, func(ctx context.Context, tx bun.Tx) error {
		if err := dbsql.SetTenant(ctx, tx); err != nil {
			return err
		}
		return fn(ctx, tx)
	})
}

// WithTenant confines every database call made with the returned context to tenant id: cqrs writes,
// pagination reads, StartTx and RunInTx all stamp it on their transaction for row-level security.
// See the sql package (WithTenant) for the policy pattern the tables need.
func WithTenant(ctx context.Context, id string) context.Context { return dbsql.WithTenant(ctx, id) }

// TenantFrom returns the tenant carried by ctx.
func TenantFrom(ctx context.Context) (string, bool) { return dbsql.TenantFrom(ctx) }

// WithoutRowLocks makes the in-transaction reads made with the returned context (GetByIDWithTx,
// FindWithTx, ...) plain reads instead of SELECT ... FOR UPDATE. See sql.WithoutRowLocks.
func WithoutRowLocks(ctx context.Context) context.Context { return dbsql.WithoutRowLocks(ctx) }
