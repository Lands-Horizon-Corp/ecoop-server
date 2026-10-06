package database

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"
)

// Option configures a DatabaseService beyond NewDatabaseService's required arguments.
type Option func(*DatabaseService)

// WithConnMaxLifetime recycles pooled connections after d (both pools).
func WithConnMaxLifetime(d time.Duration) Option {
	return func(db *DatabaseService) { db.sqlOpts = append(db.sqlOpts, sql.WithConnMaxLifetime(d)) }
}

// WithConnMaxIdleTime closes pooled connections idle for d (both pools).
func WithConnMaxIdleTime(d time.Duration) Option {
	return func(db *DatabaseService) { db.sqlOpts = append(db.sqlOpts, sql.WithConnMaxIdleTime(d)) }
}

// ServerTimeouts are enforced by Postgres itself, so they hold even for a caller that forgot a
// context deadline. Zero leaves a timeout unchanged.
type ServerTimeouts struct {
	Statement         time.Duration // statement_timeout: a single statement may run at most this long
	Lock              time.Duration // lock_timeout: waiting for a row or table lock
	IdleInTransaction time.Duration // idle_in_transaction_session_timeout: an abandoned open transaction
}

// WithServerTimeouts stores the timeouts on both databases at Start (ALTER DATABASE ... SET), which
// works for direct connections and through PgBouncer alike.
func WithServerTimeouts(t ServerTimeouts) Option {
	settings := map[string]string{}
	for name, d := range map[string]time.Duration{
		"statement_timeout":                   t.Statement,
		"lock_timeout":                        t.Lock,
		"idle_in_transaction_session_timeout": t.IdleInTransaction,
	} {
		if d > 0 {
			settings[name] = fmt.Sprintf("%dms", d.Milliseconds())
		}
	}
	return func(db *DatabaseService) { db.sqlOpts = append(db.sqlOpts, sql.WithDatabaseSettings(settings)) }
}

// WithPgBouncer adapts the service to PgBouncer in transaction pooling mode: queries use pgx's
// "exec" mode (no named prepared statements tied to one server connection), and Start refuses to run
// migrations, because goose's session-level advisory lock cannot work when consecutive statements may
// land on different server connections. Run migrations against Postgres directly.
func WithPgBouncer() Option {
	return func(db *DatabaseService) { db.pgbouncer = true }
}

func withDSNParam(dsn, key, value string) string {
	u, err := url.Parse(dsn)
	if err != nil || !strings.Contains(dsn, "://") {
		return dsn + " " + key + "=" + value
	}
	q := u.Query()
	q.Set(key, value)
	u.RawQuery = q.Encode()
	return u.String()
}
