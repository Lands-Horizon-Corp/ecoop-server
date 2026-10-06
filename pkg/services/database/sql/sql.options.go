package sql

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Option configures a SQLService beyond the constructor's required arguments.
type Option func(*SQLService)

// WithConnMaxLifetime recycles a pooled connection after d, so connections to a replaced server
// (failover, load balancer, PgBouncer restart) are not reused forever.
func WithConnMaxLifetime(d time.Duration) Option {
	return func(s *SQLService) { s.connMaxLifetime = d }
}

// WithConnMaxIdleTime closes a pooled connection that has been idle for d.
func WithConnMaxIdleTime(d time.Duration) Option {
	return func(s *SQLService) { s.connMaxIdleTime = d }
}

// WithDatabaseSettings stores server settings on the database itself (ALTER DATABASE ... SET) when
// the service starts, before migrations run. Every new server session picks them up, including
// sessions opened by a pooler such as PgBouncer, where per-connection startup parameters are refused.
// Sessions that already existed keep their old values until they are recycled.
func WithDatabaseSettings(settings map[string]string) Option {
	return func(s *SQLService) { s.dbSettings = settings }
}

func safeSettingToken(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("_.:-", r)) {
			return false
		}
	}
	return true
}

// applyDatabaseSettings runs ALTER DATABASE for each setting, then drops idle pooled connections so
// the pool's own sessions start with the new values.
func (s *SQLService) applyDatabaseSettings(ctx context.Context) error {
	if len(s.dbSettings) == 0 {
		return nil
	}
	db := s.db.Load()
	if db == nil {
		return ErrNotInitialized
	}
	names := make([]string, 0, len(s.dbSettings))
	for name := range s.dbSettings {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		value := s.dbSettings[name]
		if !safeSettingToken(name) || !safeSettingToken(value) {
			return fmt.Errorf("invalid database setting %q = %q", name, value)
		}
		// ALTER DATABASE needs the database name as an identifier: format(%I) quotes it server side.
		// Name and value are restricted to [A-Za-z0-9_.:-], so they cannot break out of the literal.
		q := `DO $$ BEGIN EXECUTE format('ALTER DATABASE %I SET ` + name + ` = %L', current_database(), '` + value + `'); END $$`
		if _, err := db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("setting %s on the database: %w", name, err)
		}
	}
	sqldb := s.sqldb.Load()
	sqldb.SetMaxIdleConns(0)
	sqldb.SetMaxIdleConns(s.maxIdleConn)
	return nil
}
