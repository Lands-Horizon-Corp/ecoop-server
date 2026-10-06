package sql

import (
	"context"
	"errors"
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/pressly/goose/v3"
)

func (s *SQLService) Status(ctx context.Context) error {
	return s.observe("sql.status", func() error {
		migrator, err := s.provider()
		if errors.Is(err, goose.ErrNoMigrations) {
			_, err = fmt.Fprintln(s.out(), "no migrations found")
			return err
		}
		if err != nil {
			return err
		}
		statuses, err := migrator.Status(ctx)
		if err != nil {
			return fmt.Errorf("failed to get migration status: %w", err)
		}

		w := tabwriter.NewWriter(s.out(), 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "Applied At\tMigration")
		fmt.Fprintln(w, "==========\t=========")
		for _, st := range statuses {
			appliedAt := "Pending"
			if st.State == goose.StateApplied {
				appliedAt = st.AppliedAt.Format(time.DateTime)
			}
			fmt.Fprintf(w, "%s\t%s\n", appliedAt, st.Source.Path)
		}
		return w.Flush()
	})
}

func (s *SQLService) Version(ctx context.Context) error {
	return s.observe("sql.version", func() error {
		migrator, err := s.provider()
		if errors.Is(err, goose.ErrNoMigrations) {
			_, err = fmt.Fprintln(s.out(), "database version: 0")
			return err
		}
		if err != nil {
			return err
		}
		version, err := migrator.GetDBVersion(ctx)
		if err != nil {
			return fmt.Errorf("failed to get database version: %w", err)
		}
		_, err = fmt.Fprintf(s.out(), "database version: %d\n", version)
		return err
	})
}
