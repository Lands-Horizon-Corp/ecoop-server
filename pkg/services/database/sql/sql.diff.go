package sql

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/pressly/goose/v3"
	"github.com/uptrace/bun/migrate"
	"go.opentelemetry.io/otel/attribute"
)

func (s *SQLService) Diff(ctx context.Context, name string) (path string, err error) {
	err = s.observe("sql.diff", func() (e error) {
		if s.db == nil {
			return ErrNotInitialized
		}
		if len(s.models) == 0 {
			return ErrNoModels
		}
		slug := strings.Trim(nonNameChars.ReplaceAllString(strings.ToLower(name), "_"), "_")
		if slug == "" {
			return ErrInvalidName
		}
		dir, err := s.migrationsPath()
		if err != nil {
			return err
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("creating migrations directory: %w", err)
		}

		if err := s.requireNoPending(ctx); err != nil {
			return err
		}

		tmp, err := os.MkdirTemp("", "goose-diff-*")
		if err != nil {
			return fmt.Errorf("creating scratch directory: %w", err)
		}
		defer os.RemoveAll(tmp)
		am, err := migrate.NewAutoMigrator(s.db,
			migrate.WithModel(s.models...),
			migrate.WithExcludeTable(goose.DefaultTablename),
			migrate.WithMigrationsDirectoryAuto(tmp),
		)
		if err != nil {
			return fmt.Errorf("creating schema diff: %w", err)
		}
		files, err := am.CreateSQLMigrations(ctx)
		if err != nil {
			return fmt.Errorf("computing schema diff: %w", err)
		}
		if len(files) != 2 {
			return nil
		}

		upSQL := orderStatements(files[0].Content)
		downSQL := orderStatements(nameDroppedConstraints(files[0].Content, files[1].Content))
		if err := s.validateMigration(ctx, upSQL, downSQL); err != nil {
			return err
		}

		content := gooseFile(upSQL, downSQL)
		next, err := nextMigrationPath(dir, slug)
		if err != nil {
			return err
		}
		if err := os.WriteFile(next, []byte(content), 0o644); err != nil {
			return fmt.Errorf("writing migration %q: %w", next, err)
		}
		path = next
		return nil
	}, attribute.String("db.migration.name", name))
	return path, err
}

func (s *SQLService) requireNoPending(ctx context.Context) error {
	migrator, err := s.provider()
	if errors.Is(err, goose.ErrNoMigrations) {
		return nil
	}
	if err != nil {
		return err
	}
	pending, err := migrator.HasPending(ctx)
	if err != nil {
		return fmt.Errorf("checking pending migrations: %w", err)
	}
	if pending {
		return ErrPendingMigrations
	}
	return nil
}

func (s *SQLService) validateMigration(ctx context.Context, up, down string) error {
	tx, err := s.sqldb.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("validating migration: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, up); err != nil {
		return fmt.Errorf("%w: up does not apply: %w\n%s", ErrInvalidMigration, err, strings.TrimSpace(up))
	}
	if strings.TrimSpace(down) == "" {
		return nil
	}
	if _, err := tx.ExecContext(ctx, down); err != nil {
		return fmt.Errorf("%w: down does not apply: %w\n%s", ErrInvalidMigration, err, strings.TrimSpace(down))
	}
	return nil
}
