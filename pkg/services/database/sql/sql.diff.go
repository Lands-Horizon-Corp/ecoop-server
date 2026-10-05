package sql

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/uptrace/bun/migrate"
)

var nonNameChars = regexp.MustCompile(`[^a-z0-9]+`)

// Diff compares models with the live database and writes the difference as a goose
// migration (-- +goose Up / Down) in the migrations directory. It returns the new file's
// path, or "" when the database already matches the models.
//
// Pass every model the application owns: a table the models don't mention is generated as DROP TABLE.
// The database must be fully migrated first, otherwise the diff would repeat pending changes.
// The generated SQL is a draft: renames and data-preserving type changes must be reviewed and edited.
func (s *SQLService) Diff(ctx context.Context, name string, models ...any) (string, error) {
	if s.db == nil {
		return "", ErrNotInitialized
	}
	if len(models) == 0 {
		return "", ErrNoModels
	}
	slug := strings.Trim(nonNameChars.ReplaceAllString(strings.ToLower(name), "_"), "_")
	if slug == "" {
		return "", ErrInvalidName
	}
	if err := os.MkdirAll(migrationsDir, 0o755); err != nil {
		return "", fmt.Errorf("creating migrations directory: %w", err)
	}

	if err := s.requireNoPending(ctx); err != nil {
		return "", err
	}

	tmp, err := os.MkdirTemp("", "goose-diff-*")
	if err != nil {
		return "", fmt.Errorf("creating scratch directory: %w", err)
	}
	defer os.RemoveAll(tmp)
	am, err := migrate.NewAutoMigrator(s.db,
		migrate.WithModel(models...),
		migrate.WithExcludeTable(goose.DefaultTablename),
		migrate.WithMigrationsDirectoryAuto(tmp),
	)
	if err != nil {
		return "", fmt.Errorf("creating schema diff: %w", err)
	}
	files, err := am.CreateSQLMigrations(ctx)
	if err != nil {
		return "", fmt.Errorf("computing schema diff: %w", err)
	}
	if len(files) != 2 {
		return "", nil
	}

	content := gooseFile(files[0].Content, files[1].Content)
	path := filepath.Join(migrationsDir, time.Now().UTC().Format("20060102150405")+"_"+slug+".sql")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return "", fmt.Errorf("writing migration %q: %w", path, err)
	}
	return path, nil
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

func gooseFile(up, down string) string {
	return "-- +goose Up\n" + strings.TrimSpace(up) + "\n\n-- +goose Down\n" + strings.TrimSpace(down) + "\n"
}
