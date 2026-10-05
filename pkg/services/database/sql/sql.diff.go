package sql

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
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
//
// Before anything is written, Up and Down are executed in a transaction that is rolled back, so a
// file that would fail to apply or revert is reported as ErrInvalidMigration instead of being saved.
// The result is still a draft: renames and data-preserving type changes must be reviewed and edited.
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

	upSQL := files[0].Content
	downSQL := nameDroppedConstraints(upSQL, files[1].Content)
	if err := s.validateMigration(ctx, upSQL, downSQL); err != nil {
		return "", err
	}

	content := gooseFile(upSQL, downSQL)
	path, err := nextMigrationPath(slug)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return "", fmt.Errorf("writing migration %q: %w", path, err)
	}
	return path, nil
}

// nextMigrationPath returns a path whose version is a UTC timestamp, bumped past any existing
// version so two migrations generated in the same second still sort and apply in order.
func nextMigrationPath(slug string) (string, error) {
	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		return "", fmt.Errorf("reading migrations directory: %w", err)
	}
	version, _ := strconv.ParseInt(time.Now().UTC().Format("20060102150405"), 10, 64)
	for _, entry := range entries {
		prefix, _, _ := strings.Cut(entry.Name(), "_")
		if existing, err := strconv.ParseInt(prefix, 10, 64); err == nil && existing >= version {
			version = existing + 1
		}
	}
	return filepath.Join(migrationsDir, fmt.Sprintf("%d_%s.sql", version, slug)), nil
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

var (
	addConstraint  = regexp.MustCompile(`ALTER TABLE ((?:"[^"]+"\.)?"[^"]+") ADD CONSTRAINT "([^"]+)"`)
	dropUnnamedFK  = regexp.MustCompile(`ALTER TABLE ((?:"[^"]+"\.)?"[^"]+") DROP CONSTRAINT ""`)
	unqualifiedTbl = regexp.MustCompile(`^"public"\.`)
)

// nameDroppedConstraints fills in the constraint names bun leaves empty (DROP CONSTRAINT "") in a
// Down script, taking them from the matching ADD CONSTRAINT in Up. Down undoes Up in reverse order.
func nameDroppedConstraints(up, down string) string {
	names := map[string][]string{}
	for _, m := range addConstraint.FindAllStringSubmatch(up, -1) {
		table := unqualifiedTbl.ReplaceAllString(m[1], "")
		names[table] = append(names[table], m[2])
	}
	return dropUnnamedFK.ReplaceAllStringFunc(down, func(stmt string) string {
		table := unqualifiedTbl.ReplaceAllString(dropUnnamedFK.FindStringSubmatch(stmt)[1], "")
		stack := names[table]
		if len(stack) == 0 {
			return stmt
		}
		name := stack[len(stack)-1]
		names[table] = stack[:len(stack)-1]
		return strings.TrimSuffix(stmt, `""`) + `"` + name + `"`
	})
}

// validateMigration runs Up and then Down inside a transaction that is always rolled back, so a
// migration that would not apply (or not revert) is rejected before it is written to disk.
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
