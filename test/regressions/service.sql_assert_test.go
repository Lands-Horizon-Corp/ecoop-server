package regressions

import (
	"fmt"
	"strings"
	"testing"

	sqlsvc "github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"
)

// Assertions shared by every test that generates migrations with Diff.

// diffApply generates one migration for models, checks it is a goose Up/Down file, applies it and
// returns its text. Use settleModels when the change may need more than one migration.
func diffApply(t *testing.T, svc sqlsvc.SQLServices, name string, models ...any) string {
	t.Helper()
	path, err := svc.Diff(bg, name, models...)
	if err != nil {
		t.Fatalf("Diff(%s): %v", name, err)
	}
	if path == "" {
		t.Fatalf("Diff(%s) found nothing to do", name)
	}
	content := readFile(t, path)
	if !strings.HasPrefix(content, "-- +goose Up\n") || !strings.Contains(content, "\n-- +goose Down\n") {
		t.Fatalf("Diff(%s) did not write a goose Up/Down file:\n%s", name, content)
	}
	if err := svc.Migrate(bg); err != nil {
		t.Fatalf("Migrate after Diff(%s): %v\n%s", name, err, content)
	}
	return content
}

// settleModels applies Diff and Migrate repeatedly until the database matches models, and returns each
// migration's text. bun adds a NOT NULL column in two phases (add, then SET NOT NULL), so one model
// change can need two migrations. onApplied, if given, runs after each applied migration.
func settleModels(t *testing.T, svc sqlsvc.SQLServices, name string, models []any, onApplied ...func()) []string {
	t.Helper()
	var contents []string
	for round := 1; round <= 4; round++ {
		path, err := svc.Diff(bg, fmt.Sprintf("%s round %d", name, round), models...)
		if err != nil {
			t.Fatalf("Diff(%s) round %d: %v", name, round, err)
		}
		if path == "" {
			if len(contents) == 0 {
				t.Fatalf("Diff(%s) found nothing to do", name)
			}
			return contents
		}
		content := readFile(t, path)
		if err := svc.Migrate(bg); err != nil {
			t.Fatalf("Migrate after Diff(%s) round %d: %v\n%s", name, round, err, content)
		}
		contents = append(contents, content)
		for _, fn := range onApplied {
			fn()
		}
	}
	t.Fatalf("Diff(%s) did not converge after 4 migrations", name)
	return nil
}

// requireConverged asserts that the database already matches models: a second Diff has nothing to say
// and writes no file.
func requireConverged(t *testing.T, e *sqlEnv, svc sqlsvc.SQLServices, models ...any) {
	t.Helper()
	before := len(e.migrationFiles())
	path, err := svc.Diff(bg, "converge-check", models...)
	if err != nil {
		t.Fatalf("convergence Diff: %v", err)
	}
	if path != "" {
		t.Fatalf("schema did not converge; a second Diff still wants:\n%s", readFile(t, path))
	}
	if after := len(e.migrationFiles()); after != before {
		t.Fatalf("a no-op Diff wrote a file (%d -> %d files)", before, after)
	}
}

// gooseUp returns the Up half of a goose migration file's text.
func gooseUp(content string) string {
	u, _, _ := strings.Cut(content, "-- +goose Down")
	return u
}

// gooseDown returns the Down half of a goose migration file's text.
func gooseDown(content string) string {
	_, d, _ := strings.Cut(content, "-- +goose Down")
	return d
}
