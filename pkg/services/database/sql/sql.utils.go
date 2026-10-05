package sql

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pressly/goose/v3"
)

func nextMigrationPath(dir, slug string) (string, error) {
	entries, err := os.ReadDir(dir)
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
	return filepath.Join(dir, fmt.Sprintf("%d_%s.sql", version, slug)), nil
}

func gooseFile(up, down string) string {
	return "-- +goose Up\n" + strings.TrimSpace(up) + "\n\n-- +goose Down\n" + strings.TrimSpace(down) + "\n"
}

// orderStatements puts the statements bun generated into dependency order. bun walks its change set in
// map order, so it can emit a foreign key before the column it uses or drop a column before its constraint.
// Statements of the same kind keep bun's relative order.
func orderStatements(sql string) string {
	var stmts, cur []string
	inComment := false
	for line := range strings.SplitSeq(strings.TrimSpace(sql), "\n") {
		cur = append(cur, line)
		if strings.Contains(line, "/*") && !strings.Contains(line, "*/") {
			inComment = true
		}
		if inComment {
			if !strings.Contains(line, "*/") {
				continue
			}
			inComment = false
		}
		if strings.HasSuffix(strings.TrimSpace(line), ";") {
			stmts = append(stmts, strings.Join(cur, "\n"))
			cur = nil
		}
	}
	if len(cur) > 0 {
		stmts = append(stmts, strings.Join(cur, "\n"))
	}
	sort.SliceStable(stmts, func(i, j int) bool { return statementRank(stmts[i]) < statementRank(stmts[j]) })
	return strings.Join(stmts, "\n")
}

func statementRank(stmt string) int {
	s := strings.ToUpper(strings.TrimSpace(blockComment.ReplaceAllString(stmt, "")))
	switch {
	case strings.HasPrefix(s, "CREATE TABLE"):
		return 0
	case strings.HasPrefix(s, "DROP TABLE"):
		return 6
	case !strings.HasPrefix(s, "ALTER TABLE"):
		return 3
	case strings.Contains(s, " DROP CONSTRAINT"):
		return 1
	case strings.Contains(s, " ADD COLUMN"):
		return 2
	case strings.Contains(s, " DROP COLUMN"):
		return 5
	case strings.Contains(s, " ADD CONSTRAINT") && strings.Contains(s, "FOREIGN KEY"):
		return 4
	default:
		return 3
	}
}

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

func requireSteps(ctx context.Context, migrator *goose.Provider, steps int, state goose.State) error {
	statuses, err := migrator.Status(ctx)
	if err != nil {
		return fmt.Errorf("failed to get migration status: %w", err)
	}
	available := 0
	for _, st := range statuses {
		if st.State == state {
			available++
		}
	}
	if steps > available {
		return fmt.Errorf("%w: %d requested, %d available", ErrTooManySteps, steps, available)
	}
	return nil
}
