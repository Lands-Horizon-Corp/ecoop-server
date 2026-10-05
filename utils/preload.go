package utils

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/schema"
)

func ResolvePreload(preload []string, defaultPreloads []string) []string {
	if preload == nil {
		preload = defaultPreloads
	}
	if len(preload) == 0 {
		preload = defaultPreloads
	}
	if len(preload) == 1 && preload[0] == "" {
		preload = []string{}
	}
	return preload
}

func ValidPreloads[TData any](db bun.IDB, preload []string) (valid, dropped []string) {
	table := db.Dialect().Tables().Get(reflect.TypeFor[TData]())
	for _, raw := range preload {
		if raw == "" {
			continue
		}
		resolved, ok := resolveRelationPath(table, raw)
		if !ok {
			dropped = append(dropped, raw)
			continue
		}
		valid = append(valid, resolved)
	}
	return valid, dropped
}

func resolveRelationPath(table *schema.Table, raw string) (resolved string, ok bool) {
	segments := strings.Split(raw, ".")
	names := make([]string, 0, len(segments))
	current := table
	for _, seg := range segments {
		if current == nil {
			return "", false
		}
		name := ToPascalCase(seg)
		if name == "" {
			return "", false
		}
		rel, exists := current.Relations[name]
		if !exists {
			return "", false
		}
		names = append(names, name)
		current = rel.JoinTable
	}
	return strings.Join(names, "."), true
}

func ApplyPreloadsMany[TData any](
	ctx context.Context,
	db bun.IDB,
	data *[]TData,
	defaultPreloads []string,
	preload ...string,
) (dropped []string, err error) {
	resolved := ResolvePreload(preload, defaultPreloads)
	if len(resolved) == 0 || len(*data) == 0 {
		return nil, nil
	}
	valid, dropped := ValidPreloads[TData](db, resolved)
	if len(valid) == 0 {
		return dropped, nil
	}
	q := db.NewSelect().Model(data).WherePK()
	for _, rel := range valid {
		q = q.Relation(rel)
	}
	if err := q.Scan(ctx); err != nil {
		return dropped, fmt.Errorf("loading preloads %v: %w", valid, err)
	}
	return dropped, nil
}
