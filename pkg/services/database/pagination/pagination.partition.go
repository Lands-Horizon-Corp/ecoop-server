package pagination

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/Lands-Horizon-Corp/ecoop-server/utils"
	"github.com/uptrace/bun"
)

func (c *PaginationService[TData, TID]) EnablePartitioning(
	ctx context.Context, control string, interval string,
) error {
	if c.ReadSQLService == nil {
		return ErrReadServiceRequired
	}
	if utils.BunColumnFieldIndex[TData](control) == -1 {
		return fmt.Errorf("%w: partition control column %q", ErrUnknownField, control)
	}
	if !bunFieldIsPK[TData](control) {
		return fmt.Errorf(
			"%w: %q must be tagged as part of TData's primary key (bun:\"%s,pk,...\"); "+
				"Postgres requires a partitioned table's primary key to include the partitioning column",
			ErrInvalidPartitionKey, control, control,
		)
	}

	db := c.ReadSQLService.Client()
	table := db.Table(reflect.TypeFor[TData]())
	var schemaName string
	if err := db.NewRaw("SELECT current_schema()").Scan(ctx, &schemaName); err != nil {
		return fmt.Errorf("resolving current schema: %w", err)
	}
	if schemaName == "" {
		schemaName = "public"
	}
	qualifiedTable := schemaName + "." + table.Name

	if _, err := db.NewCreateTable().
		Model((*TData)(nil)).
		PartitionBy("RANGE (?)", bun.Ident(control)).
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating partitioned table: %w", err)
	}
	registered, err := db.NewSelect().
		TableExpr("partman.part_config").
		Where("parent_table = ?", qualifiedTable).
		Exists(ctx)
	if err != nil {
		return fmt.Errorf("checking pg_partman registration: %w", err)
	}
	if registered {
		return nil
	}
	if _, err := db.NewRaw(
		"SELECT partman.create_parent(p_parent_table => ?, p_control => ?, p_type => 'range', p_interval => ?)",
		qualifiedTable, control, interval,
	).Exec(ctx); err != nil {
		return fmt.Errorf("registering with pg_partman: %w", err)
	}
	return nil
}

func bunFieldIsPK[T any](column string) bool {
	t := reflect.TypeFor[T]()
	if t.Kind() != reflect.Struct {
		return false
	}
	for field := range t.Fields() {
		if !field.IsExported() {
			continue
		}
		tag := field.Tag.Get("bun")
		if tag == "" || tag == "-" {
			continue
		}
		parts := strings.Split(tag, ",")
		if parts[0] != column {
			continue
		}
		return slices.Contains(parts[1:], "pk")
	}
	return false
}
