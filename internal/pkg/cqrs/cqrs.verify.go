package cqrs

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// readDB is the read database's pool, or an error when there is none. Without a
// separate read database the model has nothing to replicate into.
func (c *CQRSService[TData, TResponse, TRequest, TID]) readDB() (*bun.DB, error) {
	if c == nil || c.st == nil {
		return nil, ErrNotConstructed
	}
	if utils.IsNil(c.ReadSQLService) {
		return nil, ErrReadDBNotInitialized
	}
	db := c.ReadSQLService.Client()
	if db == nil {
		return nil, ErrReadDBNotInitialized
	}
	return db, nil
}

// verify checks, before replication starts, everything that would otherwise
// fail on the first change: both databases answer, the checkpoint table
// exists, the model's table and every column of it exist in the read database,
// and the write table sends full rows.
func (c *CQRSService[TData, TResponse, TRequest, TID]) verify(ctx context.Context) error {
	wdb, err := c.writeDB()
	if err != nil {
		return err
	}
	rdb, err := c.readDB()
	if err != nil {
		return err
	}
	if err := c.WriteSQLService.Ping(ctx); err != nil {
		return fmt.Errorf("%w: %w", ErrWriteDBUnreachable, err)
	}
	if err := c.ReadSQLService.Ping(ctx); err != nil {
		return fmt.Errorf("%w: %w", ErrReadDBUnreachable, err)
	}
	if err := c.verifyReadTables(ctx, rdb); err != nil {
		return err
	}
	return c.verifyReplicaIdentity(ctx, wdb)
}

// verifyReadTables confirms the read database has the checkpoint table and the
// model's table with every column the model maps.
func (c *CQRSService[TData, TResponse, TRequest, TID]) verifyReadTables(ctx context.Context, db *bun.DB) error {
	var cpCols []string
	if err := db.NewRaw(`SELECT column_name FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = ?`, CheckpointTable).Scan(ctx, &cpCols); err != nil {
		return fmt.Errorf("inspecting the read db: %w", err)
	}
	if len(cpCols) == 0 {
		return fmt.Errorf("%w: table %s (create it with CheckpointDDL)", ErrReadTableMissing, CheckpointTable)
	}
	if c.OrderByLSN {
		var rsCols []string
		if err := db.NewRaw(`SELECT column_name FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = ?`, RowStateTable).Scan(ctx, &rsCols); err != nil {
			return fmt.Errorf("inspecting the read db: %w", err)
		}
		if len(rsCols) == 0 {
			return fmt.Errorf("%w: table %s (OrderByLSN needs it; create it with RowStateDDL)", ErrReadTableMissing, RowStateTable)
		}
	}
	var have []string
	if err := db.NewRaw(`SELECT column_name FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ?`, c.model.schema, c.model.name).Scan(ctx, &have); err != nil {
		return fmt.Errorf("inspecting the read db: %w", err)
	}
	if len(have) == 0 {
		return fmt.Errorf("%w: %s.%s", ErrReadTableMissing, c.model.schema, c.model.name)
	}
	var missing []string
	for _, col := range c.model.cols {
		if !slices.Contains(have, col) {
			missing = append(missing, col)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("%w: %s.%s lacks columns %s", ErrReadTableMissing, c.model.schema, c.model.name, strings.Join(missing, ", "))
	}
	return nil
}

// verifyReplicaIdentity requires REPLICA IDENTITY FULL on the write table.
// Without it Debezium leaves out an unchanged TOAST column on an update, and
// the read model would be written a placeholder (or lose the value).
func (c *CQRSService[TData, TResponse, TRequest, TID]) verifyReplicaIdentity(ctx context.Context, db *bun.DB) error {
	var ident []string
	err := db.NewRaw(`SELECT c.relreplident::text FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = ? AND c.relname = ?`, c.model.schema, c.model.name).Scan(ctx, &ident)
	if err != nil {
		return fmt.Errorf("inspecting the write db: %w", err)
	}
	if len(ident) == 0 {
		return fmt.Errorf("%w: %s.%s", ErrWriteTableMissing, c.model.schema, c.model.name)
	}
	if ident[0] != "f" {
		return fmt.Errorf("%w: run ALTER TABLE %s REPLICA IDENTITY FULL", ErrReplicaIdentity, c.model.qualified)
	}
	return nil
}

// RowStateDDL creates the table OrderByLSN keeps in the read database: per row,
// the WAL position of the last change applied to it, and whether that change
// deleted the row. A deleted row keeps its entry (a tombstone), which is what
// lets an older insert, replayed after the delete, be turned away. Idempotent.
const RowStateDDL = `CREATE TABLE IF NOT EXISTS cqrs_row_state (
    table_name  text        NOT NULL,
    row_key     text        NOT NULL,
    lsn         bigint      NOT NULL,
    deleted     boolean     NOT NULL DEFAULT false,
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (table_name, row_key)
)`

// CheckpointDDL creates the checkpoint table in the read database; run it with
// the read database's migrations. It is idempotent.
const CheckpointDDL = `CREATE TABLE IF NOT EXISTS cqrs_checkpoints (
    consumer_group  text        NOT NULL,
    topic           text        NOT NULL,
    partition       integer     NOT NULL,
    next_offset     bigint      NOT NULL DEFAULT 0,
    last_record_at  timestamptz,
    updated_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_group, topic, partition)
)`
