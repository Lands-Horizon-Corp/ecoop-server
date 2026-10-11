package sql_test

import (
	"context"
	"time"

	"github.com/uptrace/bun"
)

// Models for the schema diff tests. Each test owns its tables, and each
// numbered type is the same table at a later release.

// A new schema with a relation.
type DiffCustomer struct {
	bun.BaseModel `bun:"table:diff_customers"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Name          string `bun:"name,notnull"`
}

type DiffAccount struct {
	bun.BaseModel `bun:"table:diff_accounts"`
	ID            int64         `bun:"id,pk,autoincrement"`
	CustomerID    int64         `bun:"customer_id,notnull"`
	Customer      *DiffCustomer `bun:"rel:belongs-to,join:customer_id=id"`
}

// Renamed column: Nick becomes Handle, same type.
type RenameV1 struct {
	bun.BaseModel `bun:"table:renames"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Nick          string `bun:"nick"`
}

type RenameV2 struct {
	bun.BaseModel `bun:"table:renames"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Handle        string `bun:"handle"`
}

// Evolution of one table across releases.
type EvoV1 struct {
	bun.BaseModel `bun:"table:evo"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Name          string `bun:"name,notnull"`
}

type EvoV2 struct {
	bun.BaseModel `bun:"table:evo"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Name          string `bun:"name,notnull"`
	Email         string `bun:"email"`
}

type EvoV3 struct {
	bun.BaseModel `bun:"table:evo"`
	ID            int64     `bun:"id,pk,autoincrement"`
	Name          string    `bun:"name,notnull"`
	Email         string    `bun:"email,unique"`
	JoinedAt      time.Time `bun:"joined_at,nullzero"`
}

type EvoV4 struct {
	bun.BaseModel `bun:"table:evo"`
	ID            int64     `bun:"id,pk,autoincrement"`
	Name          string    `bun:"name,notnull"`
	Email         string    `bun:"email,unique"`
	JoinedAt      time.Time `bun:"joined_at,nullzero"`
	Score         int64     `bun:"score,notnull,default:0"`
}

// Diff limits.
type DefaultV1 struct {
	bun.BaseModel `bun:"table:defaults"`
	ID            int64 `bun:"id,pk,autoincrement"`
}

type DefaultV2 struct {
	bun.BaseModel `bun:"table:defaults"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Status        string `bun:"status,notnull,default:'active'"`
}

type Money struct {
	bun.BaseModel `bun:"table:money"`
	ID            int64   `bun:"id,pk,autoincrement"`
	Amount        float64 `bun:"amount,type:numeric(14,2),notnull"`
}

type ViewedV1 struct {
	bun.BaseModel `bun:"table:viewed"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Code          string `bun:"code"`
}

type ViewedV2 struct {
	bun.BaseModel `bun:"table:viewed"`
	ID            int64 `bun:"id,pk,autoincrement"`
	Code          int64 `bun:"code"`
}

// An eight-table foreign key chain: each table references the previous.
type Chain1 struct {
	bun.BaseModel `bun:"table:chain1"`
	ID            int64 `bun:"id,pk,autoincrement"`
}

type Chain2 struct {
	bun.BaseModel `bun:"table:chain2"`
	ID            int64   `bun:"id,pk,autoincrement"`
	PrevID        int64   `bun:"prev_id"`
	Prev          *Chain1 `bun:"rel:belongs-to,join:prev_id=id"`
}

type Chain3 struct {
	bun.BaseModel `bun:"table:chain3"`
	ID            int64   `bun:"id,pk,autoincrement"`
	PrevID        int64   `bun:"prev_id"`
	Prev          *Chain2 `bun:"rel:belongs-to,join:prev_id=id"`
}

type Chain4 struct {
	bun.BaseModel `bun:"table:chain4"`
	ID            int64   `bun:"id,pk,autoincrement"`
	PrevID        int64   `bun:"prev_id"`
	Prev          *Chain3 `bun:"rel:belongs-to,join:prev_id=id"`
}

type Chain5 struct {
	bun.BaseModel `bun:"table:chain5"`
	ID            int64   `bun:"id,pk,autoincrement"`
	PrevID        int64   `bun:"prev_id"`
	Prev          *Chain4 `bun:"rel:belongs-to,join:prev_id=id"`
}

type Chain6 struct {
	bun.BaseModel `bun:"table:chain6"`
	ID            int64   `bun:"id,pk,autoincrement"`
	PrevID        int64   `bun:"prev_id"`
	Prev          *Chain5 `bun:"rel:belongs-to,join:prev_id=id"`
}

type Chain7 struct {
	bun.BaseModel `bun:"table:chain7"`
	ID            int64   `bun:"id,pk,autoincrement"`
	PrevID        int64   `bun:"prev_id"`
	Prev          *Chain6 `bun:"rel:belongs-to,join:prev_id=id"`
}

type Chain8 struct {
	bun.BaseModel `bun:"table:chain8"`
	ID            int64   `bun:"id,pk,autoincrement"`
	PrevID        int64   `bun:"prev_id"`
	Prev          *Chain7 `bun:"rel:belongs-to,join:prev_id=id"`
}

var chainModels = []any{(*Chain1)(nil), (*Chain2)(nil), (*Chain3)(nil), (*Chain4)(nil),
	(*Chain5)(nil), (*Chain6)(nil), (*Chain7)(nil), (*Chain8)(nil)}

// EvoV4 with its indexes declared, as Migrate needs them: Atlas diffs the
// tables Bun creates, hooks included.
type EvoV4Idx struct {
	bun.BaseModel `bun:"table:evo"`
	ID            int64     `bun:"id,pk,autoincrement"`
	Name          string    `bun:"name,notnull"`
	Email         string    `bun:"email,unique"`
	JoinedAt      time.Time `bun:"joined_at,nullzero"`
	Score         int64     `bun:"score,notnull,default:0"`
}

func (m *EvoV4Idx) AfterCreateTable(ctx context.Context, q *bun.CreateTableQuery) error {
	for _, stmt := range []string{
		"CREATE INDEX evo_name_idx ON evo (name)",
		"CREATE INDEX evo_joined_idx ON evo (joined_at) WHERE joined_at IS NOT NULL",
		"CREATE INDEX evo_score_idx ON evo (score DESC)",
	} {
		if _, err := q.DB().ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

type DiffAccountIdx struct {
	bun.BaseModel `bun:"table:diff_accounts"`
	ID            int64         `bun:"id,pk,autoincrement"`
	CustomerID    int64         `bun:"customer_id,notnull"`
	Customer      *DiffCustomer `bun:"rel:belongs-to,join:customer_id=id"`
}

func (m *DiffAccountIdx) AfterCreateTable(ctx context.Context, q *bun.CreateTableQuery) error {
	_, err := q.DB().NewCreateIndex().Model(m).Index("diff_accounts_customer_idx").Column("customer_id").Exec(ctx)
	return err
}

// Broken names a column type Postgres does not have.
type Broken struct {
	bun.BaseModel `bun:"table:broken"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Value         string `bun:"value,type:no_such_type"`
}
