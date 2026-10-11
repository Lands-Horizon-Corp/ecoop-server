package sql_test

import "github.com/uptrace/bun"

// Test models. MemberV2 is MemberV1 with one more column, as a model evolves.

type MemberV1 struct {
	bun.BaseModel `bun:"table:members"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Name          string `bun:"name,notnull"`
}

type MemberV2 struct {
	bun.BaseModel `bun:"table:members"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Name          string `bun:"name,notnull"`
	Email         string `bun:"email"`
}

type Loan struct {
	bun.BaseModel `bun:"table:loans"`
	ID            int64   `bun:"id,pk,autoincrement"`
	MemberID      int64   `bun:"member_id"`
	Amount        float64 `bun:"amount,notnull"`
}

type Branch struct {
	bun.BaseModel `bun:"table:branches"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Code          string `bun:"code,unique"`
}
