//go:build integration

package sql_test

import (
	"context"
	"time"

	"github.com/uptrace/bun"
)

// Indexes and checks are declared on the models, in Go, with Bun's
// AfterCreateTable hook: Migrate creates each model's table in a scratch
// database and Atlas diffs the result, hooks included. A type gets the hook
// from the release that adds the index or check onward.

func execDDL(ctx context.Context, q *bun.CreateTableQuery, stmts ...string) error {
	for _, stmt := range stmts {
		if _, err := q.DB().ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// ledgerIndex is release 5's index on the ledger, wherever the table is.
func ledgerIndex(ctx context.Context, q *bun.CreateTableQuery, model any) error {
	_, err := q.DB().NewCreateIndex().Model(model).Index("ledger_entries_account_created_idx").
		Column("account_id", "created_at").Exec(ctx)
	return err
}

// ledgerCheck is release 12's check on the ledger, wherever the table is.
func ledgerCheck(ctx context.Context, q *bun.CreateTableQuery, table string) error {
	return execDDL(ctx, q, `ALTER TABLE "`+table+`" ADD CONSTRAINT ledger_entries_amount_nonzero CHECK (amount <> 0)`)
}

func memberIndex(ctx context.Context, q *bun.CreateTableQuery, model any) error {
	_, err := q.DB().NewCreateIndex().Model(model).Index("members_lower_email_idx").ColumnExpr("lower(email)").Exec(ctx)
	return err
}

func accountCheck(ctx context.Context, q *bun.CreateTableQuery) error {
	return execDDL(ctx, q, `ALTER TABLE accounts ADD CONSTRAINT accounts_balance_nonneg CHECK (balance >= 0)`)
}

// The ledger foundation: a cooperative bank's schema across twenty releases,
// written only as Bun models (plus the hand-written migrations Bun cannot
// express). Each type is one table at one release; a release lists every
// table, because a model left out is a table to drop.

// branches

type LBranch struct {
	bun.BaseModel `bun:"table:branches"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Code          string `bun:"code,notnull,unique"`
	Name          string `bun:"name,notnull"`
}

// R18: the branch name moves out (destructive, reviewed).
type LBranchNoName struct {
	bun.BaseModel `bun:"table:branches"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Code          string `bun:"code,notnull,unique"`
}

// members

type LMember struct {
	bun.BaseModel `bun:"table:members"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Name          string `bun:"name,notnull"`
	Email         string `bun:"email,nullzero"`
}

// R3: members belong to a branch.
type LMemberBranch struct {
	bun.BaseModel `bun:"table:members"`
	ID            int64    `bun:"id,pk,autoincrement"`
	Name          string   `bun:"name,notnull"`
	Email         string   `bun:"email,nullzero"`
	BranchID      int64    `bun:"branch_id,nullzero"`
	Branch        *LBranch `bun:"rel:belongs-to,join:branch_id=id"`
}

// R8: name renamed to full_name.
type LMemberFullName struct {
	bun.BaseModel `bun:"table:members"`
	ID            int64    `bun:"id,pk,autoincrement"`
	FullName      string   `bun:"full_name,notnull"`
	Email         string   `bun:"email,nullzero"`
	BranchID      int64    `bun:"branch_id,nullzero"`
	Branch        *LBranch `bun:"rel:belongs-to,join:branch_id=id"`
}

// R13: email becomes required and unique (existing data must be fixed first).
type LMemberEmail struct {
	bun.BaseModel `bun:"table:members"`
	ID            int64    `bun:"id,pk,autoincrement"`
	FullName      string   `bun:"full_name,notnull"`
	Email         string   `bun:"email,notnull,unique"`
	BranchID      int64    `bun:"branch_id,nullzero"`
	Branch        *LBranch `bun:"rel:belongs-to,join:branch_id=id"`
}

// accounts

type LAccount struct {
	bun.BaseModel `bun:"table:accounts"`
	ID            int64    `bun:"id,pk,autoincrement"`
	MemberID      int64    `bun:"member_id,notnull"`
	Member        *LMember `bun:"rel:belongs-to,join:member_id=id"`
	Number        string   `bun:"number,notnull,unique"`
	Balance       int64    `bun:"balance,notnull,default:0"`
}

// R6: a currency with a string default.
type LAccountCurrency struct {
	bun.BaseModel `bun:"table:accounts"`
	ID            int64    `bun:"id,pk,autoincrement"`
	MemberID      int64    `bun:"member_id,notnull"`
	Member        *LMember `bun:"rel:belongs-to,join:member_id=id"`
	Number        string   `bun:"number,notnull,unique"`
	Balance       int64    `bun:"balance,notnull,default:0"`
	Currency      string   `bun:"currency,type:varchar(3),notnull,default:'PHP'"`
}

// R14: sub-accounts reference a parent account (self-reference).
type LAccountParent struct {
	bun.BaseModel `bun:"table:accounts"`
	ID            int64           `bun:"id,pk,autoincrement"`
	MemberID      int64           `bun:"member_id,notnull"`
	Member        *LMember        `bun:"rel:belongs-to,join:member_id=id"`
	Number        string          `bun:"number,notnull,unique"`
	Balance       int64           `bun:"balance,notnull,default:0"`
	Currency      string          `bun:"currency,type:varchar(3),notnull,default:'PHP'"`
	ParentID      int64           `bun:"parent_id,nullzero"`
	Parent        *LAccountParent `bun:"rel:belongs-to,join:parent_id=id"`
}

// R19: ownership moves to account_holders; member_id is dropped (reviewed).
type LAccountNoMember struct {
	bun.BaseModel `bun:"table:accounts"`
	ID            int64             `bun:"id,pk,autoincrement"`
	Number        string            `bun:"number,notnull,unique"`
	Balance       int64             `bun:"balance,notnull,default:0"`
	Currency      string            `bun:"currency,type:varchar(3),notnull,default:'PHP'"`
	ParentID      int64             `bun:"parent_id,nullzero"`
	Parent        *LAccountNoMember `bun:"rel:belongs-to,join:parent_id=id"`
}

// account_holders (R7): joint accounts, a many-to-many join with a
// composite primary key.
type LHolder struct {
	bun.BaseModel `bun:"table:account_holders"`
	AccountID     int64     `bun:"account_id,pk"`
	Account       *LAccount `bun:"rel:belongs-to,join:account_id=id"`
	MemberID      int64     `bun:"member_id,pk"`
	Member        *LMember  `bun:"rel:belongs-to,join:member_id=id"`
	Role          string    `bun:"role,notnull"`
}

// ledger_entries (R4), renamed journal_entries at R15.

type LEntry struct {
	bun.BaseModel `bun:"table:ledger_entries"`
	ID            int64     `bun:"id,pk,autoincrement"`
	AccountID     int64     `bun:"account_id,notnull"`
	Account       *LAccount `bun:"rel:belongs-to,join:account_id=id"`
	Amount        int64     `bun:"amount,notnull"`
	CreatedAt     time.Time `bun:"created_at,notnull,default:current_timestamp"`
}

// R11: entries made by a transfer point at it.
type LEntryTransfer struct {
	bun.BaseModel `bun:"table:ledger_entries"`
	ID            int64      `bun:"id,pk,autoincrement"`
	AccountID     int64      `bun:"account_id,notnull"`
	Account       *LAccount  `bun:"rel:belongs-to,join:account_id=id"`
	Amount        int64      `bun:"amount,notnull"`
	CreatedAt     time.Time  `bun:"created_at,notnull,default:current_timestamp"`
	TransferID    int64      `bun:"transfer_id,nullzero"`
	Transfer      *LTransfer `bun:"rel:belongs-to,join:transfer_id=id"`
}

// R15: the table is renamed journal_entries.
type LJournal struct {
	bun.BaseModel `bun:"table:journal_entries"`
	ID            int64      `bun:"id,pk,autoincrement"`
	AccountID     int64      `bun:"account_id,notnull"`
	Account       *LAccount  `bun:"rel:belongs-to,join:account_id=id"`
	Amount        int64      `bun:"amount,notnull"`
	CreatedAt     time.Time  `bun:"created_at,notnull,default:current_timestamp"`
	TransferID    int64      `bun:"transfer_id,nullzero"`
	Transfer      *LTransfer `bun:"rel:belongs-to,join:transfer_id=id"`
}

// R16: an idempotency key, unique per account (composite unique).
type LJournalRef struct {
	bun.BaseModel `bun:"table:journal_entries"`
	ID            int64      `bun:"id,pk,autoincrement"`
	AccountID     int64      `bun:"account_id,notnull,unique:journal_account_ref"`
	Account       *LAccount  `bun:"rel:belongs-to,join:account_id=id"`
	Amount        int64      `bun:"amount,notnull"`
	CreatedAt     time.Time  `bun:"created_at,notnull,default:current_timestamp"`
	TransferID    int64      `bun:"transfer_id,nullzero"`
	Transfer      *LTransfer `bun:"rel:belongs-to,join:transfer_id=id"`
	ExternalRef   string     `bun:"external_ref,nullzero,unique:journal_account_ref"`
}

// loans (R9)

type LLoan struct {
	bun.BaseModel `bun:"table:loans"`
	ID            int64     `bun:"id,pk,autoincrement"`
	MemberID      int64     `bun:"member_id,notnull"`
	Member        *LMember  `bun:"rel:belongs-to,join:member_id=id"`
	AccountID     int64     `bun:"account_id,notnull"`
	Account       *LAccount `bun:"rel:belongs-to,join:account_id=id"`
	Principal     int64     `bun:"principal,notnull"`
	RateBps       int32     `bun:"rate_bps,notnull"`
	TermMonths    int32     `bun:"term_months,notnull"`
	Status        string    `bun:"status,notnull"`
}

// R17: term widened from integer to bigint.
type LLoanWide struct {
	bun.BaseModel `bun:"table:loans"`
	ID            int64     `bun:"id,pk,autoincrement"`
	MemberID      int64     `bun:"member_id,notnull"`
	Member        *LMember  `bun:"rel:belongs-to,join:member_id=id"`
	AccountID     int64     `bun:"account_id,notnull"`
	Account       *LAccount `bun:"rel:belongs-to,join:account_id=id"`
	Principal     int64     `bun:"principal,notnull"`
	RateBps       int32     `bun:"rate_bps,notnull"`
	TermMonths    int64     `bun:"term_months,notnull"`
	Status        string    `bun:"status,notnull"`
}

// loan_payments (R10): each payment is exactly one ledger entry (one-to-one).
type LPayment struct {
	bun.BaseModel `bun:"table:loan_payments"`
	ID            int64     `bun:"id,pk,autoincrement"`
	LoanID        int64     `bun:"loan_id,notnull"`
	Loan          *LLoan    `bun:"rel:belongs-to,join:loan_id=id"`
	EntryID       int64     `bun:"entry_id,notnull,unique"`
	Entry         *LEntry   `bun:"rel:belongs-to,join:entry_id=id"`
	Amount        int64     `bun:"amount,notnull"`
	PaidAt        time.Time `bun:"paid_at,notnull,default:current_timestamp"`
}

// R15: the entry it points at lives in journal_entries.
type LPaymentJournal struct {
	bun.BaseModel `bun:"table:loan_payments"`
	ID            int64     `bun:"id,pk,autoincrement"`
	LoanID        int64     `bun:"loan_id,notnull"`
	Loan          *LLoan    `bun:"rel:belongs-to,join:loan_id=id"`
	EntryID       int64     `bun:"entry_id,notnull,unique"`
	Entry         *LJournal `bun:"rel:belongs-to,join:entry_id=id"`
	Amount        int64     `bun:"amount,notnull"`
	PaidAt        time.Time `bun:"paid_at,notnull,default:current_timestamp"`
}

// transfers (R11): two foreign keys to the same table.
type LTransfer struct {
	bun.BaseModel `bun:"table:transfers"`
	ID            int64     `bun:"id,pk,autoincrement"`
	FromAccountID int64     `bun:"from_account_id,notnull"`
	From          *LAccount `bun:"rel:belongs-to,join:from_account_id=id"`
	ToAccountID   int64     `bun:"to_account_id,notnull"`
	To            *LAccount `bun:"rel:belongs-to,join:to_account_id=id"`
	Amount        int64     `bun:"amount,notnull"`
	CreatedAt     time.Time `bun:"created_at,notnull,default:current_timestamp"`
}

// statements (R20): one per account per period.
type LStatement struct {
	bun.BaseModel `bun:"table:statements"`
	ID            int64     `bun:"id,pk,autoincrement"`
	AccountID     int64     `bun:"account_id,notnull,unique:statement_account_period"`
	Account       *LAccount `bun:"rel:belongs-to,join:account_id=id"`
	Period        string    `bun:"period,type:varchar(7),notnull,unique:statement_account_period"`
	Closing       int64     `bun:"closing,notnull"`
}

// Release 5: the ledger and member email indexes.

type LEntryIdx struct {
	bun.BaseModel `bun:"table:ledger_entries"`
	ID            int64     `bun:"id,pk,autoincrement"`
	AccountID     int64     `bun:"account_id,notnull"`
	Account       *LAccount `bun:"rel:belongs-to,join:account_id=id"`
	Amount        int64     `bun:"amount,notnull"`
	CreatedAt     time.Time `bun:"created_at,notnull,default:current_timestamp"`
}

func (m *LEntryIdx) AfterCreateTable(ctx context.Context, q *bun.CreateTableQuery) error {
	return ledgerIndex(ctx, q, m)
}

type LMemberBranchIdx struct {
	bun.BaseModel `bun:"table:members"`
	ID            int64    `bun:"id,pk,autoincrement"`
	Name          string   `bun:"name,notnull"`
	Email         string   `bun:"email,nullzero"`
	BranchID      int64    `bun:"branch_id,nullzero"`
	Branch        *LBranch `bun:"rel:belongs-to,join:branch_id=id"`
}

func (m *LMemberBranchIdx) AfterCreateTable(ctx context.Context, q *bun.CreateTableQuery) error {
	return memberIndex(ctx, q, m)
}

func (m *LMemberFullName) AfterCreateTable(ctx context.Context, q *bun.CreateTableQuery) error {
	return memberIndex(ctx, q, m)
}

func (m *LMemberEmail) AfterCreateTable(ctx context.Context, q *bun.CreateTableQuery) error {
	return memberIndex(ctx, q, m)
}

func (m *LEntryTransfer) AfterCreateTable(ctx context.Context, q *bun.CreateTableQuery) error {
	return ledgerIndex(ctx, q, m)
}

// Release 12: checks on balances, entries and transfers, and transfer indexes.

type LAccountChecked struct {
	bun.BaseModel `bun:"table:accounts"`
	ID            int64    `bun:"id,pk,autoincrement"`
	MemberID      int64    `bun:"member_id,notnull"`
	Member        *LMember `bun:"rel:belongs-to,join:member_id=id"`
	Number        string   `bun:"number,notnull,unique"`
	Balance       int64    `bun:"balance,notnull,default:0"`
	Currency      string   `bun:"currency,type:varchar(3),notnull,default:'PHP'"`
}

func (m *LAccountChecked) AfterCreateTable(ctx context.Context, q *bun.CreateTableQuery) error {
	return accountCheck(ctx, q)
}

func (m *LAccountParent) AfterCreateTable(ctx context.Context, q *bun.CreateTableQuery) error {
	return accountCheck(ctx, q)
}

func (m *LAccountNoMember) AfterCreateTable(ctx context.Context, q *bun.CreateTableQuery) error {
	return accountCheck(ctx, q)
}

type LEntryChecked struct {
	bun.BaseModel `bun:"table:ledger_entries"`
	ID            int64      `bun:"id,pk,autoincrement"`
	AccountID     int64      `bun:"account_id,notnull"`
	Account       *LAccount  `bun:"rel:belongs-to,join:account_id=id"`
	Amount        int64      `bun:"amount,notnull"`
	CreatedAt     time.Time  `bun:"created_at,notnull,default:current_timestamp"`
	TransferID    int64      `bun:"transfer_id,nullzero"`
	Transfer      *LTransfer `bun:"rel:belongs-to,join:transfer_id=id"`
}

func (m *LEntryChecked) AfterCreateTable(ctx context.Context, q *bun.CreateTableQuery) error {
	if err := ledgerIndex(ctx, q, m); err != nil {
		return err
	}
	return ledgerCheck(ctx, q, "ledger_entries")
}

func (m *LJournal) AfterCreateTable(ctx context.Context, q *bun.CreateTableQuery) error {
	if err := ledgerIndex(ctx, q, m); err != nil {
		return err
	}
	return ledgerCheck(ctx, q, "journal_entries")
}

func (m *LJournalRef) AfterCreateTable(ctx context.Context, q *bun.CreateTableQuery) error {
	if err := ledgerIndex(ctx, q, m); err != nil {
		return err
	}
	return ledgerCheck(ctx, q, "journal_entries")
}

type LTransferChecked struct {
	bun.BaseModel `bun:"table:transfers"`
	ID            int64     `bun:"id,pk,autoincrement"`
	FromAccountID int64     `bun:"from_account_id,notnull"`
	From          *LAccount `bun:"rel:belongs-to,join:from_account_id=id"`
	ToAccountID   int64     `bun:"to_account_id,notnull"`
	To            *LAccount `bun:"rel:belongs-to,join:to_account_id=id"`
	Amount        int64     `bun:"amount,notnull"`
	CreatedAt     time.Time `bun:"created_at,notnull,default:current_timestamp"`
}

func (m *LTransferChecked) AfterCreateTable(ctx context.Context, q *bun.CreateTableQuery) error {
	return execDDL(ctx, q,
		`ALTER TABLE transfers ADD CONSTRAINT transfers_amount_positive CHECK (amount > 0)`,
		`ALTER TABLE transfers ADD CONSTRAINT transfers_distinct_accounts CHECK (from_account_id <> to_account_id)`,
		`CREATE INDEX transfers_from_idx ON transfers (from_account_id)`,
		`CREATE INDEX transfers_to_idx ON transfers (to_account_id)`)
}
