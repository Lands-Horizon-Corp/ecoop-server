package regressions

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	sqlsvc "github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/uptrace/bun"
)

// A retail-banking schema used by the migration smoke, sanity, canary and end-to-end tests.
// Money is stored as integer minor units (cents). Tables, keys and uniques come from the bun models via
// Diff; the invariants a bank needs (CHECKs, an immutable ledger, balanced double entry) are hand-written
// SQL in bankHardeningUp, because an auto-migrator cannot express them.

type bkBranch struct {
	bun.BaseModel `bun:"table:branches"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Code          string `bun:"code,notnull,unique,type:varchar(10)"`
	Name          string `bun:"name,notnull"`
	Country       string `bun:"country,notnull,type:varchar(2)"`
}

type bkCustomer struct {
	bun.BaseModel `bun:"table:customers"`
	ID            int64     `bun:"id,pk,autoincrement"`
	BranchID      int64     `bun:"branch_id,notnull"`
	LegalName     string    `bun:"legal_name,notnull"`
	TaxID         string    `bun:"tax_id,notnull,unique,type:varchar(32)"`
	KYCStatus     string    `bun:"kyc_status,notnull,type:varchar(16)"`
	CreatedAt     time.Time `bun:"created_at,notnull,default:current_timestamp"`
	Branch        *bkBranch `bun:"rel:belongs-to,join:branch_id=id"`
}

type bkAddress struct {
	bun.BaseModel `bun:"table:customer_addresses"`
	ID            int64       `bun:"id,pk,autoincrement"`
	CustomerID    int64       `bun:"customer_id,notnull"`
	Line1         string      `bun:"line1,notnull"`
	City          string      `bun:"city,notnull"`
	Country       string      `bun:"country,notnull,type:varchar(2)"`
	Customer      *bkCustomer `bun:"rel:belongs-to,join:customer_id=id"`
}

type bkProduct struct {
	bun.BaseModel  `bun:"table:account_products"`
	ID             int64  `bun:"id,pk,autoincrement"`
	Code           string `bun:"code,notnull,unique,type:varchar(10)"`
	Name           string `bun:"name,notnull"`
	Kind           string `bun:"kind,notnull,type:varchar(16)"`
	InterestBps    int32  `bun:"interest_bps,notnull,default:0"`
	OverdraftLimit int64  `bun:"overdraft_limit,notnull,default:0"`
}

type bkAccount struct {
	bun.BaseModel `bun:"table:accounts"`
	ID            int64       `bun:"id,pk,autoincrement"`
	CustomerID    int64       `bun:"customer_id,notnull"`
	ProductID     int64       `bun:"product_id,notnull"`
	Number        string      `bun:"number,notnull,unique,type:varchar(34)"`
	Currency      string      `bun:"currency,notnull,type:varchar(3)"`
	Status        string      `bun:"status,notnull,type:varchar(12)"`
	Balance       int64       `bun:"balance,notnull,default:0"`
	Version       int64       `bun:"version,notnull,default:0"`
	OpenedAt      time.Time   `bun:"opened_at,notnull,default:current_timestamp"`
	Customer      *bkCustomer `bun:"rel:belongs-to,join:customer_id=id"`
	Product       *bkProduct  `bun:"rel:belongs-to,join:product_id=id"`
}

// bkAccountV2 is the next release of accounts: a nullable nickname and a NOT NULL risk score.
type bkAccountV2 struct {
	bun.BaseModel `bun:"table:accounts"`
	ID            int64       `bun:"id,pk,autoincrement"`
	CustomerID    int64       `bun:"customer_id,notnull"`
	ProductID     int64       `bun:"product_id,notnull"`
	Number        string      `bun:"number,notnull,unique,type:varchar(34)"`
	Currency      string      `bun:"currency,notnull,type:varchar(3)"`
	Status        string      `bun:"status,notnull,type:varchar(12)"`
	Balance       int64       `bun:"balance,notnull,default:0"`
	Version       int64       `bun:"version,notnull,default:0"`
	OpenedAt      time.Time   `bun:"opened_at,notnull,default:current_timestamp"`
	Nickname      string      `bun:"nickname"`
	RiskScore     int32       `bun:"risk_score,notnull,default:0"`
	Customer      *bkCustomer `bun:"rel:belongs-to,join:customer_id=id"`
	Product       *bkProduct  `bun:"rel:belongs-to,join:product_id=id"`
}

// bkRiskProfile is new in the next release.
type bkRiskProfile struct {
	bun.BaseModel `bun:"table:risk_profiles"`
	ID            int64       `bun:"id,pk,autoincrement"`
	CustomerID    int64       `bun:"customer_id,notnull,unique"`
	Tier          int32       `bun:"tier,notnull,default:0"`
	ReviewedAt    time.Time   `bun:"reviewed_at,notnull,default:current_timestamp"`
	Customer      *bkCustomer `bun:"rel:belongs-to,join:customer_id=id"`
}

type bkCard struct {
	bun.BaseModel `bun:"table:cards"`
	ID            int64      `bun:"id,pk,autoincrement"`
	AccountID     int64      `bun:"account_id,notnull"`
	PANHash       string     `bun:"pan_hash,notnull,unique,type:varchar(64)"`
	Status        string     `bun:"status,notnull,type:varchar(12)"`
	ExpiresAt     time.Time  `bun:"expires_at,notnull"`
	Account       *bkAccount `bun:"rel:belongs-to,join:account_id=id"`
}

type bkTransaction struct {
	bun.BaseModel  `bun:"table:transactions"`
	ID             int64     `bun:"id,pk,autoincrement"`
	IdempotencyKey string    `bun:"idempotency_key,notnull,unique,type:varchar(64)"`
	Kind           string    `bun:"kind,notnull,type:varchar(16)"`
	Status         string    `bun:"status,notnull,type:varchar(12)"`
	CreatedAt      time.Time `bun:"created_at,notnull,default:current_timestamp"`
}

type bkLedgerEntry struct {
	bun.BaseModel `bun:"table:ledger_entries"`
	ID            int64          `bun:"id,pk,autoincrement"`
	TransactionID int64          `bun:"transaction_id,notnull"`
	AccountID     int64          `bun:"account_id,notnull"`
	Direction     string         `bun:"direction,notnull,type:varchar(1)"`
	Amount        int64          `bun:"amount,notnull"`
	PostedAt      time.Time      `bun:"posted_at,notnull,default:current_timestamp"`
	Transaction   *bkTransaction `bun:"rel:belongs-to,join:transaction_id=id"`
	Account       *bkAccount     `bun:"rel:belongs-to,join:account_id=id"`
}

type bkTransfer struct {
	bun.BaseModel `bun:"table:transfers"`
	ID            int64          `bun:"id,pk,autoincrement"`
	TransactionID int64          `bun:"transaction_id,notnull,unique"`
	FromAccountID int64          `bun:"from_account_id,notnull"`
	ToAccountID   int64          `bun:"to_account_id,notnull"`
	Amount        int64          `bun:"amount,notnull"`
	Transaction   *bkTransaction `bun:"rel:belongs-to,join:transaction_id=id"`
	From          *bkAccount     `bun:"rel:belongs-to,join:from_account_id=id"`
	To            *bkAccount     `bun:"rel:belongs-to,join:to_account_id=id"`
}

type bkHold struct {
	bun.BaseModel `bun:"table:holds"`
	ID            int64      `bun:"id,pk,autoincrement"`
	AccountID     int64      `bun:"account_id,notnull"`
	Amount        int64      `bun:"amount,notnull"`
	Reason        string     `bun:"reason,notnull,type:varchar(32)"`
	ReleasedAt    *time.Time `bun:"released_at"`
	Account       *bkAccount `bun:"rel:belongs-to,join:account_id=id"`
}

type bkLoan struct {
	bun.BaseModel `bun:"table:loans"`
	ID            int64       `bun:"id,pk,autoincrement"`
	CustomerID    int64       `bun:"customer_id,notnull"`
	AccountID     int64       `bun:"account_id,notnull"`
	Principal     int64       `bun:"principal,notnull"`
	RateBps       int32       `bun:"rate_bps,notnull"`
	TermMonths    int32       `bun:"term_months,notnull"`
	Status        string      `bun:"status,notnull,type:varchar(12)"`
	Customer      *bkCustomer `bun:"rel:belongs-to,join:customer_id=id"`
	Account       *bkAccount  `bun:"rel:belongs-to,join:account_id=id"`
}

type bkInstallment struct {
	bun.BaseModel `bun:"table:loan_installments"`
	ID            int64      `bun:"id,pk,autoincrement"`
	LoanID        int64      `bun:"loan_id,notnull"`
	DueDate       time.Time  `bun:"due_date,notnull"`
	PrincipalDue  int64      `bun:"principal_due,notnull"`
	InterestDue   int64      `bun:"interest_due,notnull"`
	PaidAt        *time.Time `bun:"paid_at"`
	Loan          *bkLoan    `bun:"rel:belongs-to,join:loan_id=id"`
}

type bkFXRate struct {
	bun.BaseModel `bun:"table:fx_rates"`
	ID            int64     `bun:"id,pk,autoincrement"`
	Base          string    `bun:"base,notnull,type:varchar(3)"`
	Quote         string    `bun:"quote,notnull,type:varchar(3)"`
	RatePPM       int64     `bun:"rate_ppm,notnull"`
	AsOf          time.Time `bun:"as_of,notnull"`
}

type bkPayee struct {
	bun.BaseModel `bun:"table:payees"`
	ID            int64       `bun:"id,pk,autoincrement"`
	CustomerID    int64       `bun:"customer_id,notnull"`
	AccountNumber string      `bun:"account_number,notnull,type:varchar(34)"`
	Name          string      `bun:"name,notnull"`
	Customer      *bkCustomer `bun:"rel:belongs-to,join:customer_id=id"`
}

type bkStandingOrder struct {
	bun.BaseModel `bun:"table:standing_orders"`
	ID            int64      `bun:"id,pk,autoincrement"`
	AccountID     int64      `bun:"account_id,notnull"`
	PayeeID       int64      `bun:"payee_id,notnull"`
	Amount        int64      `bun:"amount,notnull"`
	Frequency     string     `bun:"frequency,notnull,type:varchar(12)"`
	NextRun       time.Time  `bun:"next_run,notnull"`
	Account       *bkAccount `bun:"rel:belongs-to,join:account_id=id"`
	Payee         *bkPayee   `bun:"rel:belongs-to,join:payee_id=id"`
}

type bkAuditEvent struct {
	bun.BaseModel `bun:"table:audit_events"`
	ID            int64     `bun:"id,pk,autoincrement"`
	Actor         string    `bun:"actor,notnull,type:varchar(64)"`
	Action        string    `bun:"action,notnull,type:varchar(32)"`
	Entity        string    `bun:"entity,notnull,type:varchar(32)"`
	EntityID      int64     `bun:"entity_id,notnull"`
	Payload       string    `bun:"payload,type:jsonb"`
	CreatedAt     time.Time `bun:"created_at,notnull,default:current_timestamp"`
}

// bankModels is the released schema: 16 tables.
func bankModels() []any {
	return []any{
		(*bkAuditEvent)(nil), (*bkStandingOrder)(nil), (*bkPayee)(nil), (*bkFXRate)(nil),
		(*bkInstallment)(nil), (*bkLoan)(nil), (*bkHold)(nil), (*bkTransfer)(nil),
		(*bkLedgerEntry)(nil), (*bkTransaction)(nil), (*bkCard)(nil), (*bkAccount)(nil),
		(*bkProduct)(nil), (*bkAddress)(nil), (*bkCustomer)(nil), (*bkBranch)(nil),
	}
}

// bankModelsV2 is the next release: accounts gain two columns and a new table appears.
func bankModelsV2() []any {
	models := bankModels()
	for i, m := range models {
		if _, ok := m.(*bkAccount); ok {
			models[i] = (*bkAccountV2)(nil)
		}
	}
	return append(models, (*bkRiskProfile)(nil))
}

var bankTables = []string{
	"branches", "customers", "customer_addresses", "account_products", "accounts", "cards",
	"transactions", "ledger_entries", "transfers", "holds", "loans", "loan_installments",
	"fx_rates", "payees", "standing_orders", "audit_events",
}

const bankHardeningUp = `
ALTER TABLE accounts ADD CONSTRAINT accounts_balance_nonneg CHECK (balance >= 0);
ALTER TABLE accounts ADD CONSTRAINT accounts_currency_len CHECK (char_length(currency) = 3);
ALTER TABLE accounts ADD CONSTRAINT accounts_status_valid CHECK (status IN ('active', 'frozen', 'closed'));
ALTER TABLE ledger_entries ADD CONSTRAINT ledger_amount_pos CHECK (amount > 0);
ALTER TABLE ledger_entries ADD CONSTRAINT ledger_direction_valid CHECK (direction IN ('D', 'C'));
ALTER TABLE transfers ADD CONSTRAINT transfers_distinct_accounts CHECK (from_account_id <> to_account_id);
ALTER TABLE transfers ADD CONSTRAINT transfers_amount_pos CHECK (amount > 0);
ALTER TABLE holds ADD CONSTRAINT holds_amount_pos CHECK (amount > 0);
CREATE INDEX ledger_entries_account_idx ON ledger_entries (account_id, posted_at);
CREATE UNIQUE INDEX cards_one_active_per_account ON cards (account_id) WHERE status = 'active';

-- +goose StatementBegin
CREATE FUNCTION ledger_immutable() RETURNS trigger AS $$
BEGIN
  RAISE EXCEPTION 'ledger_entries are immutable';
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd
CREATE TRIGGER ledger_no_update BEFORE UPDATE OR DELETE ON ledger_entries
  FOR EACH ROW EXECUTE FUNCTION ledger_immutable();

-- +goose StatementBegin
CREATE FUNCTION ledger_balanced() RETURNS trigger AS $$
DECLARE d bigint; c bigint;
BEGIN
  SELECT COALESCE(SUM(amount) FILTER (WHERE direction = 'D'), 0),
         COALESCE(SUM(amount) FILTER (WHERE direction = 'C'), 0)
    INTO d, c FROM ledger_entries WHERE transaction_id = NEW.transaction_id;
  IF d <> c THEN
    RAISE EXCEPTION 'unbalanced transaction %: debits % credits %', NEW.transaction_id, d, c;
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd
CREATE CONSTRAINT TRIGGER ledger_balanced_trg AFTER INSERT ON ledger_entries
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION ledger_balanced();
`

const bankHardeningDown = `
DROP TRIGGER ledger_balanced_trg ON ledger_entries;
DROP FUNCTION ledger_balanced();
DROP TRIGGER ledger_no_update ON ledger_entries;
DROP FUNCTION ledger_immutable();
DROP INDEX cards_one_active_per_account;
DROP INDEX ledger_entries_account_idx;
ALTER TABLE holds DROP CONSTRAINT holds_amount_pos;
ALTER TABLE transfers DROP CONSTRAINT transfers_amount_pos;
ALTER TABLE transfers DROP CONSTRAINT transfers_distinct_accounts;
ALTER TABLE ledger_entries DROP CONSTRAINT ledger_direction_valid;
ALTER TABLE ledger_entries DROP CONSTRAINT ledger_amount_pos;
ALTER TABLE accounts DROP CONSTRAINT accounts_status_valid;
ALTER TABLE accounts DROP CONSTRAINT accounts_currency_len;
ALTER TABLE accounts DROP CONSTRAINT accounts_balance_nonneg;
`

// ---- deployment helpers -----------------------------------------------------

// settleModels runs Diff + Migrate until the database matches models (bun adds NOT NULL columns in
// two phases) and returns each generated migration.
func settleModels(t *testing.T, svc sqlsvc.SQLServices, name string, models []any) []string {
	t.Helper()
	var out []string
	for round := 1; round <= 4; round++ {
		path, err := svc.Diff(bg, fmt.Sprintf("%s r%d", name, round), models...)
		if err != nil {
			t.Fatalf("Diff(%s) round %d: %v", name, round, err)
		}
		if path == "" {
			if len(out) == 0 {
				t.Fatalf("Diff(%s) found nothing to do", name)
			}
			return out
		}
		out = append(out, readFile(t, path))
		if err := svc.Migrate(bg); err != nil {
			t.Fatalf("Migrate after Diff(%s) round %d: %v\n%s", name, round, err, out[len(out)-1])
		}
	}
	t.Fatalf("Diff(%s) did not converge", name)
	return nil
}

// bootstrapBank deploys the released schema to e: generated tables, then the hardening migration.
func bootstrapBank(t *testing.T, e *sqlEnv) sqlsvc.SQLServices {
	t.Helper()
	svc := e.running()
	settleModels(t, svc, "bank baseline", bankModels())
	e.writeNext("bank_hardening", bankHardeningUp, bankHardeningDown)
	if err := svc.Migrate(bg); err != nil {
		t.Fatalf("applying the hardening migration: %v", err)
	}
	return svc
}

// seedBank creates one branch, one product and n customers with one account each holding balance cents.
func seedBank(t *testing.T, e *sqlEnv, n int, balance int64) []int64 {
	t.Helper()
	e.exec(`INSERT INTO branches (code, name, country) VALUES ('HQ', 'Head Office', 'US') ON CONFLICT DO NOTHING`)
	e.exec(`INSERT INTO account_products (code, name, kind) VALUES ('CHK', 'Checking', 'checking') ON CONFLICT DO NOTHING`)
	base := e.scanInt(`SELECT count(*) FROM customers`)
	ids := make([]int64, 0, n)
	for i := range n {
		seq := base + int64(i)
		e.exec(`INSERT INTO customers (branch_id, legal_name, tax_id, kyc_status)
			VALUES ((SELECT id FROM branches WHERE code = 'HQ'), $1, $2, 'verified')`,
			fmt.Sprintf("Customer %d", seq), fmt.Sprintf("TAX-%05d", seq))
		ids = append(ids, e.scanInt(`INSERT INTO accounts (customer_id, product_id, number, currency, status, balance)
			VALUES ((SELECT max(id) FROM customers), (SELECT id FROM account_products WHERE code = 'CHK'), $1, 'USD', 'active', $2)
			RETURNING id`, fmt.Sprintf("ACC-%05d", seq), balance))
	}
	return ids
}

var errInsufficientFunds = errors.New("insufficient funds")

func pgCode(err error) string {
	if pg, ok := errors.AsType[*pgconn.PgError](err); ok {
		return pg.Code
	}
	return ""
}

// bankTransfer moves amount from one account to another as a single ACID transaction: an idempotent
// transaction row, row locks taken in id order, the balance update, and a balanced pair of ledger entries.
func bankTransfer(ctx context.Context, db *sql.DB, key string, from, to, amount int64) error {
	for attempt := range 20 {
		err := bankTransferOnce(ctx, db, key, from, to, amount)
		switch pgCode(err) {
		case "":
			return err
		case "40001", "40P01": // serialization failure, deadlock: safe to retry
			time.Sleep(time.Duration(attempt+1) * 2 * time.Millisecond)
			continue
		case "23514": // check_violation: the balance would go negative
			return errInsufficientFunds
		default:
			return err
		}
	}
	return errors.New("transfer: too many retries")
}

func bankTransferOnce(ctx context.Context, db *sql.DB, key string, from, to, amount int64) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var txID int64
	if err := tx.QueryRowContext(ctx,
		`INSERT INTO transactions (idempotency_key, kind, status) VALUES ($1, 'transfer', 'posted') RETURNING id`, key,
	).Scan(&txID); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT id FROM accounts WHERE id IN ($1, $2) ORDER BY id FOR UPDATE`, from, to)
	if err != nil {
		return err
	}
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_ = rows.Close()

	steps := []struct {
		query string
		args  []any
	}{
		{`UPDATE accounts SET balance = balance - $1, version = version + 1 WHERE id = $2`, []any{amount, from}},
		{`UPDATE accounts SET balance = balance + $1, version = version + 1 WHERE id = $2`, []any{amount, to}},
		{`INSERT INTO ledger_entries (transaction_id, account_id, direction, amount) VALUES ($1, $2, 'D', $3)`, []any{txID, from, amount}},
		{`INSERT INTO ledger_entries (transaction_id, account_id, direction, amount) VALUES ($1, $2, 'C', $3)`, []any{txID, to, amount}},
		{`INSERT INTO transfers (transaction_id, from_account_id, to_account_id, amount) VALUES ($1, $2, $3, $4)`, []any{txID, from, to, amount}},
	}
	for _, s := range steps {
		if _, err := tx.ExecContext(ctx, s.query, s.args...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// requireBankInvariants checks what ACID must guarantee, whatever migrations ran: money is conserved,
// nothing is negative, every transaction balances, and each balance equals its opening balance plus its ledger.
func requireBankInvariants(t *testing.T, e *sqlEnv, accounts int, opening int64) {
	t.Helper()
	if got, want := e.scanInt(`SELECT COALESCE(SUM(balance), 0) FROM accounts`), int64(accounts)*opening; got != want {
		t.Fatalf("money was created or destroyed: total balance %d, want %d", got, want)
	}
	if n := e.scanInt(`SELECT count(*) FROM accounts WHERE balance < 0`); n != 0 {
		t.Fatalf("%d accounts have a negative balance", n)
	}
	if n := e.scanInt(`SELECT count(*) FROM (SELECT transaction_id FROM ledger_entries GROUP BY transaction_id
		HAVING SUM(CASE direction WHEN 'D' THEN amount ELSE -amount END) <> 0) x`); n != 0 {
		t.Fatalf("%d transactions are unbalanced", n)
	}
	if n := e.scanInt(`SELECT count(*) FROM transactions t WHERE (SELECT count(*) FROM ledger_entries l WHERE l.transaction_id = t.id) <> 2`); n != 0 {
		t.Fatalf("%d transactions do not have exactly two ledger entries", n)
	}
	if n := e.scanInt(`SELECT count(*) FROM accounts a WHERE a.balance <> $1 + COALESCE(
		(SELECT SUM(CASE direction WHEN 'C' THEN amount ELSE -amount END) FROM ledger_entries WHERE account_id = a.id), 0)`, opening); n != 0 {
		t.Fatalf("%d accounts do not match their ledger", n)
	}
}

// accountsChecksum fingerprints id and balance, which no schema release changes.
func accountsChecksum(e *sqlEnv) string {
	e.t.Helper()
	var s string
	if err := e.db.QueryRow(`SELECT COALESCE(md5(string_agg(id::text || ':' || balance::text, ',' ORDER BY id)), '') FROM accounts`).Scan(&s); err != nil {
		e.t.Fatalf("checksum: %v", err)
	}
	return s
}

func (e *sqlEnv) countTables(names []string) int {
	n := 0
	for _, name := range names {
		if e.hasTable(name) {
			n++
		}
	}
	return n
}

func (e *sqlEnv) triggerCount() int64 {
	return e.scanInt(`SELECT count(*) FROM pg_trigger WHERE tgname IN ('ledger_no_update', 'ledger_balanced_trg')`)
}

func (e *sqlEnv) gooseRows() int64 {
	return e.scanInt(`SELECT count(*) FROM goose_db_version WHERE version_id > 0`)
}

// versionOutput returns the version printed by Version.
func (e *sqlEnv) versionOutput(svc sqlsvc.SQLServices) int64 {
	e.t.Helper()
	e.resetStatusOutput()
	if err := svc.Version(bg); err != nil {
		e.t.Fatalf("Version: %v", err)
	}
	var v int64
	if _, err := fmt.Sscanf(strings.TrimSpace(e.statusOutput()), "database version: %d", &v); err != nil {
		e.t.Fatalf("cannot parse Version output %q: %v", e.statusOutput(), err)
	}
	return v
}
