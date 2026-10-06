package regressions

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	sqlsvc "github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"
	"github.com/uptrace/bun"
)

// The banking evolution: twenty releases of a core-banking schema, the foundation for how production
// changes are made. It is built from the breaking changes that actually hurt: money stored as numeric
// moved to integer minor units with a dual-write window, a table split in two, a table renamed behind a
// compatibility view, a text column converted to a code, a column replaced by a join table, soft delete
// with partial uniqueness, an immutable double-entry ledger, history archived with carry-forward,
// duplicate customers merged before a unique constraint, NOT VALID constraints validated later, and
// views that block column changes. Diff generates what it can; everything else is a hand-written
// migration with a real Down. Every release runs against rows that must survive, and the ledger must
// reconcile to the balances after each one.

// ---- models -----------------------------------------------------------------

type cbCustomerV1 struct {
	bun.BaseModel `bun:"table:customers"`
	ID            int64  `bun:"id,pk,autoincrement"`
	LegalName     string `bun:"legal_name,notnull"`
	TaxID         string `bun:"tax_id,notnull"`
	Email         string `bun:"email"`
	Phone         string `bun:"phone"`
}

type cbCustomerV2 struct { // contact details moved to their own table
	bun.BaseModel `bun:"table:customers"`
	ID            int64  `bun:"id,pk,autoincrement"`
	LegalName     string `bun:"legal_name,notnull"`
	TaxID         string `bun:"tax_id,notnull"`
}

type cbCustomerV3 struct { // tax id unique once duplicates are merged
	bun.BaseModel `bun:"table:customers"`
	ID            int64  `bun:"id,pk,autoincrement"`
	LegalName     string `bun:"legal_name,notnull"`
	TaxID         string `bun:"tax_id,notnull,unique"`
}

type cbCustomerV4 struct { // legal_name renamed
	bun.BaseModel `bun:"table:customers"`
	ID            int64  `bun:"id,pk,autoincrement"`
	DisplayName   string `bun:"display_name,notnull"`
	TaxID         string `bun:"tax_id,notnull,unique"`
}

type cbCustomerHead = cbCustomerV4

type cbAccountV1 struct { // money as numeric, status as text
	bun.BaseModel `bun:"table:accounts"`
	ID            int64           `bun:"id,pk,autoincrement"`
	CustomerID    int64           `bun:"customer_id,notnull"`
	Customer      *cbCustomerHead `bun:"rel:belongs-to,join:customer_id=id"`
	Number        string          `bun:"number,notnull,unique"`
	Currency      string          `bun:"currency,notnull"`
	Status        string          `bun:"status,notnull,default:'active'"`
	Balance       float64         `bun:"balance,notnull,type:numeric,default:0"`
}

type cbAccountV2 struct { // expand: balance_minor next to the old balance
	bun.BaseModel `bun:"table:accounts"`
	ID            int64           `bun:"id,pk,autoincrement"`
	CustomerID    int64           `bun:"customer_id,notnull"`
	Customer      *cbCustomerHead `bun:"rel:belongs-to,join:customer_id=id"`
	Number        string          `bun:"number,notnull,unique"`
	Currency      string          `bun:"currency,notnull"`
	Status        string          `bun:"status,notnull,default:'active'"`
	Balance       float64         `bun:"balance,notnull,type:numeric,default:0"`
	BalanceMinor  int64           `bun:"balance_minor"`
}

type cbAccountV3 struct { // balance_minor is now required
	bun.BaseModel `bun:"table:accounts"`
	ID            int64           `bun:"id,pk,autoincrement"`
	CustomerID    int64           `bun:"customer_id,notnull"`
	Customer      *cbCustomerHead `bun:"rel:belongs-to,join:customer_id=id"`
	Number        string          `bun:"number,notnull,unique"`
	Currency      string          `bun:"currency,notnull"`
	Status        string          `bun:"status,notnull,default:'active'"`
	Balance       float64         `bun:"balance,notnull,type:numeric,default:0"`
	BalanceMinor  int64           `bun:"balance_minor,notnull"`
}

type cbAccountV4 struct { // only used to show Diff refusing to drop the old balance
	bun.BaseModel `bun:"table:accounts"`
	ID            int64           `bun:"id,pk,autoincrement"`
	CustomerID    int64           `bun:"customer_id,notnull"`
	Customer      *cbCustomerHead `bun:"rel:belongs-to,join:customer_id=id"`
	Number        string          `bun:"number,notnull,unique"`
	Currency      string          `bun:"currency,notnull"`
	Status        string          `bun:"status,notnull,default:'active'"`
	BalanceMinor  int64           `bun:"balance_minor,notnull"`
}

type cbAccountV5 struct { // contract: balance_minor renamed to balance
	bun.BaseModel `bun:"table:accounts"`
	ID            int64           `bun:"id,pk,autoincrement"`
	CustomerID    int64           `bun:"customer_id,notnull"`
	Customer      *cbCustomerHead `bun:"rel:belongs-to,join:customer_id=id"`
	Number        string          `bun:"number,notnull,unique"`
	Currency      string          `bun:"currency,notnull"`
	Status        string          `bun:"status,notnull,default:'active'"`
	Balance       int64           `bun:"balance,notnull"`
}

type cbAccountV6 struct { // status text becomes a smallint code
	bun.BaseModel `bun:"table:accounts"`
	ID            int64           `bun:"id,pk,autoincrement"`
	CustomerID    int64           `bun:"customer_id,notnull"`
	Customer      *cbCustomerHead `bun:"rel:belongs-to,join:customer_id=id"`
	Number        string          `bun:"number,notnull,unique"`
	Currency      string          `bun:"currency,notnull"`
	Status        int16           `bun:"status,notnull,default:1"`
	Balance       int64           `bun:"balance,notnull"`
}

type cbAccountV7 struct { // customer_id gone: holders live in account_holders
	bun.BaseModel `bun:"table:accounts"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Number        string `bun:"number,notnull,unique"`
	Currency      string `bun:"currency,notnull"`
	Status        int16  `bun:"status,notnull,default:1"`
	Balance       int64  `bun:"balance,notnull"`
}

type cbAccountV8 struct { // soft close
	bun.BaseModel `bun:"table:accounts"`
	ID            int64     `bun:"id,pk,autoincrement"`
	Number        string    `bun:"number,notnull,unique"`
	Currency      string    `bun:"currency,notnull"`
	Status        int16     `bun:"status,notnull,default:1"`
	Balance       int64     `bun:"balance,notnull"`
	ClosedAt      time.Time `bun:"closed_at"`
}

type cbAccountV9 struct { // number uniqueness now only among open accounts (a partial index, by hand)
	bun.BaseModel `bun:"table:accounts"`
	ID            int64     `bun:"id,pk,autoincrement"`
	Number        string    `bun:"number,notnull"`
	Currency      string    `bun:"currency,notnull"`
	Status        int16     `bun:"status,notnull,default:1"`
	Balance       int64     `bun:"balance,notnull"`
	ClosedAt      time.Time `bun:"closed_at"`
}

type cbAccountV10 struct { // ISO currency codes
	bun.BaseModel `bun:"table:accounts"`
	ID            int64     `bun:"id,pk,autoincrement"`
	Number        string    `bun:"number,notnull"`
	Currency      string    `bun:"currency,notnull,type:varchar(3)"`
	Status        int16     `bun:"status,notnull,default:1"`
	Balance       int64     `bun:"balance,notnull"`
	ClosedAt      time.Time `bun:"closed_at"`
}

type cbAccountV11 struct { // IBAN-length numbers, blocked by a dependent view until it is rebuilt
	bun.BaseModel `bun:"table:accounts"`
	ID            int64     `bun:"id,pk,autoincrement"`
	Number        string    `bun:"number,notnull,type:varchar(34)"`
	Currency      string    `bun:"currency,notnull,type:varchar(3)"`
	Status        int16     `bun:"status,notnull,default:1"`
	Balance       int64     `bun:"balance,notnull"`
	ClosedAt      time.Time `bun:"closed_at"`
}

type cbAccountHead = cbAccountV11

type cbEntryV1 struct { // table ledger_entries, numeric amounts
	bun.BaseModel `bun:"table:ledger_entries"`
	ID            int64          `bun:"id,pk,autoincrement"`
	AccountID     int64          `bun:"account_id,notnull"`
	Account       *cbAccountHead `bun:"rel:belongs-to,join:account_id=id"`
	Direction     string         `bun:"direction,notnull"`
	Amount        float64        `bun:"amount,notnull,type:numeric"`
	PostedAt      time.Time      `bun:"posted_at,notnull,default:current_timestamp"`
}

type cbEntryV2 struct {
	bun.BaseModel `bun:"table:ledger_entries"`
	ID            int64          `bun:"id,pk,autoincrement"`
	AccountID     int64          `bun:"account_id,notnull"`
	Account       *cbAccountHead `bun:"rel:belongs-to,join:account_id=id"`
	Direction     string         `bun:"direction,notnull"`
	Amount        float64        `bun:"amount,notnull,type:numeric"`
	AmountMinor   int64          `bun:"amount_minor"`
	PostedAt      time.Time      `bun:"posted_at,notnull,default:current_timestamp"`
}

type cbEntryV3 struct {
	bun.BaseModel `bun:"table:ledger_entries"`
	ID            int64          `bun:"id,pk,autoincrement"`
	AccountID     int64          `bun:"account_id,notnull"`
	Account       *cbAccountHead `bun:"rel:belongs-to,join:account_id=id"`
	Direction     string         `bun:"direction,notnull"`
	Amount        float64        `bun:"amount,notnull,type:numeric"`
	AmountMinor   int64          `bun:"amount_minor,notnull"`
	PostedAt      time.Time      `bun:"posted_at,notnull,default:current_timestamp"`
}

type cbEntryV4 struct { // only used to show Diff refusing to drop the old amount
	bun.BaseModel `bun:"table:ledger_entries"`
	ID            int64          `bun:"id,pk,autoincrement"`
	AccountID     int64          `bun:"account_id,notnull"`
	Account       *cbAccountHead `bun:"rel:belongs-to,join:account_id=id"`
	Direction     string         `bun:"direction,notnull"`
	AmountMinor   int64          `bun:"amount_minor,notnull"`
	PostedAt      time.Time      `bun:"posted_at,notnull,default:current_timestamp"`
}

type cbEntryV5 struct { // integer amounts under the original name
	bun.BaseModel `bun:"table:ledger_entries"`
	ID            int64          `bun:"id,pk,autoincrement"`
	AccountID     int64          `bun:"account_id,notnull"`
	Account       *cbAccountHead `bun:"rel:belongs-to,join:account_id=id"`
	Direction     string         `bun:"direction,notnull"`
	Amount        int64          `bun:"amount,notnull"`
	PostedAt      time.Time      `bun:"posted_at,notnull,default:current_timestamp"`
}

type cbEntryV6 struct { // the table is renamed to journal_entries
	bun.BaseModel `bun:"table:journal_entries"`
	ID            int64          `bun:"id,pk,autoincrement"`
	AccountID     int64          `bun:"account_id,notnull"`
	Account       *cbAccountHead `bun:"rel:belongs-to,join:account_id=id"`
	Direction     string         `bun:"direction,notnull"`
	Amount        int64          `bun:"amount,notnull"`
	PostedAt      time.Time      `bun:"posted_at,notnull,default:current_timestamp"`
}

type cbEntryV7 struct { // entries can belong to a transfer
	bun.BaseModel `bun:"table:journal_entries"`
	ID            int64          `bun:"id,pk,autoincrement"`
	AccountID     int64          `bun:"account_id,notnull"`
	Account       *cbAccountHead `bun:"rel:belongs-to,join:account_id=id"`
	Direction     string         `bun:"direction,notnull"`
	Amount        int64          `bun:"amount,notnull"`
	PostedAt      time.Time      `bun:"posted_at,notnull,default:current_timestamp"`
	TransferID    int64          `bun:"transfer_id"`
	Transfer      *cbTransfer    `bun:"rel:belongs-to,join:transfer_id=id"`
}

type cbContact struct {
	bun.BaseModel `bun:"table:customer_contacts"`
	ID            int64           `bun:"id,pk,autoincrement"`
	CustomerID    int64           `bun:"customer_id,notnull,unique:customer_kind"`
	Customer      *cbCustomerHead `bun:"rel:belongs-to,join:customer_id=id"`
	Kind          string          `bun:"kind,notnull,unique:customer_kind"`
	Value         string          `bun:"value,notnull"`
}

type cbHolder struct { // composite primary key
	bun.BaseModel `bun:"table:account_holders"`
	AccountID     int64           `bun:"account_id,pk"`
	Account       *cbAccountHead  `bun:"rel:belongs-to,join:account_id=id"`
	CustomerID    int64           `bun:"customer_id,pk"`
	Customer      *cbCustomerHead `bun:"rel:belongs-to,join:customer_id=id"`
	Role          string          `bun:"role,notnull,default:'owner'"`
}

type cbTransfer struct {
	bun.BaseModel  `bun:"table:transfers"`
	ID             int64          `bun:"id,pk,autoincrement"`
	IdempotencyKey string         `bun:"idempotency_key,notnull,unique"`
	FromAccountID  int64          `bun:"from_account_id,notnull"`
	From           *cbAccountHead `bun:"rel:belongs-to,join:from_account_id=id"`
	ToAccountID    int64          `bun:"to_account_id,notnull"`
	To             *cbAccountHead `bun:"rel:belongs-to,join:to_account_id=id"`
	Amount         int64          `bun:"amount,notnull"`
	State          int16          `bun:"state,notnull,default:0"`
	CreatedAt      time.Time      `bun:"created_at,notnull,default:current_timestamp"`
}

type cbArchiveV1 struct {
	bun.BaseModel `bun:"table:journal_archive"`
	ID            int64     `bun:"id,pk"`
	AccountID     int64     `bun:"account_id,notnull"`
	Direction     string    `bun:"direction,notnull"`
	Amount        int64     `bun:"amount,notnull"`
	PostedAt      time.Time `bun:"posted_at,notnull"`
	ArchivedAt    time.Time `bun:"archived_at,notnull,default:current_timestamp"`
}

type cbArchiveV2 struct { // the account foreign key, added NOT VALID by hand
	bun.BaseModel `bun:"table:journal_archive"`
	ID            int64          `bun:"id,pk"`
	AccountID     int64          `bun:"account_id,notnull"`
	Account       *cbAccountHead `bun:"rel:belongs-to,join:account_id=id"`
	Direction     string         `bun:"direction,notnull"`
	Amount        int64          `bun:"amount,notnull"`
	PostedAt      time.Time      `bun:"posted_at,notnull"`
	ArchivedAt    time.Time      `bun:"archived_at,notnull,default:current_timestamp"`
}

type cbOutbox struct {
	bun.BaseModel `bun:"table:outbox_events"`
	ID            int64     `bun:"id,pk,autoincrement"`
	Topic         string    `bun:"topic,notnull"`
	Payload       string    `bun:"payload,type:jsonb"`
	CreatedAt     time.Time `bun:"created_at,notnull,default:current_timestamp"`
}

// ---- hand-written SQL reused by an Up and a later Down -----------------------

const cbSyncMinorUnits = `-- +goose StatementBegin
CREATE FUNCTION sync_minor_units() RETURNS trigger AS $$
BEGIN
	IF TG_TABLE_NAME = 'accounts' THEN
		IF NEW.balance_minor IS NULL OR (TG_OP = 'UPDATE' AND NEW.balance IS DISTINCT FROM OLD.balance) THEN
			NEW.balance_minor := round(NEW.balance * 100)::bigint;
		END IF;
	ELSE
		IF NEW.amount_minor IS NULL OR (TG_OP = 'UPDATE' AND NEW.amount IS DISTINCT FROM OLD.amount) THEN
			NEW.amount_minor := round(NEW.amount * 100)::bigint;
		END IF;
	END IF;
	RETURN NEW;
END
$$ LANGUAGE plpgsql;
-- +goose StatementEnd
CREATE TRIGGER accounts_sync_minor BEFORE INSERT OR UPDATE ON accounts FOR EACH ROW EXECUTE FUNCTION sync_minor_units();
CREATE TRIGGER entries_sync_minor BEFORE INSERT OR UPDATE ON ledger_entries FOR EACH ROW EXECUTE FUNCTION sync_minor_units();`

const cbAccountOverview = `CREATE VIEW account_overview AS
SELECT id, number, currency, balance, status FROM accounts WHERE closed_at IS NULL;`

// ---- helpers ----------------------------------------------------------------

// requireLedgerBalanced asserts every account's balance equals its credits minus its debits.
func requireLedgerBalanced(t *testing.T, e *sqlEnv, entries, amount, balance string) {
	t.Helper()
	q := fmt.Sprintf(`SELECT count(*) FROM accounts a WHERE a.%[3]s <> COALESCE(
		(SELECT SUM(CASE direction WHEN 'C' THEN %[2]s ELSE -%[2]s END) FROM %[1]s WHERE account_id = a.id), 0)`, entries, amount, balance)
	if n := e.scanInt(q); n != 0 {
		t.Fatalf("%d accounts no longer reconcile with %s.%s", n, entries, amount)
	}
}

func requireTotals(t *testing.T, e *sqlEnv, column string, usd, eur int64) {
	t.Helper()
	for currency, want := range map[string]int64{"USD": usd, "EUR": eur} {
		got := e.scanInt(fmt.Sprintf(`SELECT COALESCE(sum(%s), 0) FROM accounts WHERE currency = $1`, column), currency)
		if got != want {
			t.Fatalf("total %s %s = %d; want %d", currency, column, got, want)
		}
	}
}

// cbTransferTx posts a transfer and the account updates in one transaction. An unbalanced transfer
// posts only the debit, so the ledger's deferred balance check must reject it at commit.
func cbTransferTx(e *sqlEnv, key string, from, to, amount int64, balanced bool) error {
	tx, err := e.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var id int64
	if err := tx.QueryRow(`INSERT INTO transfers (idempotency_key, from_account_id, to_account_id, amount) VALUES ($1, $2, $3, $4) RETURNING id`,
		key, from, to, amount).Scan(&id); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO journal_entries (account_id, amount, direction, transfer_id) VALUES ($1, $2, 'D', $3)`, from, amount, id); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE accounts SET balance = balance - $2 WHERE id = $1`, from, amount); err != nil {
		return err
	}
	if balanced {
		if _, err := tx.Exec(`INSERT INTO journal_entries (account_id, amount, direction, transfer_id) VALUES ($1, $2, 'C', $3)`, to, amount, id); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE accounts SET balance = balance + $2 WHERE id = $1`, to, amount); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// buildBanking walks a development database through twenty releases, with data in place, and returns the
// history plus the file count and version right after release 3, the point a lagging production stays at.
func buildBanking(t *testing.T) (dev *evolution, lagFiles int, lagVersion int64) {
	t.Helper()
	e := newSQLEnv(t)
	dev = &evolution{t: t, e: e, snaps: map[int64]string{}}

	var cust, acct, jrnl, contact, holder, transfer, archive, outbox any
	models := func() []any {
		var m []any
		for _, model := range []any{cust, acct, jrnl, contact, holder, transfer, archive, outbox} {
			if model != nil {
				m = append(m, model)
			}
		}
		return m
	}
	generate := func(name string) { t.Helper(); dev.generate(name, models()...) }
	converged := func() { t.Helper(); requireConverged(t, e, models()...) }
	acctID := func(number string) int64 { return e.scanInt(`SELECT id FROM accounts WHERE number = $1`, number) }
	custID := func(name string) int64 {
		return e.scanInt(`SELECT id FROM customers WHERE legal_name = $1 OR display_name = $1 LIMIT 1`, name)
	}
	_ = custID

	// Release 1: customers, with a duplicate (same tax id) that a later release must merge.
	cust = (*cbCustomerV1)(nil)
	generate("customers")
	e.exec(`INSERT INTO customers (legal_name, tax_id, email, phone) VALUES
		('Ada Lovelace', 'T-100', 'ada@bank.io', '555-0001'),
		('Grace Hopper', 'T-200', 'grace@bank.io', '555-0002'),
		('Alan Turing', 'T-300', 'alan@bank.io', NULL),
		('Edsger Dijkstra', 'T-400', 'edsger@bank.io', '555-0004'),
		('Barbara Liskov', 'T-500', 'barbara@bank.io', '555-0005'),
		('Barbara L.', 'T-500', 'b.liskov@bank.io', '555-0006')`)

	// Release 2: accounts, with money as numeric and status as text: the two decisions regretted later.
	acct = (*cbAccountV1)(nil)
	generate("accounts")
	e.exec(`INSERT INTO accounts (customer_id, number, currency, balance)
		SELECT c.id, v.number, v.currency, v.balance
		FROM (VALUES ('Ada Lovelace', 'AC-001', 'USD', 1000.50), ('Grace Hopper', 'AC-002', 'USD', 250.25),
		             ('Alan Turing', 'AC-003', 'EUR', 80.00), ('Edsger Dijkstra', 'AC-004', 'USD', 0.00),
		             ('Barbara Liskov', 'AC-005', 'USD', 12.34), ('Barbara L.', 'AC-006', 'USD', 7.66)) AS v(name, number, currency, balance)
		JOIN customers c ON c.legal_name = v.name`)

	// Release 3: the ledger, then the integrity Diff cannot express: indexes and CHECK constraints.
	jrnl = (*cbEntryV1)(nil)
	generate("ledger_entries")
	dev.hand("ledger_integrity",
		`CREATE INDEX ledger_account_posted_idx ON ledger_entries (account_id, posted_at DESC);
CREATE INDEX ledger_posted_brin ON ledger_entries USING brin (posted_at);
ALTER TABLE ledger_entries ADD CONSTRAINT ledger_direction_valid CHECK (direction IN ('C', 'D'));
ALTER TABLE ledger_entries ADD CONSTRAINT ledger_amount_positive CHECK (amount > 0);`,
		`ALTER TABLE ledger_entries DROP CONSTRAINT ledger_amount_positive;
ALTER TABLE ledger_entries DROP CONSTRAINT ledger_direction_valid;
DROP INDEX ledger_posted_brin;
DROP INDEX ledger_account_posted_idx;`)
	converged()
	e.exec(`INSERT INTO ledger_entries (account_id, direction, amount, posted_at)
		SELECT a.id, v.direction, v.amount, v.posted_at::timestamptz
		FROM (VALUES ('AC-001', 'C', 1100.50, '2024-01-15'), ('AC-001', 'D', 100.00, '2024-02-01'),
		             ('AC-002', 'C', 150.25, '2024-01-15'), ('AC-002', 'C', 100.00, '2024-02-01'),
		             ('AC-003', 'C', 80.00, '2024-01-20'), ('AC-005', 'C', 12.34, '2024-03-01'),
		             ('AC-006', 'C', 7.66, '2024-03-01')) AS v(number, direction, amount, posted_at)
		JOIN accounts a ON a.number = v.number`)
	mustFail(t, e, "direction CHECK", `INSERT INTO ledger_entries (account_id, direction, amount) VALUES ($1, 'X', 1)`, acctID("AC-001"))
	mustFail(t, e, "amount CHECK", `INSERT INTO ledger_entries (account_id, direction, amount) VALUES ($1, 'C', 0)`, acctID("AC-001"))
	requireLedgerBalanced(t, e, "ledger_entries", "amount", "balance")
	lagFiles, lagVersion = len(e.migrationFiles()), e.appliedVersion()

	// Release 4, expand: integer minor units next to the numeric columns, backfilled, and kept in sync by a
	// trigger so old application code that still writes numeric amounts keeps working during the rollout.
	acct, jrnl = (*cbAccountV2)(nil), (*cbEntryV2)(nil)
	generate("minor_units_expand")
	dev.hand("minor_units_backfill_and_dual_write",
		`UPDATE accounts SET balance_minor = round(balance * 100)::bigint WHERE balance_minor IS NULL;
UPDATE ledger_entries SET amount_minor = round(amount * 100)::bigint WHERE amount_minor IS NULL;
`+cbSyncMinorUnits,
		`DROP TRIGGER entries_sync_minor ON ledger_entries;
DROP TRIGGER accounts_sync_minor ON accounts;
DROP FUNCTION sync_minor_units();`)
	if n := e.scanInt(`SELECT count(*) FROM accounts WHERE balance_minor IS NULL`) + e.scanInt(`SELECT count(*) FROM ledger_entries WHERE amount_minor IS NULL`); n != 0 {
		t.Fatalf("%d rows were not backfilled", n)
	}
	// An old writer that knows nothing about minor units still produces a correct row.
	e.exec(`INSERT INTO accounts (customer_id, number, currency, balance) VALUES ((SELECT id FROM customers WHERE legal_name = 'Ada Lovelace'), 'AC-007', 'USD', 5.55)`)
	e.exec(`INSERT INTO ledger_entries (account_id, direction, amount, posted_at) VALUES ($1, 'C', 5.55, '2024-04-01')`, acctID("AC-007"))
	if got := e.scanInt(`SELECT balance_minor FROM accounts WHERE number = 'AC-007'`); got != 555 {
		t.Fatalf("dual write produced %d minor units for 5.55", got)
	}
	requireLedgerBalanced(t, e, "ledger_entries", "amount", "balance")
	requireLedgerBalanced(t, e, "ledger_entries", "amount_minor", "balance_minor")
	requireTotals(t, e, "balance_minor", 127630, 8000)

	// Release 5, contract: require the new columns, then drop the old ones by hand. Diff would write the drop,
	// but its Down brings the columns back empty and nullable, so a rollback would silently lose the money.
	acct, jrnl = (*cbAccountV3)(nil), (*cbEntryV3)(nil)
	generate("minor_units_required")
	path, err := e.diff("drop old money columns", cust, (*cbAccountV4)(nil), (*cbEntryV4)(nil))
	if err != nil || path == "" {
		t.Fatalf("Diff dropping the old money columns = %q, %v", path, err)
	}
	if down := gooseDown(readFile(t, path)); strings.Contains(down, "NOT NULL") || strings.Contains(down, "UPDATE") {
		t.Fatalf("Diff's Down now restores the dropped columns; the hand-written contraction may no longer be needed:\n%s", down)
	}
	if err := os.Remove(path); err != nil { // never applied: the real contraction below is written by hand
		t.Fatal(err)
	}
	dev.hand("minor_units_contract",
		`DROP TRIGGER entries_sync_minor ON ledger_entries;
DROP TRIGGER accounts_sync_minor ON accounts;
DROP FUNCTION sync_minor_units();
ALTER TABLE ledger_entries DROP CONSTRAINT ledger_amount_positive;
ALTER TABLE accounts DROP COLUMN balance;
ALTER TABLE ledger_entries DROP COLUMN amount;
ALTER TABLE accounts RENAME COLUMN balance_minor TO balance;
ALTER TABLE ledger_entries RENAME COLUMN amount_minor TO amount;
ALTER TABLE ledger_entries ADD CONSTRAINT ledger_amount_positive CHECK (amount > 0);`,
		`ALTER TABLE ledger_entries DROP CONSTRAINT ledger_amount_positive;
ALTER TABLE ledger_entries RENAME COLUMN amount TO amount_minor;
ALTER TABLE accounts RENAME COLUMN balance TO balance_minor;
ALTER TABLE accounts ADD COLUMN balance numeric NOT NULL DEFAULT 0;
UPDATE accounts SET balance = balance_minor / 100.0;
ALTER TABLE ledger_entries ADD COLUMN amount numeric;
UPDATE ledger_entries SET amount = amount_minor / 100.0;
ALTER TABLE ledger_entries ALTER COLUMN amount SET NOT NULL;
ALTER TABLE ledger_entries ADD CONSTRAINT ledger_amount_positive CHECK (amount > 0);
`+cbSyncMinorUnits)
	acct, jrnl = (*cbAccountV5)(nil), (*cbEntryV5)(nil)
	converged()
	requireLedgerBalanced(t, e, "ledger_entries", "amount", "balance")
	requireTotals(t, e, "balance", 127630, 8000)
	mustFail(t, e, "amount CHECK survived the column swap", `INSERT INTO ledger_entries (account_id, direction, amount) VALUES ($1, 'C', 0)`, acctID("AC-001"))

	// Release 6, split: contact details leave customers. The data is copied before the columns are dropped.
	contact = (*cbContact)(nil)
	generate("customer_contacts")
	dev.hand("copy_contacts",
		`INSERT INTO customer_contacts (customer_id, kind, value) SELECT id, 'email', email FROM customers WHERE email IS NOT NULL;
INSERT INTO customer_contacts (customer_id, kind, value) SELECT id, 'phone', phone FROM customers WHERE phone IS NOT NULL;`,
		`DELETE FROM customer_contacts;`)
	if got := e.scanInt(`SELECT count(*) FROM customer_contacts`); got != 11 {
		t.Fatalf("%d contacts were copied; want 6 emails and 5 phones", got)
	}
	cust = (*cbCustomerV2)(nil)
	generate("drop_contact_columns")
	if e.hasColumn("customers", "email") || e.hasColumn("customers", "phone") {
		t.Fatal("the contact columns are still on customers")
	}

	// Release 7, rename: the table becomes journal_entries; an updatable view keeps the old name working.
	dev.hand("rename_ledger_to_journal",
		`ALTER TABLE ledger_entries RENAME TO journal_entries;
ALTER SEQUENCE ledger_entries_id_seq RENAME TO journal_entries_id_seq;
ALTER TABLE journal_entries RENAME CONSTRAINT ledger_entries_pkey TO journal_entries_pkey;
ALTER TABLE journal_entries RENAME CONSTRAINT ledger_entries_account_id_fkey TO journal_entries_account_id_fkey;
CREATE VIEW ledger_entries AS SELECT id, account_id, direction, amount, posted_at FROM journal_entries;`,
		`DROP VIEW ledger_entries;
ALTER TABLE journal_entries RENAME CONSTRAINT journal_entries_account_id_fkey TO ledger_entries_account_id_fkey;
ALTER TABLE journal_entries RENAME CONSTRAINT journal_entries_pkey TO ledger_entries_pkey;
ALTER SEQUENCE journal_entries_id_seq RENAME TO ledger_entries_id_seq;
ALTER TABLE journal_entries RENAME TO ledger_entries;`)
	jrnl = (*cbEntryV6)(nil)
	converged()
	e.exec(`INSERT INTO ledger_entries (account_id, direction, amount) VALUES ($1, 'C', 1)`, acctID("AC-004")) // an old reader through the view
	if e.scanInt(`SELECT count(*) FROM journal_entries WHERE account_id = $1`, acctID("AC-004")) != 1 {
		t.Fatal("a row written through the compatibility view did not reach the table")
	}
	e.exec(`DELETE FROM ledger_entries WHERE account_id = $1`, acctID("AC-004"))

	// Release 8, type change: text status becomes a code. Diff alone cannot cast text to smallint.
	e.exec(`UPDATE accounts SET status = 'frozen' WHERE number = 'AC-004'`)
	if _, err := e.diff("status codes", cust, (*cbAccountV6)(nil), jrnl, contact); !errors.Is(err, sqlsvc.ErrInvalidMigration) {
		t.Fatalf("Diff converting text to smallint = %v; want ErrInvalidMigration", err)
	}
	dev.hand("account_status_codes",
		`ALTER TABLE accounts ALTER COLUMN status DROP DEFAULT;
ALTER TABLE accounts ALTER COLUMN status TYPE smallint USING CASE status WHEN 'active' THEN 1 WHEN 'frozen' THEN 2 WHEN 'closed' THEN 3 ELSE 0 END;
ALTER TABLE accounts ALTER COLUMN status SET DEFAULT 1;`,
		`ALTER TABLE accounts ALTER COLUMN status DROP DEFAULT;
ALTER TABLE accounts ALTER COLUMN status TYPE varchar USING CASE status WHEN 1 THEN 'active' WHEN 2 THEN 'frozen' WHEN 3 THEN 'closed' ELSE 'active' END;
ALTER TABLE accounts ALTER COLUMN status SET DEFAULT 'active';`)
	acct = (*cbAccountV6)(nil)
	converged()
	if e.scanInt(`SELECT status FROM accounts WHERE number = 'AC-004'`) != 2 || e.scanInt(`SELECT count(*) FROM accounts WHERE status = 1`) != 6 {
		t.Fatal("status codes were not mapped from the old text")
	}

	// Release 9, replace a column with a join table: joint accounts need many holders per account.
	holder = (*cbHolder)(nil)
	generate("account_holders")
	dev.hand("seed_account_holders",
		`INSERT INTO account_holders (account_id, customer_id, role) SELECT id, customer_id, 'owner' FROM accounts ON CONFLICT DO NOTHING;`,
		`DELETE FROM account_holders;`)
	e.exec(`INSERT INTO account_holders (account_id, customer_id, role)
		SELECT a.id, c.id, 'joint' FROM accounts a, customers c WHERE a.number = 'AC-001' AND c.legal_name = 'Grace Hopper'`)
	if got := e.scanInt(`SELECT count(*) FROM account_holders`); got != 8 {
		t.Fatalf("%d holders; want 7 owners and 1 joint", got)
	}

	// Release 10: drop accounts.customer_id, guarded so an account without an owner stops the migration.
	// The Down restores the column from the holders, so even a rollback keeps the data.
	e.writeNext("drop_account_customer_id",
		`-- +goose StatementBegin
DO $$
BEGIN
	IF EXISTS (SELECT 1 FROM accounts a WHERE NOT EXISTS (SELECT 1 FROM account_holders h WHERE h.account_id = a.id AND h.role = 'owner')) THEN
		RAISE EXCEPTION 'every account needs an owner before accounts.customer_id can go';
	END IF;
END
$$;
-- +goose StatementEnd
ALTER TABLE accounts DROP COLUMN customer_id;`,
		`ALTER TABLE accounts ADD COLUMN customer_id bigint;
UPDATE accounts SET customer_id = (SELECT h.customer_id FROM account_holders h WHERE h.account_id = accounts.id AND h.role = 'owner');
ALTER TABLE accounts ALTER COLUMN customer_id SET NOT NULL;
ALTER TABLE accounts ADD CONSTRAINT accounts_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES customers (id);`)
	e.exec(`INSERT INTO accounts (customer_id, number, currency, balance) VALUES ((SELECT min(id) FROM customers), 'AC-099', 'USD', 0)`) // no holder row
	if err := e.migrate(); err == nil {
		t.Fatal("the guard let an account without an owner through")
	}
	if !e.hasColumn("accounts", "customer_id") {
		t.Fatal("the guarded migration changed the schema before failing")
	}
	e.exec(`DELETE FROM accounts WHERE number = 'AC-099'`)
	dev.applyHand("drop_account_customer_id")
	acct = (*cbAccountV7)(nil)
	converged()

	// Release 11, soft close: a closed account's number can be reused, so uniqueness applies to open accounts only.
	acct = (*cbAccountV8)(nil)
	generate("account_closed_at")
	dev.hand("open_account_numbers_unique",
		`ALTER TABLE accounts DROP CONSTRAINT accounts_number_key;
CREATE UNIQUE INDEX accounts_number_open_uq ON accounts (number) WHERE closed_at IS NULL;`,
		`DROP INDEX accounts_number_open_uq;
ALTER TABLE accounts ADD CONSTRAINT accounts_number_key UNIQUE (number);`)
	acct = (*cbAccountV9)(nil)
	converged()
	mustFail(t, e, "open account numbers are unique", `INSERT INTO accounts (number, currency, balance) VALUES ('AC-004', 'USD', 0)`)
	e.exec(`UPDATE accounts SET closed_at = now() WHERE number = 'AC-004'`)
	e.exec(`INSERT INTO accounts (number, currency, balance) VALUES ('AC-004', 'USD', 0)`) // the closed number is free again
	mustFail(t, e, "only one open account per number", `INSERT INTO accounts (number, currency, balance) VALUES ('AC-004', 'USD', 0)`)
	e.exec(`DELETE FROM accounts WHERE number = 'AC-004' AND closed_at IS NULL`)

	// Release 12, an immutable ledger: entries can be added but never changed or removed.
	dev.hand("ledger_immutable",
		`-- +goose StatementBegin
CREATE FUNCTION forbid_ledger_change() RETURNS trigger AS $$
BEGIN
	RAISE EXCEPTION 'journal entries are immutable (% on %)', TG_OP, TG_TABLE_NAME;
END
$$ LANGUAGE plpgsql;
-- +goose StatementEnd
CREATE TRIGGER ledger_immutable BEFORE UPDATE OR DELETE ON journal_entries FOR EACH ROW EXECUTE FUNCTION forbid_ledger_change();`,
		`DROP TRIGGER ledger_immutable ON journal_entries;
DROP FUNCTION forbid_ledger_change();`)
	mustFail(t, e, "immutable ledger (update)", `UPDATE journal_entries SET amount = amount + 1`)
	mustFail(t, e, "immutable ledger (delete)", `DELETE FROM journal_entries`)
	converged()

	// Release 13, transfers: a foreign key from the ledger, and a deferred check that every transfer balances.
	transfer, jrnl = (*cbTransfer)(nil), (*cbEntryV7)(nil)
	generate("transfers")
	dev.hand("transfers_must_balance",
		`-- +goose StatementBegin
CREATE FUNCTION assert_transfer_balanced() RETURNS trigger AS $$
DECLARE net bigint;
BEGIN
	SELECT COALESCE(SUM(CASE direction WHEN 'C' THEN amount ELSE -amount END), 0) INTO net FROM journal_entries WHERE transfer_id = NEW.transfer_id;
	IF net <> 0 THEN
		RAISE EXCEPTION 'transfer % is unbalanced by %', NEW.transfer_id, net;
	END IF;
	RETURN NULL;
END
$$ LANGUAGE plpgsql;
-- +goose StatementEnd
CREATE CONSTRAINT TRIGGER journal_balanced AFTER INSERT ON journal_entries
	DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.transfer_id IS NOT NULL) EXECUTE FUNCTION assert_transfer_balanced();`,
		`DROP TRIGGER journal_balanced ON journal_entries;
DROP FUNCTION assert_transfer_balanced();`)
	if err := cbTransferTx(e, "t-1", acctID("AC-002"), acctID("AC-001"), 5000, true); err != nil {
		t.Fatalf("a balanced transfer: %v", err)
	}
	if err := cbTransferTx(e, "t-2", acctID("AC-002"), acctID("AC-001"), 700, false); err == nil {
		t.Fatal("an unbalanced transfer committed")
	}
	if err := cbTransferTx(e, "t-1", acctID("AC-002"), acctID("AC-001"), 1, true); err == nil {
		t.Fatal("a repeated idempotency key was accepted")
	}
	if e.scanInt(`SELECT count(*) FROM transfers`) != 1 || e.scanInt(`SELECT count(*) FROM journal_entries WHERE transfer_id IS NOT NULL`) != 2 {
		t.Fatal("a rejected transfer left rows behind")
	}
	requireLedgerBalanced(t, e, "journal_entries", "amount", "balance")
	requireTotals(t, e, "balance", 127630, 8000)

	// Release 14: ISO currency codes: Diff narrows the type, a CHECK enforces the format.
	acct = (*cbAccountV10)(nil)
	generate("currency_iso_length")
	dev.hand("currency_iso_check",
		`ALTER TABLE accounts ADD CONSTRAINT accounts_currency_iso CHECK (currency ~ '^[A-Z]{3}$');`,
		`ALTER TABLE accounts DROP CONSTRAINT accounts_currency_iso;`)
	mustFail(t, e, "currency CHECK", `INSERT INTO accounts (number, currency, balance) VALUES ('AC-X1', 'usd', 0)`)
	mustFail(t, e, "currency length", `INSERT INTO accounts (number, currency, balance) VALUES ('AC-X2', 'USDX', 0)`)

	// Release 15: reporting objects that depend on the tables.
	dev.hand("reporting_views",
		cbAccountOverview+`
CREATE MATERIALIZED VIEW account_daily_totals AS
SELECT account_id, date_trunc('day', posted_at) AS day, sum(CASE direction WHEN 'C' THEN amount ELSE -amount END) AS net
FROM journal_entries GROUP BY 1, 2;
CREATE UNIQUE INDEX account_daily_totals_uq ON account_daily_totals (account_id, day);`,
		`DROP MATERIALIZED VIEW account_daily_totals;
DROP VIEW account_overview;`)
	e.exec(`REFRESH MATERIALIZED VIEW account_daily_totals`)
	if e.scanInt(`SELECT count(*) FROM account_daily_totals`) == 0 || e.scanInt(`SELECT count(*) FROM account_overview`) != 6 {
		t.Fatal("the reporting views do not see the data (the closed account must be hidden)")
	}
	converged()

	// Release 16: a view blocks the column change, so Diff refuses; the migration rebuilds the view around it.
	acct = (*cbAccountV11)(nil)
	if _, err := e.diff("iban numbers", models()...); !errors.Is(err, sqlsvc.ErrInvalidMigration) {
		t.Fatalf("Diff altering a column used by a view = %v; want ErrInvalidMigration", err)
	}
	dev.hand("account_number_iban_length",
		`DROP VIEW account_overview;
ALTER TABLE accounts ALTER COLUMN number TYPE varchar(34);
`+cbAccountOverview,
		`DROP VIEW account_overview;
ALTER TABLE accounts ALTER COLUMN number TYPE varchar;
`+cbAccountOverview)
	converged()
	if e.scanInt(`SELECT count(*) FROM account_overview`) != 6 {
		t.Fatal("the rebuilt view lost rows")
	}

	// Release 17: archive settled history, carrying each account's net forward so balances still reconcile.
	archive = (*cbArchiveV1)(nil)
	generate("journal_archive")
	dev.hand("archive_2024_entries",
		`ALTER TABLE journal_entries DISABLE TRIGGER ledger_immutable;
INSERT INTO journal_archive (id, account_id, direction, amount, posted_at)
	SELECT id, account_id, direction, amount, posted_at FROM journal_entries WHERE posted_at < '2025-01-01';
INSERT INTO journal_entries (account_id, direction, amount, posted_at)
	SELECT account_id, CASE WHEN net >= 0 THEN 'C' ELSE 'D' END, abs(net), '2025-01-01'
	FROM (SELECT account_id, SUM(CASE direction WHEN 'C' THEN amount ELSE -amount END) AS net FROM journal_archive GROUP BY account_id) s
	WHERE net <> 0;
DELETE FROM journal_entries WHERE posted_at < '2025-01-01';
ALTER TABLE journal_entries ENABLE TRIGGER ledger_immutable;`,
		`ALTER TABLE journal_entries DISABLE TRIGGER ledger_immutable;
DELETE FROM journal_entries WHERE posted_at = '2025-01-01' AND transfer_id IS NULL;
INSERT INTO journal_entries (id, account_id, direction, amount, posted_at)
	SELECT id, account_id, direction, amount, posted_at FROM journal_archive;
DELETE FROM journal_archive;
ALTER TABLE journal_entries ENABLE TRIGGER ledger_immutable;`)
	if got := e.scanInt(`SELECT count(*) FROM journal_archive`); got != 8 {
		t.Fatalf("%d entries archived; want the 8 from 2024", got)
	}
	if got := e.scanInt(`SELECT count(*) FROM journal_entries`); got != 8 {
		t.Fatalf("%d live entries; want 6 carry-forwards and the 2 of the transfer", got)
	}
	requireLedgerBalanced(t, e, "journal_entries", "amount", "balance")
	requireTotals(t, e, "balance", 127630, 8000)
	mustFail(t, e, "the immutable trigger is back on", `DELETE FROM journal_entries`)

	// Release 18: merge duplicate customers by hand, re-pointing everything that referenced the duplicate,
	// and only then let Diff add the unique constraint that would have failed before.
	if e.scanInt(`SELECT count(*) - count(DISTINCT tax_id) FROM customers`) != 1 {
		t.Fatal("the seed data should contain exactly one duplicate customer")
	}
	dev.hand("merge_duplicate_customers",
		`CREATE TEMP TABLE dup_map ON COMMIT DROP AS SELECT id AS dup_id, min(id) OVER (PARTITION BY tax_id) AS keep_id FROM customers;
DELETE FROM dup_map WHERE dup_id = keep_id;
INSERT INTO account_holders (account_id, customer_id, role)
	SELECT h.account_id, m.keep_id, h.role FROM account_holders h JOIN dup_map m ON m.dup_id = h.customer_id ON CONFLICT DO NOTHING;
DELETE FROM account_holders WHERE customer_id IN (SELECT dup_id FROM dup_map);
INSERT INTO customer_contacts (customer_id, kind, value)
	SELECT m.keep_id, c.kind, c.value FROM customer_contacts c JOIN dup_map m ON m.dup_id = c.customer_id ON CONFLICT (customer_id, kind) DO NOTHING;
DELETE FROM customer_contacts WHERE customer_id IN (SELECT dup_id FROM dup_map);
DELETE FROM customers WHERE id IN (SELECT dup_id FROM dup_map);`,
		`SELECT 1;`)
	cust = (*cbCustomerV3)(nil)
	generate("customer_tax_id_unique")
	if e.scanInt(`SELECT count(*) FROM customers`) != 5 || e.scanInt(`SELECT count(*) FROM customer_contacts`) != 9 {
		t.Fatal("the merge left duplicates or lost contacts")
	}
	if e.scanInt(`SELECT count(*) FROM account_holders WHERE account_id = $1 AND customer_id = (SELECT id FROM customers WHERE legal_name = 'Barbara Liskov')`, acctID("AC-006")) != 1 {
		t.Fatal("the duplicate's account was not re-pointed to the surviving customer")
	}
	mustFail(t, e, "unique tax id", `INSERT INTO customers (legal_name, tax_id) VALUES ('Clone', 'T-100')`)

	// Release 19, zero-downtime constraints: added NOT VALID so legacy rows do not block the deploy,
	// validated in a later release once the data is clean.
	e.exec(`INSERT INTO transfers (idempotency_key, from_account_id, to_account_id, amount) VALUES ('legacy-bad', $1, $2, 0)`, acctID("AC-001"), acctID("AC-002"))
	dev.hand("not_valid_constraints",
		`ALTER TABLE transfers ADD CONSTRAINT transfers_amount_positive CHECK (amount > 0) NOT VALID;
ALTER TABLE journal_archive ADD CONSTRAINT journal_archive_account_id_fkey FOREIGN KEY (account_id) REFERENCES accounts (id) NOT VALID;`,
		`ALTER TABLE journal_archive DROP CONSTRAINT journal_archive_account_id_fkey;
ALTER TABLE transfers DROP CONSTRAINT transfers_amount_positive;`)
	archive = (*cbArchiveV2)(nil)
	converged()
	mustFail(t, e, "NOT VALID still checks new rows", `INSERT INTO transfers (idempotency_key, from_account_id, to_account_id, amount) VALUES ('new-bad', $1, $2, -1)`, acctID("AC-001"), acctID("AC-002"))
	mustFail(t, e, "validating while a bad row exists", `ALTER TABLE transfers VALIDATE CONSTRAINT transfers_amount_positive`)
	e.exec(`DELETE FROM transfers WHERE idempotency_key = 'legacy-bad'`)
	dev.hand("validate_constraints",
		`ALTER TABLE transfers VALIDATE CONSTRAINT transfers_amount_positive;
ALTER TABLE journal_archive VALIDATE CONSTRAINT journal_archive_account_id_fkey;`,
		`ALTER TABLE journal_archive DROP CONSTRAINT journal_archive_account_id_fkey;
ALTER TABLE journal_archive ADD CONSTRAINT journal_archive_account_id_fkey FOREIGN KEY (account_id) REFERENCES accounts (id) NOT VALID;
ALTER TABLE transfers DROP CONSTRAINT transfers_amount_positive;
ALTER TABLE transfers ADD CONSTRAINT transfers_amount_positive CHECK (amount > 0) NOT VALID;`)
	if e.scanInt(`SELECT count(*) FROM pg_constraint WHERE conname IN ('transfers_amount_positive', 'journal_archive_account_id_fkey') AND NOT convalidated`) != 0 {
		t.Fatal("a constraint is still NOT VALID")
	}

	// Release 20: retire the compatibility view, rename a column, add an outbox fed by a trigger, index its jsonb.
	dev.hand("drop_ledger_entries_view",
		`DROP VIEW ledger_entries;`,
		`CREATE VIEW ledger_entries AS SELECT id, account_id, direction, amount, posted_at FROM journal_entries;`)
	if e.hasTable("ledger_entries") {
		t.Fatal("the compatibility view is still there")
	}
	cust, outbox = (*cbCustomerV4)(nil), (*cbOutbox)(nil)
	generate("display_name_and_outbox")
	if e.hasColumn("customers", "legal_name") || e.scanInt(`SELECT count(*) FROM customers WHERE display_name IN ('Ada Lovelace', 'Grace Hopper')`) != 2 {
		t.Fatal("the rename lost or kept data")
	}
	dev.hand("outbox_trigger_and_index",
		`-- +goose StatementBegin
CREATE FUNCTION emit_transfer_event() RETURNS trigger AS $$
BEGIN
	INSERT INTO outbox_events (topic, payload) VALUES ('transfer.created', jsonb_build_object('id', NEW.id, 'amount', NEW.amount));
	RETURN NEW;
END
$$ LANGUAGE plpgsql;
-- +goose StatementEnd
CREATE TRIGGER transfers_outbox AFTER INSERT ON transfers FOR EACH ROW EXECUTE FUNCTION emit_transfer_event();
CREATE INDEX outbox_events_payload_idx ON outbox_events USING gin (payload jsonb_path_ops);
CREATE INDEX outbox_events_created_idx ON outbox_events (created_at DESC);`,
		`DROP INDEX outbox_events_created_idx;
DROP INDEX outbox_events_payload_idx;
DROP TRIGGER transfers_outbox ON transfers;
DROP FUNCTION emit_transfer_event();`)
	if err := cbTransferTx(e, "t-3", acctID("AC-001"), acctID("AC-002"), 2500, true); err != nil {
		t.Fatalf("a transfer after the final release: %v", err)
	}
	if e.scanInt(`SELECT count(*) FROM outbox_events WHERE payload @> '{"amount": 2500}'`) != 1 {
		t.Fatal("the transfer did not reach the outbox")
	}
	converged()
	requireLedgerBalanced(t, e, "journal_entries", "amount", "balance")
	requireTotals(t, e, "balance", 127630, 8000)
	if got := e.scanInt(`SELECT count(*) FROM accounts`); got != 7 {
		t.Fatalf("%d accounts; want 7", got)
	}

	if got := len(dev.order); got < 30 {
		t.Fatalf("only %d migrations were applied across twenty releases", got)
	}
	return dev, lagFiles, lagVersion
}

// ---- tests ------------------------------------------------------------------

// Happy: twenty releases of a banking schema, with money, ledger and customer data in place throughout.
func TestSQLBanking_TwentyReleasesOfBreakingChanges(t *testing.T) {
	dev, _, _ := buildBanking(t)

	final := fingerprint(dev.e)
	for _, want := range []string{
		"journal_balanced", "ledger_immutable", "accounts_number_open_uq", "account_daily_totals",
		"outbox_events_payload_idx", "accounts_currency_iso",
	} {
		if !strings.Contains(final, want) {
			t.Errorf("final schema is missing %s", want)
		}
	}
	if strings.Contains(final, "sync_minor_units") {
		t.Error("the dual-write trigger function should be gone after the contract release")
	}
}

// Happy: a production database still on release 3 (numeric money, text status, one table of customers) is
// upgraded through every breaking change at once, with 200 customers, 20 of them duplicates.
func TestSQLBanking_ProductionBehindBySeventeenReleasesUpgradesWithItsData(t *testing.T) {
	dev, lagFiles, lagVersion := buildBanking(t)

	prod := dev.e.sibling()
	svc := prod.newServiceWith(false)
	if err := svc.Run(bg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Stop(bg) })
	if err := svc.UpSteps(bg, lagFiles); err != nil {
		t.Fatalf("bringing production to release 3: %v", err)
	}
	if prod.appliedVersion() != lagVersion || fingerprint(prod) != dev.snaps[lagVersion] {
		t.Fatal("production at release 3 does not match development at release 3")
	}

	prod.exec(`INSERT INTO customers (legal_name, tax_id, email, phone)
		SELECT 'Customer ' || g, 'P-' || (CASE WHEN g > 180 THEN g - 180 ELSE g END), 'p' || g || '@bank.io', '555-' || lpad(g::text, 4, '0')
		FROM generate_series(1, 200) g`)
	prod.exec(`INSERT INTO accounts (customer_id, number, currency, balance)
		SELECT id, 'PA-' || lpad(id::text, 5, '0'), 'USD', ((id * 37) % 100000) / 100.0 FROM customers`)
	prod.exec(`INSERT INTO ledger_entries (account_id, direction, amount, posted_at) SELECT id, 'C', balance, '2024-06-01' FROM accounts WHERE balance > 0`)
	wantMinor := prod.scanInt(`SELECT sum(round(balance * 100))::bigint FROM accounts`)
	requireLedgerBalanced(t, prod, "ledger_entries", "amount", "balance")

	if err := svc.Migrate(bg); err != nil {
		t.Fatalf("upgrading production through seventeen releases: %v", err)
	}
	if prod.appliedVersion() != dev.e.appliedVersion() || fingerprint(prod) != fingerprint(dev.e) {
		t.Fatal("production and development diverged after the upgrade")
	}
	if got := prod.scanInt(`SELECT sum(balance) FROM accounts`); got != wantMinor {
		t.Fatalf("total balance %d minor units; want %d: money changed across the conversion", got, wantMinor)
	}
	requireLedgerBalanced(t, prod, "journal_entries", "amount", "balance")

	if got := prod.scanInt(`SELECT count(*) FROM customers`); got != 180 {
		t.Fatalf("customers = %d; want 180 after merging 20 duplicates", got)
	}
	if got := prod.scanInt(`SELECT count(*) FROM accounts`); got != 200 {
		t.Fatalf("accounts = %d; none may be lost in the merge", got)
	}
	if got := prod.scanInt(`SELECT count(*) FROM account_holders WHERE role = 'owner'`); got != 200 {
		t.Fatalf("%d owners; every account needs exactly one", got)
	}
	if got := prod.scanInt(`SELECT count(*) FROM customer_contacts`); got != 360 {
		t.Fatalf("contacts = %d; want an email and a phone for each of 180 customers", got)
	}
	if got := prod.scanInt(`SELECT count(*) FROM journal_archive`); got != 200 {
		t.Fatalf("archived entries = %d; want the 200 from 2024", got)
	}
	if got := prod.scanInt(`SELECT count(*) FROM journal_entries WHERE posted_at = '2025-01-01'`); got != 200 {
		t.Fatalf("carry-forward entries = %d; want one per account", got)
	}
	if got := prod.scanInt(`SELECT count(*) FROM accounts WHERE status = 1 AND closed_at IS NULL`); got != 200 {
		t.Fatalf("%d accounts have status 1 and are open; want all 200", got)
	}

	// The upgraded database enforces everything the later releases introduced.
	mustFail(t, prod, "immutable ledger", `UPDATE journal_entries SET amount = amount + 1`)
	mustFail(t, prod, "unique tax id", `INSERT INTO customers (legal_name, tax_id) VALUES ('Clone', 'P-1')`)
	mustFail(t, prod, "currency CHECK", `INSERT INTO accounts (number, currency, balance) VALUES ('X', 'usd', 0)`)
	first, last := prod.scanInt(`SELECT min(id) FROM accounts`), prod.scanInt(`SELECT max(id) FROM accounts`)
	if err := cbTransferTx(prod, "up-1", first, last, 100, false); err == nil {
		t.Fatal("an unbalanced transfer committed on the upgraded database")
	}
	if err := cbTransferTx(prod, "up-2", first, last, 100, true); err != nil {
		t.Fatalf("a balanced transfer on the upgraded database: %v", err)
	}
	if prod.scanInt(`SELECT count(*) FROM outbox_events`) != 1 {
		t.Fatal("the upgraded database did not emit the outbox event")
	}
	requireLedgerBalanced(t, prod, "journal_entries", "amount", "balance")

	if err := svc.Migrate(bg); err != nil {
		t.Fatalf("a second Migrate must be a no-op: %v", err)
	}
}

// Happy: the full history rebuilds an empty database, and rolling back one migration at a time passes
// through exactly the schema each release had going forward, including views, triggers and functions.
func TestSQLBanking_ReplayFromScratchThenRollBackEveryMigration(t *testing.T) {
	dev, _, _ := buildBanking(t)
	final := fingerprint(dev.e)

	fresh := dev.e.sibling()
	svc := fresh.running()
	if fingerprint(fresh) != final {
		t.Fatal("a database built from the migration files differs from the evolved one")
	}

	for i := len(dev.order) - 1; i >= 1; i-- {
		if err := svc.Rollback(bg); err != nil {
			t.Fatalf("rolling back migration %d of %d: %v", i+1, len(dev.order), err)
		}
		want := dev.order[i-1]
		if got := fresh.appliedVersion(); got != want {
			t.Fatalf("after rollback #%d the version is %d; want %d", len(dev.order)-i, got, want)
		}
		if got := fingerprint(fresh); got != dev.snaps[want] {
			t.Fatalf("after rolling back to version %d the schema differs from the original:\n--- got\n%s\n--- want\n%s", want, got, dev.snaps[want])
		}
	}
	if err := svc.Rollback(bg); err != nil {
		t.Fatalf("rolling back the first migration: %v", err)
	}
	if fresh.schema() != "" || strings.Contains(fingerprint(fresh), "CREATE") {
		t.Fatalf("objects survived a full rollback:\n%s", fingerprint(fresh))
	}

	if err := svc.Migrate(bg); err != nil {
		t.Fatalf("re-applying the whole history: %v", err)
	}
	if fingerprint(fresh) != final {
		t.Fatal("re-applying after a full rollback did not reproduce the final schema")
	}
}
