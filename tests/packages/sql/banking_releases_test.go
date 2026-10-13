//go:build integration

package sql_test

import (
	"os"
	"path/filepath"
	"testing"

	ecsql "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sql"
)

// A banking schema's release history: twenty hand-written goose migrations
// with breaking changes (money from numeric to integer cents, text status to
// an enum, a renamed column, deduplicated emails) and views, triggers and
// functions. Release i has version i*10, leaving room for out-of-order tests.

type release struct{ name, up, down string }

var bankReleases = []release{
	{"customers",
		`CREATE TABLE customers (id bigserial PRIMARY KEY, name text NOT NULL, email text NOT NULL);`,
		`DROP TABLE customers;`},
	{"accounts",
		`CREATE TABLE accounts (id bigserial PRIMARY KEY, customer_id bigint NOT NULL REFERENCES customers (id),
  number text, balance numeric(14,2) NOT NULL DEFAULT 0, status text NOT NULL DEFAULT 'active');`,
		`DROP TABLE accounts;`},
	{"ledger",
		`CREATE TABLE ledger (id bigserial PRIMARY KEY, account_id bigint NOT NULL REFERENCES accounts (id),
  amount numeric(14,2) NOT NULL, created_at timestamptz NOT NULL DEFAULT now());`,
		`DROP TABLE ledger;`},
	{"unique_email",
		`UPDATE customers c SET email = c.email || '+dup' || c.id
  WHERE c.id <> (SELECT min(d.id) FROM customers d WHERE d.email = c.email);
CREATE UNIQUE INDEX customers_email_key ON customers (email);`,
		`DROP INDEX customers_email_key;`},
	{"account_numbers",
		`UPDATE accounts SET number = 'AC' || lpad(id::text, 10, '0') WHERE number IS NULL;
ALTER TABLE accounts ALTER COLUMN number SET NOT NULL;
ALTER TABLE accounts ADD CONSTRAINT accounts_number_key UNIQUE (number);`,
		`ALTER TABLE accounts DROP CONSTRAINT accounts_number_key;
ALTER TABLE accounts ALTER COLUMN number DROP NOT NULL;`},
	{"balance_cents",
		`ALTER TABLE accounts ADD COLUMN balance_cents bigint NOT NULL DEFAULT 0;
UPDATE accounts SET balance_cents = round(balance * 100);
ALTER TABLE accounts DROP COLUMN balance;`,
		`ALTER TABLE accounts ADD COLUMN balance numeric(14,2) NOT NULL DEFAULT 0;
UPDATE accounts SET balance = balance_cents / 100.0;
ALTER TABLE accounts DROP COLUMN balance_cents;`},
	{"ledger_cents",
		`ALTER TABLE ledger ADD COLUMN amount_cents bigint;
UPDATE ledger SET amount_cents = round(amount * 100);
ALTER TABLE ledger ALTER COLUMN amount_cents SET NOT NULL;
ALTER TABLE ledger DROP COLUMN amount;`,
		`ALTER TABLE ledger ADD COLUMN amount numeric(14,2);
UPDATE ledger SET amount = amount_cents / 100.0;
ALTER TABLE ledger ALTER COLUMN amount SET NOT NULL;
ALTER TABLE ledger DROP COLUMN amount_cents;`},
	{"status_enum",
		`CREATE TYPE account_status AS ENUM ('active', 'frozen', 'closed');
ALTER TABLE accounts ALTER COLUMN status DROP DEFAULT;
ALTER TABLE accounts ALTER COLUMN status TYPE account_status USING status::account_status;
ALTER TABLE accounts ALTER COLUMN status SET DEFAULT 'active';`,
		`ALTER TABLE accounts ALTER COLUMN status DROP DEFAULT;
ALTER TABLE accounts ALTER COLUMN status TYPE text USING status::text;
ALTER TABLE accounts ALTER COLUMN status SET DEFAULT 'active';
DROP TYPE account_status;`},
	{"balance_nonneg",
		`ALTER TABLE accounts ADD CONSTRAINT accounts_balance_nonneg CHECK (balance_cents >= 0);`,
		`ALTER TABLE accounts DROP CONSTRAINT accounts_balance_nonneg;`},
	{"ledger_nonzero",
		`ALTER TABLE ledger ADD CONSTRAINT ledger_amount_nonzero CHECK (amount_cents <> 0);`,
		`ALTER TABLE ledger DROP CONSTRAINT ledger_amount_nonzero;`},
	{"ledger_append_only",
		`-- +goose StatementBegin
CREATE FUNCTION ledger_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'ledger is append-only';
END $$;
-- +goose StatementEnd
CREATE TRIGGER ledger_no_change BEFORE UPDATE OR DELETE ON ledger FOR EACH ROW EXECUTE FUNCTION ledger_immutable();`,
		`DROP TRIGGER ledger_no_change ON ledger;
DROP FUNCTION ledger_immutable();`},
	{"account_balances_view",
		`CREATE VIEW account_balances AS
  SELECT a.id, a.number, a.balance_cents, coalesce(sum(l.amount_cents), 0)::bigint AS ledger_cents
    FROM accounts a LEFT JOIN ledger l ON l.account_id = a.id GROUP BY a.id;`,
		`DROP VIEW account_balances;`},
	{"transfers",
		`CREATE TABLE transfers (id bigserial PRIMARY KEY,
  from_account bigint NOT NULL REFERENCES accounts (id), to_account bigint NOT NULL REFERENCES accounts (id),
  amount_cents bigint NOT NULL CONSTRAINT transfers_amount_positive CHECK (amount_cents > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT transfers_distinct_accounts CHECK (from_account <> to_account));`,
		`DROP TABLE transfers;`},
	{"ledger_transfer_id",
		`ALTER TABLE ledger ADD COLUMN transfer_id bigint REFERENCES transfers (id);`,
		`ALTER TABLE ledger DROP COLUMN transfer_id;`},
	{"transfer_function",
		`-- +goose StatementBegin
CREATE FUNCTION transfer(p_from bigint, p_to bigint, p_amount bigint) RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE t bigint;
BEGIN
  PERFORM 1 FROM accounts WHERE id IN (p_from, p_to) ORDER BY id FOR UPDATE;
  INSERT INTO transfers (from_account, to_account, amount_cents) VALUES (p_from, p_to, p_amount) RETURNING id INTO t;
  UPDATE accounts SET balance_cents = balance_cents - p_amount WHERE id = p_from AND status = 'active';
  IF NOT FOUND THEN RAISE EXCEPTION 'account % is not active', p_from; END IF;
  UPDATE accounts SET balance_cents = balance_cents + p_amount WHERE id = p_to AND status = 'active';
  IF NOT FOUND THEN RAISE EXCEPTION 'account % is not active', p_to; END IF;
  INSERT INTO ledger (account_id, amount_cents, transfer_id) VALUES (p_from, -p_amount, t), (p_to, p_amount, t);
  RETURN t;
END $$;
-- +goose StatementEnd`,
		`DROP FUNCTION transfer(bigint, bigint, bigint);`},
	{"ledger_account_index",
		`CREATE INDEX ledger_account_created_idx ON ledger (account_id, created_at);`,
		`DROP INDEX ledger_account_created_idx;`},
	{"rename_full_name",
		`ALTER TABLE customers RENAME COLUMN name TO full_name;`,
		`ALTER TABLE customers RENAME COLUMN full_name TO name;`},
	{"customer_phone",
		`ALTER TABLE customers ADD COLUMN phone text;`,
		`ALTER TABLE customers DROP COLUMN phone;`},
	{"account_currency",
		`ALTER TABLE accounts ADD COLUMN currency char(3) NOT NULL DEFAULT 'PHP';`,
		`ALTER TABLE accounts DROP COLUMN currency;`},
	{"customer_accounts_view",
		`CREATE VIEW customer_accounts AS
  SELECT c.id AS customer_id, c.full_name, a.number, a.balance_cents, a.currency
    FROM customers c JOIN accounts a ON a.customer_id = c.id;`,
		`DROP VIEW customer_accounts;`},
}

const bankLatest = 200 // version of the last release

// writeReleases writes releases from..to (1-based, inclusive) into e.
func writeReleases(t testing.TB, e *env, from, to int) {
	t.Helper()
	for i := from; i <= to; i++ {
		r := bankReleases[i-1]
		e.write(t, int64(i*10), r.name, r.up, r.down)
	}
}

// bankSeedSQL fills a database at release 3: 200 customers, 20 of them
// sharing an email with another, one account each (half without a number)
// with numeric money, and one ledger row per account.
const bankSeedSQL = `
INSERT INTO customers (name, email)
  SELECT 'Customer ' || g, 'c' || (CASE WHEN g > 180 THEN g - 180 ELSE g END) || '@bank.test' FROM generate_series(1, 200) g;
INSERT INTO accounts (customer_id, number, balance, status)
  SELECT id, CASE WHEN id % 2 = 0 THEN 'N' || id END, (id * 10.25)::numeric(14,2), (ARRAY['active','frozen','closed'])[1 + id % 3]
    FROM customers;
INSERT INTO ledger (account_id, amount) SELECT id, balance FROM accounts;`

// bankSeedCents is the money bankSeedSQL puts in, in cents.
var bankSeedCents = func() int64 {
	var sum int64
	for id := int64(1); id <= 200; id++ {
		sum += id * 1025
	}
	return sum
}()

// copyMigrations copies every migration file from src into dst.
func copyMigrations(t testing.TB, src, dst *env) {
	t.Helper()
	for _, f := range src.files("*.sql") {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst.path, filepath.Base(f)), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// replayAndRollBack applies e's migrations one at a time on a fresh database
// (each step with its own deadline, so a long history on a busy server is
// not cut off by one overall timeout),
// recording the schema after each, then rolls back one at a time checking
// each step restores the schema recorded before it. It returns the final
// schema.
func replayAndRollBack(t *testing.T, e *env) string {
	t.Helper()
	fresh := newEnv(t)
	copyMigrations(t, e, fresh)
	s := fresh.started(t)
	n := len(fresh.files("*.sql"))
	snaps := []string{snapshot(t, s)}
	for i := range n {
		if err := s.UpSteps(ctxT(t), 1); err != nil {
			t.Fatalf("replay step %d: %v", i+1, err)
		}
		snaps = append(snaps, snapshot(t, s))
	}
	final := snaps[n]
	for i := n; i > 0; i-- {
		if err := s.RollbackSteps(ctxT(t), 1); err != nil {
			t.Fatalf("rolling back step %d: %v", i, err)
		}
		if got := snapshot(t, s); got != snaps[i-1] {
			t.Fatalf("rolling back step %d did not restore the schema:\n%s", i, diffLines(snaps[i-1], got))
		}
	}
	return final
}

// bankMoney is the money in accounts, in cents, whichever column holds it
// at the current release.
func bankMoney(t testing.TB, s ecsql.ISQLService) int64 {
	t.Helper()
	col := "balance_cents"
	if count(t, s, "SELECT count(*) FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'accounts' AND column_name = 'balance'") == 1 {
		col = "round(balance * 100)"
	}
	var sum int64
	if err := s.Client().NewRaw("SELECT coalesce(sum("+col+"), 0)::bigint FROM accounts").Scan(ctxT(t), &sum); err != nil {
		t.Fatal(err)
	}
	return sum
}

// bankAt returns a started service on a new database migrated to release n
// and seeded at release 3 (when n >= 3), with all twenty releases written.
func bankAt(t *testing.T, n int) (*env, ecsql.ISQLService) {
	t.Helper()
	e := newEnv(t)
	writeReleases(t, e, 1, len(bankReleases))
	s := e.started(t)
	ctx := ctxT(t)
	if n >= 3 {
		if err := s.UpSteps(ctx, 3); err != nil {
			t.Fatal(err)
		}
		execSQL(t, s, bankSeedSQL)
		n -= 3
	}
	if n > 0 {
		if err := s.UpSteps(ctx, n); err != nil {
			t.Fatal(err)
		}
	}
	return e, s
}

// account opens an active account with balance cents for a new customer.
func account(t testing.TB, s ecsql.ISQLService, cents int64) int64 {
	t.Helper()
	var id int64
	err := s.Client().NewRaw(`WITH c AS (INSERT INTO customers (full_name, email) VALUES ('T', gen_random_uuid() || '@t.test') RETURNING id)
INSERT INTO accounts (customer_id, number, balance_cents) SELECT id, gen_random_uuid()::text, ? FROM c RETURNING id`, cents).Scan(ctxT(t), &id)
	if err != nil {
		t.Fatalf("opening account: %v", err)
	}
	return id
}

func balance(t testing.TB, s ecsql.ISQLService, id int64) int64 {
	t.Helper()
	var c int64
	if err := s.Client().NewRaw("SELECT balance_cents FROM accounts WHERE id = ?", id).Scan(ctxT(t), &c); err != nil {
		t.Fatal(err)
	}
	return c
}

// copySome copies the first n migration files from src into dst.
func copySome(t testing.TB, src, dst *env, n int) {
	t.Helper()
	for _, f := range src.files("*.sql")[:n] {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst.path, filepath.Base(f)), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}
