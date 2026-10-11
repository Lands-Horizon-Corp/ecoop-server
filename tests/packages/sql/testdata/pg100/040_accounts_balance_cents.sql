-- v040, phase 2. Balances in cents, expanded and contracted in one step while the table is still
-- small enough.
-- +goose Up
ALTER TABLE accounts ADD COLUMN balance_cents bigint NOT NULL DEFAULT 0;
UPDATE accounts SET balance_cents = round(balance * 100);
ALTER TABLE accounts ADD CONSTRAINT accounts_balance_cents_check CHECK (balance_cents >= 0);
ALTER TABLE accounts DROP COLUMN balance;

-- +goose Down
ALTER TABLE accounts ADD COLUMN balance numeric(14,2) NOT NULL DEFAULT 0;
UPDATE accounts SET balance = balance_cents / 100.0;
ALTER TABLE accounts ADD CONSTRAINT accounts_balance_check CHECK (balance >= 0);
ALTER TABLE accounts DROP CONSTRAINT accounts_balance_cents_check;
ALTER TABLE accounts DROP COLUMN balance_cents;
