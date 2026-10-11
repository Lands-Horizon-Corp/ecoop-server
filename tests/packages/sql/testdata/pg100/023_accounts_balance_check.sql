-- v023, phase 2. No negative balances, enforced by the database.
-- +goose Up
ALTER TABLE accounts ADD CONSTRAINT accounts_balance_check CHECK (balance >= 0);

-- +goose Down
ALTER TABLE accounts DROP CONSTRAINT accounts_balance_check;
