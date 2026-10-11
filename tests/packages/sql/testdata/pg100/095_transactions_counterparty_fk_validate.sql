-- v095, phase 4. Step 3: validate the history without blocking writes.
-- +goose Up
SET LOCAL lock_timeout = '5s';
ALTER TABLE transactions VALIDATE CONSTRAINT transactions_counterparty_account_fkey;

-- +goose Down
ALTER TABLE transactions DROP CONSTRAINT transactions_counterparty_account_fkey;
ALTER TABLE transactions ADD CONSTRAINT transactions_counterparty_account_fkey FOREIGN KEY (counterparty_account_id) REFERENCES accounts (id) NOT VALID;
