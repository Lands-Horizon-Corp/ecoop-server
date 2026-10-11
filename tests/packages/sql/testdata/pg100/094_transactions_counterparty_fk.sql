-- v094, phase 4. Step 2: a foreign key added NOT VALID: checked for new rows, no scan of history
-- under lock.
-- +goose Up
SET LOCAL lock_timeout = '5s';
ALTER TABLE transactions ADD CONSTRAINT transactions_counterparty_account_fkey FOREIGN KEY (counterparty_account_id) REFERENCES accounts (id) NOT VALID;

-- +goose Down
ALTER TABLE transactions DROP CONSTRAINT transactions_counterparty_account_fkey;
