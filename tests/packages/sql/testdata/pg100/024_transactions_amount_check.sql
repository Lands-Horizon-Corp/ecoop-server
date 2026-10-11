-- v024, phase 2. A zero-amount transaction is always a bug.
-- +goose Up
ALTER TABLE transactions ADD CONSTRAINT transactions_amount_check CHECK (amount <> 0);

-- +goose Down
ALTER TABLE transactions DROP CONSTRAINT transactions_amount_check;
