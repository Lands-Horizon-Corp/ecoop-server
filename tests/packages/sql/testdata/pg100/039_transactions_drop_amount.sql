-- v039, phase 2. Contract: the numeric column goes (its check with it). Down restores both from
-- the cents.
-- +goose Up
ALTER TABLE transactions DROP COLUMN amount;

-- +goose Down
ALTER TABLE transactions ADD COLUMN amount numeric(14,2);
UPDATE transactions SET amount = amount_cents / 100.0;
ALTER TABLE transactions ALTER COLUMN amount SET NOT NULL;
ALTER TABLE transactions ADD CONSTRAINT transactions_amount_check CHECK (amount <> 0);
