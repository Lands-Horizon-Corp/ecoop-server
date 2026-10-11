-- v038, phase 2. Money as integer cents: exact arithmetic, no rounding drift. Expand: new column,
-- backfill, constrain.
-- +goose Up
ALTER TABLE transactions ADD COLUMN amount_cents bigint;
UPDATE transactions SET amount_cents = round(amount * 100);
ALTER TABLE transactions ALTER COLUMN amount_cents SET NOT NULL;
ALTER TABLE transactions ADD CONSTRAINT transactions_amount_cents_check CHECK (amount_cents <> 0);

-- +goose Down
ALTER TABLE transactions DROP CONSTRAINT transactions_amount_cents_check;
ALTER TABLE transactions DROP COLUMN amount_cents;
