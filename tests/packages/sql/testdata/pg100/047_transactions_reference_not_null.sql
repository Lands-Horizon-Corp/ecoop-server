-- v047, phase 2. Nullable-to-NOT NULL with a backfill first: every transaction gets a reference.
-- +goose Up
UPDATE transactions SET reference = 'TX' || id WHERE reference IS NULL;
ALTER TABLE transactions ALTER COLUMN reference SET NOT NULL;

-- +goose Down
ALTER TABLE transactions ALTER COLUMN reference DROP NOT NULL;
