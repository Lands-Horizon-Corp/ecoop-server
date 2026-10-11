-- v090, phase 4. Step 5: with a validated CHECK in place, SET NOT NULL skips the table scan
-- (PG12+). The check is then redundant.
-- +goose Up
SET LOCAL lock_timeout = '5s';
ALTER TABLE accounts ALTER COLUMN iban SET NOT NULL;
ALTER TABLE accounts DROP CONSTRAINT accounts_iban_not_null;

-- +goose Down
ALTER TABLE accounts ADD CONSTRAINT accounts_iban_not_null CHECK (iban IS NOT NULL);
ALTER TABLE accounts ALTER COLUMN iban DROP NOT NULL;
