-- v089, phase 4. Step 4: VALIDATE scans the rows under SHARE UPDATE EXCLUSIVE, which lets reads
-- and writes continue.
-- +goose Up
SET LOCAL lock_timeout = '5s';
ALTER TABLE accounts VALIDATE CONSTRAINT accounts_iban_not_null;

-- +goose Down
ALTER TABLE accounts DROP CONSTRAINT accounts_iban_not_null;
ALTER TABLE accounts ADD CONSTRAINT accounts_iban_not_null CHECK (iban IS NOT NULL) NOT VALID;
