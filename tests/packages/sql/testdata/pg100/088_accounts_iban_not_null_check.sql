-- v088, phase 4. Step 3: a CHECK added NOT VALID is enforced for new writes at once, without
-- scanning existing rows under lock.
-- +goose Up
SET LOCAL lock_timeout = '5s';
ALTER TABLE accounts ADD CONSTRAINT accounts_iban_not_null CHECK (iban IS NOT NULL) NOT VALID;

-- +goose Down
ALTER TABLE accounts DROP CONSTRAINT accounts_iban_not_null;
