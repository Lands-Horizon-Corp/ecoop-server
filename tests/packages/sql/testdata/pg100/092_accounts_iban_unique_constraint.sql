-- v092, phase 4. Promote the ready index to a constraint: catalog-only, no scan.
-- +goose Up
SET LOCAL lock_timeout = '5s';
ALTER TABLE accounts ADD CONSTRAINT accounts_iban_key UNIQUE USING INDEX accounts_iban_key;

-- +goose Down
ALTER TABLE accounts DROP CONSTRAINT accounts_iban_key;
CREATE UNIQUE INDEX accounts_iban_key ON accounts (iban);
