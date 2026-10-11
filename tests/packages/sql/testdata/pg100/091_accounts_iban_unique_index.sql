-- v091, phase 4. Unique without blocking writes: build the index CONCURRENTLY. A failed concurrent
-- build leaves an INVALID index behind, so the file drops any leftover first and can simply be run
-- again.
-- +goose NO TRANSACTION
-- +goose Up
DROP INDEX CONCURRENTLY IF EXISTS accounts_iban_key;
CREATE UNIQUE INDEX CONCURRENTLY accounts_iban_key ON accounts (iban);

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS accounts_iban_key;
