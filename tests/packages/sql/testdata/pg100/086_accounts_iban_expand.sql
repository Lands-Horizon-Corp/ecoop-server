-- v086, phase 4. Required column without a long lock, step 1: add it nullable (instant).
-- +goose Up
SET LOCAL lock_timeout = '5s';
ALTER TABLE accounts ADD COLUMN iban text;

-- +goose Down
ALTER TABLE accounts DROP COLUMN iban;
