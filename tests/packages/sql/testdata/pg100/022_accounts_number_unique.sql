-- v022, phase 2. Account numbers are unique.
-- +goose Up
CREATE UNIQUE INDEX accounts_number_key ON accounts (number);
DROP INDEX accounts_number_idx;

-- +goose Down
CREATE INDEX accounts_number_idx ON accounts (number);
DROP INDEX accounts_number_key;
