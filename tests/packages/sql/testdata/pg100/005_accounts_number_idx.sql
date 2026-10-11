-- v005, phase 1. Tellers look accounts up by number.
-- +goose Up
CREATE INDEX accounts_number_idx ON accounts (number);

-- +goose Down
DROP INDEX accounts_number_idx;
