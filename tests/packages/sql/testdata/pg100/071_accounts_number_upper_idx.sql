-- v071, phase 3. Expression index for case-insensitive account number entry.
-- +goose Up
CREATE INDEX accounts_number_upper_idx ON accounts (upper(number));

-- +goose Down
DROP INDEX accounts_number_upper_idx;
