-- v053, phase 3. Expression index: case-insensitive name search.
-- +goose Up
CREATE INDEX users_lower_name_idx ON users (lower(first_name || ' ' || last_name));

-- +goose Down
DROP INDEX users_lower_name_idx;
