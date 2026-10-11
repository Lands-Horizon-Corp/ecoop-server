-- v035, phase 2. Splitting an attribute, step 1: add the new columns and backfill them from the
-- old one.
-- +goose Up
ALTER TABLE users ADD COLUMN first_name text;
ALTER TABLE users ADD COLUMN last_name text;
UPDATE users SET first_name = split_part(full_name, ' ', 1),
                 last_name  = substr(full_name, length(split_part(full_name, ' ', 1)) + 2);

-- +goose Down
ALTER TABLE users DROP COLUMN last_name;
ALTER TABLE users DROP COLUMN first_name;
