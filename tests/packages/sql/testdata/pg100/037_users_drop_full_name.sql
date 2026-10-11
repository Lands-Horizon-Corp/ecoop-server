-- v037, phase 2. Step 3: the old column goes. Down rebuilds it from the parts, so no data is lost
-- either way.
-- +goose Up
ALTER TABLE users DROP COLUMN full_name;

-- +goose Down
ALTER TABLE users ADD COLUMN full_name text;
UPDATE users SET full_name = first_name || ' ' || last_name;
ALTER TABLE users ALTER COLUMN full_name SET NOT NULL;
