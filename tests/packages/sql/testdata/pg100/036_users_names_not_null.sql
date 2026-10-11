-- v036, phase 2. Step 2: nullable-to-NOT NULL, safe because step 1 filled every row.
-- +goose Up
ALTER TABLE users ALTER COLUMN first_name SET NOT NULL;
ALTER TABLE users ALTER COLUMN last_name SET NOT NULL;

-- +goose Down
ALTER TABLE users ALTER COLUMN last_name DROP NOT NULL;
ALTER TABLE users ALTER COLUMN first_name DROP NOT NULL;
