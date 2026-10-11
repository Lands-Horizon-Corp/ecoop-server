-- v027, phase 2. User states become an enum.
-- +goose Up
CREATE TYPE user_status AS ENUM ('active', 'suspended', 'closed');
ALTER TABLE users ALTER COLUMN status DROP DEFAULT;
ALTER TABLE users ALTER COLUMN status TYPE user_status USING status::user_status;
ALTER TABLE users ALTER COLUMN status SET DEFAULT 'active';

-- +goose Down
ALTER TABLE users ALTER COLUMN status DROP DEFAULT;
ALTER TABLE users ALTER COLUMN status TYPE text USING status::text;
ALTER TABLE users ALTER COLUMN status SET DEFAULT 'active';
DROP TYPE user_status;
