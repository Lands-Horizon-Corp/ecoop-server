-- v026, phase 2. Account states become an enum (the default is dropped and restored around the
-- conversion).
-- +goose Up
CREATE TYPE account_status AS ENUM ('open', 'frozen', 'closed');
ALTER TABLE accounts ALTER COLUMN status DROP DEFAULT;
ALTER TABLE accounts ALTER COLUMN status TYPE account_status USING status::account_status;
ALTER TABLE accounts ALTER COLUMN status SET DEFAULT 'open';

-- +goose Down
ALTER TABLE accounts ALTER COLUMN status DROP DEFAULT;
ALTER TABLE accounts ALTER COLUMN status TYPE text USING status::text;
ALTER TABLE accounts ALTER COLUMN status SET DEFAULT 'open';
DROP TYPE account_status;
