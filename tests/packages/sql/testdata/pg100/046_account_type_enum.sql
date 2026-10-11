-- v046, phase 2. Account products.
-- +goose Up
CREATE TYPE account_type AS ENUM ('checking', 'savings');
ALTER TABLE accounts ADD COLUMN type account_type NOT NULL DEFAULT 'savings';

-- +goose Down
ALTER TABLE accounts DROP COLUMN type;
DROP TYPE account_type;
