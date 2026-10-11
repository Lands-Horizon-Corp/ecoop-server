-- v025, phase 2. MILESTONE v025 (Phase 2: Growth & Normalization). Free text becomes a closed
-- vocabulary. An enum costs 4 bytes instead of a string, rejects typos at the database, and
-- documents the domain in the schema. The conversion rewrites the table, acceptable while it is
-- still small; Phase 4 shows how to change types without rewrites.
-- +goose Up
CREATE TYPE transaction_kind AS ENUM ('deposit', 'withdrawal', 'transfer_in', 'transfer_out', 'fee');
ALTER TABLE transactions ALTER COLUMN kind TYPE transaction_kind USING kind::transaction_kind;

-- +goose Down
ALTER TABLE transactions ALTER COLUMN kind TYPE text USING kind::text;
DROP TYPE transaction_kind;
