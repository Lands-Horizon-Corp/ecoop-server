-- v072, phase 3. Trigram GIN (pg_trgm) for 'contains' search on emails in the back office.
-- +goose Up
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX users_email_trgm_idx ON users USING gin (email gin_trgm_ops);

-- +goose Down
DROP INDEX users_email_trgm_idx;
DROP EXTENSION pg_trgm;
