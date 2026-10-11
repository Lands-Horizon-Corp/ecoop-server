-- v100, phase 4. MILESTONE v100 (Phase 4: Enterprise Scale & Zero-Downtime). The last step uses
-- everything before it: a partial index over live (not soft-deleted) users, built CONCURRENTLY so
-- sign-ups and logins never wait. Every Phase 4 file either avoids heavy locks or bounds them with
-- lock_timeout, backfills in batches, splits validation from enforcement, and has a Down that
-- restores the exact previous schema.
-- +goose NO TRANSACTION
-- +goose Up
CREATE INDEX CONCURRENTLY users_live_email_idx ON users (email) WHERE deleted_at IS NULL;

-- +goose Down
DROP INDEX CONCURRENTLY users_live_email_idx;
