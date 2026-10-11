-- v021, phase 2. Uniqueness moves from application code into the database; the plain index it
-- replaces goes.
-- +goose Up
CREATE UNIQUE INDEX users_email_key ON users (email);
DROP INDEX users_email_idx;

-- +goose Down
CREATE INDEX users_email_idx ON users (email);
DROP INDEX users_email_key;
