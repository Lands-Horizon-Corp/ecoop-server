-- v004, phase 1. Login looks users up by email.
-- +goose Up
CREATE INDEX users_email_idx ON users (email);

-- +goose Down
DROP INDEX users_email_idx;
