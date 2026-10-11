-- v041, phase 2. Emails are stored normalized; the unique index then really means one account per
-- mailbox.
-- +goose Up
ALTER TABLE users ADD CONSTRAINT users_email_lowercase CHECK (email = lower(email));

-- +goose Down
ALTER TABLE users DROP CONSTRAINT users_email_lowercase;
