-- v049, phase 2. Plausible birth dates only.
-- +goose Up
ALTER TABLE users ADD CONSTRAINT users_date_of_birth_check CHECK (date_of_birth IS NULL OR date_of_birth > DATE '1900-01-01');

-- +goose Down
ALTER TABLE users DROP CONSTRAINT users_date_of_birth_check;
