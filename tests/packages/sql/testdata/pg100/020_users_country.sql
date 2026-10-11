-- v020, phase 1. Residency, for regulatory reporting.
-- +goose Up
ALTER TABLE users ADD COLUMN country char(2) NOT NULL DEFAULT 'PH';

-- +goose Down
ALTER TABLE users DROP COLUMN country;
