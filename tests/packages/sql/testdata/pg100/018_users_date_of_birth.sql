-- v018, phase 1. KYC data.
-- +goose Up
ALTER TABLE users ADD COLUMN date_of_birth date;

-- +goose Down
ALTER TABLE users DROP COLUMN date_of_birth;
