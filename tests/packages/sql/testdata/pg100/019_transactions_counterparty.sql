-- v019, phase 1. Who the money came from or went to, as free text.
-- +goose Up
ALTER TABLE transactions ADD COLUMN counterparty text;

-- +goose Down
ALTER TABLE transactions DROP COLUMN counterparty;
