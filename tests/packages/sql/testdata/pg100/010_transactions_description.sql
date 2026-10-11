-- v010, phase 1. Free-text memo. (The data-seeding test loads 100,000 rows per table at this
-- version.)
-- +goose Up
ALTER TABLE transactions ADD COLUMN description text;

-- +goose Down
ALTER TABLE transactions DROP COLUMN description;
