-- v096, phase 4. A partitioned table cannot CREATE INDEX CONCURRENTLY, so: an invalid parent index
-- ON ONLY the parent, each partition built CONCURRENTLY, then attached; the parent turns valid
-- when the last one attaches.
-- +goose NO TRANSACTION
-- +goose Up
CREATE INDEX IF NOT EXISTS transactions_counterparty_idx ON ONLY transactions (counterparty_account_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS transactions_y2023_counterparty_idx ON transactions_y2023 (counterparty_account_id);
ALTER INDEX transactions_counterparty_idx ATTACH PARTITION transactions_y2023_counterparty_idx;
CREATE INDEX CONCURRENTLY IF NOT EXISTS transactions_y2024_counterparty_idx ON transactions_y2024 (counterparty_account_id);
ALTER INDEX transactions_counterparty_idx ATTACH PARTITION transactions_y2024_counterparty_idx;
CREATE INDEX CONCURRENTLY IF NOT EXISTS transactions_y2025_counterparty_idx ON transactions_y2025 (counterparty_account_id);
ALTER INDEX transactions_counterparty_idx ATTACH PARTITION transactions_y2025_counterparty_idx;
CREATE INDEX CONCURRENTLY IF NOT EXISTS transactions_y2026_counterparty_idx ON transactions_y2026 (counterparty_account_id);
ALTER INDEX transactions_counterparty_idx ATTACH PARTITION transactions_y2026_counterparty_idx;
CREATE INDEX CONCURRENTLY IF NOT EXISTS transactions_y2027_counterparty_idx ON transactions_y2027 (counterparty_account_id);
ALTER INDEX transactions_counterparty_idx ATTACH PARTITION transactions_y2027_counterparty_idx;
CREATE INDEX CONCURRENTLY IF NOT EXISTS transactions_y2028_counterparty_idx ON transactions_y2028 (counterparty_account_id);
ALTER INDEX transactions_counterparty_idx ATTACH PARTITION transactions_y2028_counterparty_idx;
CREATE INDEX CONCURRENTLY IF NOT EXISTS transactions_default_counterparty_idx ON transactions_default (counterparty_account_id);
ALTER INDEX transactions_counterparty_idx ATTACH PARTITION transactions_default_counterparty_idx;

-- +goose Down
DROP INDEX IF EXISTS transactions_counterparty_idx;
