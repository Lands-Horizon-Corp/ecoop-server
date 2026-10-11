-- v075, phase 3. Extended statistics on the partitioned table.
-- +goose Up
CREATE STATISTICS transactions_account_kind_stats (ndistinct) ON account_id, kind FROM transactions;

-- +goose Down
DROP STATISTICS transactions_account_kind_stats;
