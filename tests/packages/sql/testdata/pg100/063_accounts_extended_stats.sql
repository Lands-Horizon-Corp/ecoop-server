-- v063, phase 3. Extended statistics: user and currency are correlated, and the planner should
-- know.
-- +goose Up
CREATE STATISTICS accounts_user_currency_stats (dependencies) ON user_id, currency FROM accounts;

-- +goose Down
DROP STATISTICS accounts_user_currency_stats;
