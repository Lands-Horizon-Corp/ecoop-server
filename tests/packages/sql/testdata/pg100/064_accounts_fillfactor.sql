-- v064, phase 3. Balances update constantly; free space per page keeps updates HOT (no index
-- churn).
-- +goose Up
ALTER TABLE accounts SET (fillfactor = 90);

-- +goose Down
ALTER TABLE accounts RESET (fillfactor);
