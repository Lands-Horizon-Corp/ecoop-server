-- v060, phase 3. Covering index for 'my accounts' with balances. Replacing an index (create, then
-- drop) takes the strongest lock first: building the new one under SHARE and then upgrading to
-- ACCESS EXCLUSIVE for the drop deadlocks with a deposit that already holds a key-share lock and
-- wants to update the row (found by the load test).
-- +goose Up
SET LOCAL lock_timeout = '5s';
LOCK TABLE accounts IN ACCESS EXCLUSIVE MODE;
CREATE INDEX accounts_user_cover_idx ON accounts (user_id) INCLUDE (balance_cents, currency);
DROP INDEX accounts_user_id_idx;

-- +goose Down
CREATE INDEX accounts_user_id_idx ON accounts (user_id);
DROP INDEX accounts_user_cover_idx;
