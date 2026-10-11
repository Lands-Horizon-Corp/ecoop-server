-- v098, phase 4. Partition creation becomes a function a scheduler calls; this release provisions
-- 2029 with it.
-- +goose Up
SET LOCAL lock_timeout = '5s';
-- +goose StatementBegin
CREATE FUNCTION ensure_transactions_partition(p_year integer) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  EXECUTE format(
    'CREATE TABLE IF NOT EXISTS %I PARTITION OF transactions FOR VALUES FROM (%L) TO (%L)',
    'transactions_y' || p_year,
    make_timestamptz(p_year, 1, 1, 0, 0, 0, 'UTC'),
    make_timestamptz(p_year + 1, 1, 1, 0, 0, 0, 'UTC'));
END $$;
-- +goose StatementEnd
SELECT ensure_transactions_partition(2029);

-- +goose Down
DROP TABLE IF EXISTS transactions_y2029;
DROP FUNCTION ensure_transactions_partition(integer);
