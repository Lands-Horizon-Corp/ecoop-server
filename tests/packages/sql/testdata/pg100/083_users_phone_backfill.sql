-- v083, phase 4. Step 3: backfill existing rows in batches of 10,000, each its own short
-- transaction.
-- +goose NO TRANSACTION
-- +goose Up
-- +goose StatementBegin
DO $$
DECLARE
  last_id bigint := 0;
  max_id  bigint;
BEGIN
  SELECT coalesce(max(id), 0) INTO max_id FROM users;
  WHILE last_id < max_id LOOP
    UPDATE users SET phone_number = phone WHERE id > last_id AND id <= last_id + 10000 AND phone_number IS DISTINCT FROM phone;
    last_id := last_id + 10000;
    COMMIT; -- one short transaction per batch: no long row locks, no bloat spike
  END LOOP;
END $$;
-- +goose StatementEnd

-- +goose Down
SELECT 1; -- step 1's Down removes the column
