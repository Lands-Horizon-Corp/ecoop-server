-- v087, phase 4. Step 2: backfill in batches.
-- +goose NO TRANSACTION
-- +goose Up
-- +goose StatementBegin
DO $$
DECLARE
  last_id bigint := 0;
  max_id  bigint;
BEGIN
  SELECT coalesce(max(id), 0) INTO max_id FROM accounts;
  WHILE last_id < max_id LOOP
    UPDATE accounts SET iban = 'PH' || lpad(id::text, 20, '0') WHERE id > last_id AND id <= last_id + 10000 AND iban IS NULL;
    last_id := last_id + 10000;
    COMMIT; -- one short transaction per batch: no long row locks, no bloat spike
  END LOOP;
END $$;
-- +goose StatementEnd

-- +goose Down
SELECT 1; -- step 1's Down removes the column
