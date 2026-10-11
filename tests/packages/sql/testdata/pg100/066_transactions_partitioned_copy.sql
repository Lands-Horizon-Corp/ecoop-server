-- v066, phase 3. Step 2: copy the history. Writes made meanwhile already reach the twin through
-- the trigger; ON CONFLICT skips them.
-- +goose Up
INSERT INTO transactions_p SELECT * FROM transactions ON CONFLICT DO NOTHING;

-- +goose Down
TRUNCATE transactions_p;
