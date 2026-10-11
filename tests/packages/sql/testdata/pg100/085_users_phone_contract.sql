-- v085, phase 4. Step 5, contract: once every reader uses phone_number, the trigger and the old
-- column go. Down restores both, data included.
-- +goose Up
SET LOCAL lock_timeout = '5s';
DROP TRIGGER users_sync_phone ON users;
DROP FUNCTION users_sync_phone();
ALTER TABLE users DROP COLUMN phone;

-- +goose Down
ALTER TABLE users ADD COLUMN phone text;
UPDATE users SET phone = phone_number;
-- +goose StatementBegin
CREATE FUNCTION users_sync_phone() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    NEW.phone_number := coalesce(NEW.phone_number, NEW.phone);
    NEW.phone := coalesce(NEW.phone, NEW.phone_number);
  ELSIF NEW.phone IS DISTINCT FROM OLD.phone THEN
    NEW.phone_number := NEW.phone;
  ELSIF NEW.phone_number IS DISTINCT FROM OLD.phone_number THEN
    NEW.phone := NEW.phone_number;
  END IF;
  RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER users_sync_phone BEFORE INSERT OR UPDATE ON users FOR EACH ROW EXECUTE FUNCTION users_sync_phone();
