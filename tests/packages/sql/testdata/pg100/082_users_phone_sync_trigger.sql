-- v082, phase 4. Step 2: a trigger keeps both columns in step, so old and new application versions
-- can run side by side (the parallel run).
-- +goose Up
SET LOCAL lock_timeout = '5s';
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

-- +goose Down
DROP TRIGGER users_sync_phone ON users;
DROP FUNCTION users_sync_phone();
