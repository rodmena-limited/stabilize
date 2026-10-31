-- migrate: up
SET LOCAL lock_timeout = '5s';

ALTER TABLE processed_messages ALTER COLUMN processed_at TYPE TIMESTAMPTZ;

-- migrate: down
SELECT 1;
