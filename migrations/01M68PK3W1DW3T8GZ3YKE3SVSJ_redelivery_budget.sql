-- migrate: up
SET LOCAL lock_timeout = '5s';

ALTER TABLE queue_messages ADD COLUMN store_retries INTEGER NOT NULL DEFAULT 0;
ALTER TABLE queue_messages ADD COLUMN store_retry_since TIMESTAMPTZ;
ALTER TABLE queue_messages_dlq ADD COLUMN store_retries INTEGER NOT NULL DEFAULT 0;
ALTER TABLE queue_messages_dlq ADD COLUMN store_retry_since TIMESTAMPTZ;

-- migrate: down
SELECT 1;
