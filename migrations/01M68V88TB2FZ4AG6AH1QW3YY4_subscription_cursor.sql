-- migrate: up
SET LOCAL lock_timeout = '5s';

ALTER TABLE event_subscriptions ADD COLUMN IF NOT EXISTS enabled BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE event_subscriptions ADD COLUMN IF NOT EXISTS last_commit_sequence BIGINT NOT NULL DEFAULT 0;

-- migrate: down
SELECT 1;
