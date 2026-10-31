-- migrate: up
SET LOCAL lock_timeout = '5s';

ALTER TABLE workflow_signals ADD COLUMN signal_user TEXT NOT NULL DEFAULT '';
ALTER TABLE workflow_signals ADD COLUMN persistent BOOLEAN NOT NULL DEFAULT TRUE;

-- migrate: down
SELECT 1;
