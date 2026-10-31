-- migrate: up
SET LOCAL lock_timeout = '5s';

ALTER TABLE task_executions ADD COLUMN generation INTEGER NOT NULL DEFAULT 0;

-- migrate: down
SELECT 1;
