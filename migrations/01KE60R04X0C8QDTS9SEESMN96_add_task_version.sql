-- migrate: up
ALTER TABLE task_executions ADD COLUMN version INTEGER DEFAULT 0;

-- migrate: down
ALTER TABLE task_executions DROP COLUMN version;
