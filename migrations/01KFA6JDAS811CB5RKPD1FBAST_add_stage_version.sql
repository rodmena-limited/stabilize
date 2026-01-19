-- migrate: up
ALTER TABLE stage_executions ADD COLUMN version INTEGER DEFAULT 0;

-- migrate: down
ALTER TABLE stage_executions DROP COLUMN version;
