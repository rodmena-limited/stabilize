-- migration: workflow_config_version
-- id: 01M4Q8VTM40WEC4HR2X31Q07A5

-- migrate: up

ALTER TABLE pipeline_executions ADD COLUMN IF NOT EXISTS config_version VARCHAR(64);

-- migrate: down

ALTER TABLE pipeline_executions DROP COLUMN IF EXISTS config_version;
