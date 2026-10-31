-- migrate: up
SET LOCAL lock_timeout = '5s';

ALTER TABLE pipeline_executions ADD COLUMN pipeline_config_version VARCHAR(64);
ALTER TABLE pipeline_executions ADD COLUMN pipeline_config_sha256 VARCHAR(64);
ALTER TABLE pipeline_executions ADD COLUMN pipeline_config_hash_alg VARCHAR(32);
CREATE INDEX IF NOT EXISTS idx_executions_tenant_config_version
    ON pipeline_executions (stabilize_tenant, pipeline_config_id, pipeline_config_version);

-- migrate: down
SELECT 1;
