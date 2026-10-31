-- migrate: up
SET LOCAL lock_timeout = '5s';

CREATE INDEX IF NOT EXISTS idx_queue_tenant_poll
    ON queue_messages (stabilize_tenant, deliver_at, locked_until, attempts);
CREATE INDEX IF NOT EXISTS idx_queue_tenant_attempts
    ON queue_messages (stabilize_tenant, attempts);
CREATE INDEX IF NOT EXISTS idx_queue_pipeline_config_id
    ON queue_messages ((payload ->> 'pipeline_config_id'));
CREATE INDEX IF NOT EXISTS idx_executions_tenant_config_status
    ON pipeline_executions (stabilize_tenant, pipeline_config_id, status);
CREATE INDEX IF NOT EXISTS idx_executions_tenant_status
    ON pipeline_executions (stabilize_tenant, status);
CREATE INDEX IF NOT EXISTS idx_stages_tenant_status_id
    ON stage_executions (stabilize_tenant, status, id);
CREATE INDEX IF NOT EXISTS idx_events_tenant_sequence
    ON events (stabilize_tenant, sequence);
CREATE INDEX IF NOT EXISTS idx_events_tenant_commit
    ON events (stabilize_tenant, coalesce(commit_xid, '0'::xid8), sequence);

-- migrate: down
SELECT 1;
