-- migration: purge_and_lookup_indexes
-- id: 01M4SVVY5XE3S18Y0CPQ8TATDP

-- migrate: up

CREATE INDEX IF NOT EXISTS idx_executions_end_time
    ON pipeline_executions (end_time) WHERE end_time IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_executions_application_end_time
    ON pipeline_executions (application, end_time) WHERE end_time IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_queue_messages_execution_id
    ON queue_messages ((payload ->> 'execution_id'));

CREATE INDEX IF NOT EXISTS idx_queue_dlq_execution_id
    ON queue_messages_dlq ((payload ->> 'execution_id'));

-- migrate: down

DROP INDEX IF EXISTS idx_queue_dlq_execution_id;
DROP INDEX IF EXISTS idx_queue_messages_execution_id;
DROP INDEX IF EXISTS idx_executions_application_end_time;
DROP INDEX IF EXISTS idx_executions_end_time;
