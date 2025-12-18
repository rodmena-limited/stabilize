-- migration: initial_schema
-- id: 01KDQ4N9QPJ6Q4MCV3V9GHWPV4

-- migrate: up

CREATE TABLE pipeline_executions (
    id VARCHAR(26) PRIMARY KEY,
    type VARCHAR(50) NOT NULL,
    application VARCHAR(255) NOT NULL,
    name VARCHAR(255),
    status VARCHAR(50) NOT NULL,
    start_time BIGINT,
    end_time BIGINT,
    start_time_expiry BIGINT,
    trigger JSONB,
    context JSONB,
    is_canceled BOOLEAN DEFAULT FALSE,
    canceled_by VARCHAR(255),
    cancellation_reason TEXT,
    paused JSONB,
    pipeline_config_id VARCHAR(255),
    is_limit_concurrent BOOLEAN DEFAULT FALSE,
    max_concurrent_executions INT DEFAULT 0,
    keep_waiting_pipelines BOOLEAN DEFAULT FALSE,
    origin VARCHAR(50) DEFAULT 'unknown',
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE stage_executions (
    id VARCHAR(26) PRIMARY KEY,
    execution_id VARCHAR(26) NOT NULL REFERENCES pipeline_executions(id) ON DELETE CASCADE,
    ref_id VARCHAR(50) NOT NULL,
    type VARCHAR(100) NOT NULL,
    name VARCHAR(255),
    status VARCHAR(50) NOT NULL,
    context JSONB,
    outputs JSONB,
    requisite_stage_ref_ids TEXT[],
    parent_stage_id VARCHAR(26),
    synthetic_stage_owner VARCHAR(20),
    start_time BIGINT,
    end_time BIGINT,
    start_time_expiry BIGINT,
    scheduled_time BIGINT,
    UNIQUE(execution_id, ref_id)
);

CREATE TABLE task_executions (
    id VARCHAR(26) PRIMARY KEY,
    stage_id VARCHAR(26) NOT NULL REFERENCES stage_executions(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    implementing_class VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    start_time BIGINT,
    end_time BIGINT,
    stage_start BOOLEAN DEFAULT FALSE,
    stage_end BOOLEAN DEFAULT FALSE,
    loop_start BOOLEAN DEFAULT FALSE,
    loop_end BOOLEAN DEFAULT FALSE,
    task_exception_details JSONB
);

CREATE TABLE queue_messages (
    id SERIAL PRIMARY KEY,
    message_id VARCHAR(36) NOT NULL UNIQUE,
    message_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    deliver_at TIMESTAMP NOT NULL DEFAULT NOW(),
    attempts INT DEFAULT 0,
    max_attempts INT DEFAULT 10,
    locked_until TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_executions_application ON pipeline_executions(application);
CREATE INDEX idx_executions_config_id ON pipeline_executions(pipeline_config_id);
CREATE INDEX idx_executions_status ON pipeline_executions(status);
CREATE INDEX idx_stage_execution ON stage_executions(execution_id);
CREATE INDEX idx_stage_parent ON stage_executions(parent_stage_id);
CREATE INDEX idx_task_stage ON task_executions(stage_id);
CREATE INDEX idx_queue_deliver ON queue_messages(deliver_at) WHERE attempts < 10;
CREATE INDEX idx_queue_locked ON queue_messages(locked_until);

-- migrate: down

DROP INDEX IF EXISTS idx_queue_locked;
DROP INDEX IF EXISTS idx_queue_deliver;
DROP INDEX IF EXISTS idx_task_stage;
DROP INDEX IF EXISTS idx_stage_parent;
DROP INDEX IF EXISTS idx_stage_execution;
DROP INDEX IF EXISTS idx_executions_status;
DROP INDEX IF EXISTS idx_executions_config_id;
DROP INDEX IF EXISTS idx_executions_application;
DROP TABLE IF EXISTS queue_messages;
DROP TABLE IF EXISTS task_executions;
DROP TABLE IF EXISTS stage_executions;
DROP TABLE IF EXISTS pipeline_executions;
