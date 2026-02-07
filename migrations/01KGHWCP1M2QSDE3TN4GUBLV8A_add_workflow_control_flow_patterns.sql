-- migrate: up
ALTER TABLE stage_executions ADD COLUMN join_type TEXT DEFAULT 'AND';
ALTER TABLE stage_executions ADD COLUMN join_threshold INTEGER DEFAULT 0;
ALTER TABLE stage_executions ADD COLUMN split_type TEXT DEFAULT 'AND';
ALTER TABLE stage_executions ADD COLUMN split_conditions JSONB DEFAULT '{}';
ALTER TABLE stage_executions ADD COLUMN mi_config JSONB;
ALTER TABLE stage_executions ADD COLUMN deferred_choice_group TEXT;
ALTER TABLE stage_executions ADD COLUMN milestone_ref_id TEXT;
ALTER TABLE stage_executions ADD COLUMN milestone_status TEXT;
ALTER TABLE stage_executions ADD COLUMN mutex_key TEXT;
ALTER TABLE stage_executions ADD COLUMN cancel_region TEXT;

CREATE TABLE IF NOT EXISTS workflow_signals (
    id BIGSERIAL PRIMARY KEY,
    execution_id TEXT NOT NULL,
    stage_ref_id TEXT NOT NULL,
    signal_name TEXT NOT NULL,
    signal_data JSONB DEFAULT '{}',
    consumed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    consumed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_signals_stage
    ON workflow_signals(execution_id, stage_ref_id, consumed);
CREATE INDEX IF NOT EXISTS idx_signals_name
    ON workflow_signals(signal_name);

-- migrate: down
DROP TABLE IF EXISTS workflow_signals;
ALTER TABLE stage_executions DROP COLUMN IF EXISTS cancel_region;
ALTER TABLE stage_executions DROP COLUMN IF EXISTS mutex_key;
ALTER TABLE stage_executions DROP COLUMN IF EXISTS milestone_status;
ALTER TABLE stage_executions DROP COLUMN IF EXISTS milestone_ref_id;
ALTER TABLE stage_executions DROP COLUMN IF EXISTS deferred_choice_group;
ALTER TABLE stage_executions DROP COLUMN IF EXISTS mi_config;
ALTER TABLE stage_executions DROP COLUMN IF EXISTS split_conditions;
ALTER TABLE stage_executions DROP COLUMN IF EXISTS split_type;
ALTER TABLE stage_executions DROP COLUMN IF EXISTS join_threshold;
ALTER TABLE stage_executions DROP COLUMN IF EXISTS join_type;
