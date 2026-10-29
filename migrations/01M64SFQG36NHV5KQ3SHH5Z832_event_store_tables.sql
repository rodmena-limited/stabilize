-- migration: event_store_tables
-- id: 01M64SFQG36NHV5KQ3SHH5Z832

-- migrate: up

CREATE TABLE IF NOT EXISTS events (
    sequence BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(26) NOT NULL UNIQUE,
    event_type VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    entity_type VARCHAR(50) NOT NULL,
    entity_id VARCHAR(26) NOT NULL,
    workflow_id VARCHAR(26) NOT NULL,
    version INTEGER NOT NULL,
    data JSONB NOT NULL DEFAULT '{}',
    correlation_id VARCHAR(36) NOT NULL,
    causation_id VARCHAR(26),
    actor VARCHAR(255) DEFAULT 'system',
    source_handler VARCHAR(100),
    schema_version INTEGER DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_events_entity ON events(entity_type, entity_id, sequence);
CREATE INDEX IF NOT EXISTS idx_events_workflow ON events(workflow_id, sequence);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type, timestamp);
CREATE INDEX IF NOT EXISTS idx_events_correlation ON events(correlation_id);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);

CREATE TABLE IF NOT EXISTS snapshots (
    id SERIAL PRIMARY KEY,
    entity_type VARCHAR(50) NOT NULL,
    entity_id VARCHAR(26) NOT NULL,
    workflow_id VARCHAR(26) NOT NULL,
    version INTEGER NOT NULL,
    sequence BIGINT NOT NULL,
    state JSONB NOT NULL,
    state_hash VARCHAR(64),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(entity_type, entity_id, version)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_entity ON snapshots(entity_type, entity_id);

CREATE TABLE IF NOT EXISTS event_subscriptions (
    id VARCHAR(100) PRIMARY KEY,
    event_types TEXT[],
    entity_filter JSONB,
    last_sequence BIGINT DEFAULT 0,
    webhook_url VARCHAR(500),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_commit_cursor TEXT DEFAULT '0'
);

ALTER TABLE event_subscriptions ADD COLUMN IF NOT EXISTS last_commit_cursor TEXT DEFAULT '0';

DO $$
BEGIN
    IF current_setting('server_version_num')::int >= 130000 THEN
        ALTER TABLE events ADD COLUMN IF NOT EXISTS commit_xid xid8;
        ALTER TABLE events ALTER COLUMN commit_xid SET DEFAULT pg_current_xact_id();
        CREATE INDEX IF NOT EXISTS idx_events_commit ON events(commit_xid, sequence);
    END IF;
END
$$;

-- migrate: down

SELECT 1;
