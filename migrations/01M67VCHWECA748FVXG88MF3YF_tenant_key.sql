-- migration: tenant_key
-- id: 01M67VCHWECA748FVXG88MF3YF

-- migrate: up

SET LOCAL lock_timeout = '5s';

ALTER TABLE pipeline_executions ADD COLUMN stabilize_tenant TEXT NOT NULL DEFAULT '';
ALTER TABLE stage_executions ADD COLUMN stabilize_tenant TEXT NOT NULL DEFAULT '';
ALTER TABLE task_executions ADD COLUMN stabilize_tenant TEXT NOT NULL DEFAULT '';
ALTER TABLE queue_messages ADD COLUMN stabilize_tenant TEXT NOT NULL DEFAULT '';
ALTER TABLE queue_messages_dlq ADD COLUMN stabilize_tenant TEXT NOT NULL DEFAULT '';
ALTER TABLE processed_messages ADD COLUMN stabilize_tenant TEXT NOT NULL DEFAULT '';
ALTER TABLE workflow_signals ADD COLUMN stabilize_tenant TEXT NOT NULL DEFAULT '';
ALTER TABLE stage_claims ADD COLUMN stabilize_tenant TEXT NOT NULL DEFAULT '';
ALTER TABLE events ADD COLUMN stabilize_tenant TEXT NOT NULL DEFAULT '';
ALTER TABLE snapshots ADD COLUMN stabilize_tenant TEXT NOT NULL DEFAULT '';
ALTER TABLE event_subscriptions ADD COLUMN stabilize_tenant TEXT NOT NULL DEFAULT '';
ALTER TABLE task_leases ADD COLUMN stabilize_tenant TEXT NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS stabilize_tenancy (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    mode TEXT NOT NULL,
    enabled_at TIMESTAMPTZ,
    enabled_by TEXT
);

INSERT INTO stabilize_tenancy (id, mode) VALUES (1, 'single') ON CONFLICT (id) DO NOTHING;

CREATE OR REPLACE FUNCTION stabilize_is_table_owner(rel regclass) RETURNS boolean
    LANGUAGE sql STABLE SECURITY INVOKER SET search_path = pg_catalog, pg_temp
    AS $$ SELECT pg_has_role(current_user, c.relowner, 'MEMBER') FROM pg_catalog.pg_class c WHERE c.oid = rel $$;

-- migrate: down

SELECT 1;
