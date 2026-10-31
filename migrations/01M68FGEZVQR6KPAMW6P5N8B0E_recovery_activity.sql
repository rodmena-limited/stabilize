-- migrate: up
SET LOCAL lock_timeout = '5s';

ALTER TABLE pipeline_executions ADD COLUMN stabilize_updated_at BIGINT;
ALTER TABLE stage_executions ADD COLUMN stabilize_updated_at BIGINT;
ALTER TABLE task_executions ADD COLUMN stabilize_updated_at BIGINT;
ALTER TABLE pipeline_executions ADD COLUMN created_at_ms BIGINT;

UPDATE pipeline_executions
SET created_at_ms = (EXTRACT(EPOCH FROM created_at AT TIME ZONE 'UTC') * 1000)::bigint
WHERE created_at_ms IS NULL AND created_at IS NOT NULL;

CREATE OR REPLACE FUNCTION stabilize_touch_updated_at() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    NEW.stabilize_updated_at := (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint;
    RETURN NEW;
END;
$$;

CREATE TRIGGER stabilize_activity
    BEFORE UPDATE OF status, start_time, end_time, is_canceled, canceled_by, cancellation_reason, paused
    ON pipeline_executions FOR EACH ROW EXECUTE FUNCTION stabilize_touch_updated_at();
CREATE TRIGGER stabilize_activity
    BEFORE UPDATE OF status, context, outputs, start_time, end_time, scheduled_time, version
    ON stage_executions FOR EACH ROW EXECUTE FUNCTION stabilize_touch_updated_at();
CREATE TRIGGER stabilize_activity
    BEFORE UPDATE OF status, start_time, end_time, task_exception_details, version
    ON task_executions FOR EACH ROW EXECUTE FUNCTION stabilize_touch_updated_at();

-- migrate: down
SELECT 1;
