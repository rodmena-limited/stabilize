-- migration: queue_bigint_ids_and_timestamptz
-- id: 01M483DVR3DS980FHS8N754CWH

-- migrate: up

ALTER SEQUENCE queue_messages_id_seq AS BIGINT;
ALTER TABLE queue_messages
    ALTER COLUMN id TYPE BIGINT,
    ALTER COLUMN deliver_at TYPE TIMESTAMPTZ USING deliver_at AT TIME ZONE current_setting('TimeZone'),
    ALTER COLUMN locked_until TYPE TIMESTAMPTZ USING locked_until AT TIME ZONE current_setting('TimeZone'),
    ALTER COLUMN created_at TYPE TIMESTAMPTZ USING created_at AT TIME ZONE current_setting('TimeZone');

ALTER SEQUENCE queue_messages_dlq_id_seq AS BIGINT;
ALTER TABLE queue_messages_dlq
    ALTER COLUMN id TYPE BIGINT,
    ALTER COLUMN original_id TYPE BIGINT,
    ALTER COLUMN last_error_at TYPE TIMESTAMPTZ USING last_error_at AT TIME ZONE current_setting('TimeZone'),
    ALTER COLUMN created_at TYPE TIMESTAMPTZ USING created_at AT TIME ZONE current_setting('TimeZone'),
    ALTER COLUMN moved_at TYPE TIMESTAMPTZ USING moved_at AT TIME ZONE current_setting('TimeZone');

-- migrate: down

ALTER TABLE queue_messages_dlq
    ALTER COLUMN moved_at TYPE TIMESTAMP USING moved_at AT TIME ZONE current_setting('TimeZone'),
    ALTER COLUMN created_at TYPE TIMESTAMP USING created_at AT TIME ZONE current_setting('TimeZone'),
    ALTER COLUMN last_error_at TYPE TIMESTAMP USING last_error_at AT TIME ZONE current_setting('TimeZone'),
    ALTER COLUMN original_id TYPE INTEGER,
    ALTER COLUMN id TYPE INTEGER;
ALTER SEQUENCE queue_messages_dlq_id_seq AS INTEGER;

ALTER TABLE queue_messages
    ALTER COLUMN created_at TYPE TIMESTAMP USING created_at AT TIME ZONE current_setting('TimeZone'),
    ALTER COLUMN locked_until TYPE TIMESTAMP USING locked_until AT TIME ZONE current_setting('TimeZone'),
    ALTER COLUMN deliver_at TYPE TIMESTAMP USING deliver_at AT TIME ZONE current_setting('TimeZone'),
    ALTER COLUMN id TYPE INTEGER;
ALTER SEQUENCE queue_messages_id_seq AS INTEGER;
