-- migration: workflow_queue_table_and_stage_index
-- id: 01M4PAJCF82KCKYT5X4GMZH2XG

-- migrate: up

ALTER TABLE pipeline_executions ADD COLUMN IF NOT EXISTS queue_table VARCHAR(255);

CREATE INDEX IF NOT EXISTS idx_queue_messages_stage_id
    ON queue_messages ((payload ->> 'stage_id'));

SELECT setval(
    'queue_messages_id_seq',
    GREATEST(
        (SELECT last_value FROM queue_messages_id_seq),
        COALESCE((SELECT MAX(message_id::bigint) FROM processed_messages WHERE message_id ~ '^[0-9]{1,18}$'), 1)
    )
);

-- migrate: down

DROP INDEX IF EXISTS idx_queue_messages_stage_id;
ALTER TABLE pipeline_executions DROP COLUMN IF EXISTS queue_table;
