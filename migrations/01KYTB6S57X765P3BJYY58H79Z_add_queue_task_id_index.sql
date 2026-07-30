-- migration: add_queue_task_id_index
-- id: 01KYTB6S57X765P3BJYY58H79Z

-- migrate: up

-- Recovery's duplicate-guard (has_pending_message_for_task) matches on the
-- payload's top-level task_id. An expression index makes that an index probe
-- instead of a sequential scan of queue_messages per running task per sweep.
CREATE INDEX IF NOT EXISTS idx_queue_messages_task_id
    ON queue_messages ((payload ->> 'task_id'));

-- migrate: down

DROP INDEX IF EXISTS idx_queue_messages_task_id;
