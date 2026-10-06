-- migration: widen_queue_ids_to_bigint
-- id: 01M482Z7ZP5AXJ8PJCGB5XZTCZ

-- migrate: up

ALTER SEQUENCE queue_messages_id_seq AS BIGINT;
ALTER TABLE queue_messages ALTER COLUMN id TYPE BIGINT;
ALTER SEQUENCE queue_messages_dlq_id_seq AS BIGINT;
ALTER TABLE queue_messages_dlq ALTER COLUMN id TYPE BIGINT;
ALTER TABLE queue_messages_dlq ALTER COLUMN original_id TYPE BIGINT;

-- migrate: down

ALTER TABLE queue_messages_dlq ALTER COLUMN original_id TYPE INTEGER;
ALTER TABLE queue_messages_dlq ALTER COLUMN id TYPE INTEGER;
ALTER SEQUENCE queue_messages_dlq_id_seq AS INTEGER;
ALTER TABLE queue_messages ALTER COLUMN id TYPE INTEGER;
ALTER SEQUENCE queue_messages_id_seq AS INTEGER;
