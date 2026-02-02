-- migration: widen_message_id_column
-- id: 01KGFG6RM7NRCJ2PPJ7FZAM0Y9

-- migrate: up

-- Widen message_id column to support longer message IDs
-- The original VARCHAR(36) was sized for UUIDs but message_ids can be longer
ALTER TABLE processed_messages ALTER COLUMN message_id TYPE TEXT;

-- migrate: down

-- Revert to VARCHAR(36) - note: this may fail if there are longer values
ALTER TABLE processed_messages ALTER COLUMN message_id TYPE VARCHAR(36);
