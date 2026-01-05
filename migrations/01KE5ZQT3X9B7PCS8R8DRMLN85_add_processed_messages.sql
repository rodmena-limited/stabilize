-- migration: add_processed_messages
-- id: 01KE5ZQT3X9B7PCS8R8DRMLN85

-- migrate: up

-- Processed messages table for message deduplication (idempotency)
-- Tracks which messages have been successfully processed to prevent
-- duplicate execution on message replay
CREATE TABLE processed_messages (
    message_id VARCHAR(36) PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT NOW(),
    handler_type VARCHAR(100),
    execution_id VARCHAR(26)
);

-- Index for cleanup of old processed messages
CREATE INDEX idx_processed_messages_at ON processed_messages(processed_at);

-- Index for querying by execution (useful for debugging)
CREATE INDEX idx_processed_messages_execution ON processed_messages(execution_id);

-- migrate: down

DROP INDEX IF EXISTS idx_processed_messages_execution;
DROP INDEX IF EXISTS idx_processed_messages_at;
DROP TABLE IF EXISTS processed_messages;
