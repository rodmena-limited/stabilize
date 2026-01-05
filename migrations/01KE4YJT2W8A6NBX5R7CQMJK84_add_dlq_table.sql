-- migration: add_dlq_table
-- id: 01KE4YJT2W8A6NBX5R7CQMJK84

-- migrate: up

-- Dead Letter Queue for failed messages
-- Messages that exceed max_attempts are moved here instead of being silently dropped
CREATE TABLE queue_messages_dlq (
    id SERIAL PRIMARY KEY,
    original_id INTEGER,
    message_id VARCHAR(36) NOT NULL,
    message_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    attempts INTEGER,
    error TEXT,
    last_error_at TIMESTAMP,
    created_at TIMESTAMP,
    moved_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_queue_dlq_type ON queue_messages_dlq(message_type);
CREATE INDEX idx_queue_dlq_moved_at ON queue_messages_dlq(moved_at);

-- migrate: down

DROP INDEX IF EXISTS idx_queue_dlq_moved_at;
DROP INDEX IF EXISTS idx_queue_dlq_type;
DROP TABLE IF EXISTS queue_messages_dlq;
