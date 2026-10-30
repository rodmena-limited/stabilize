-- migration: task_leases
-- id: 01M64SFQGDVM97KQS2FFAJ72R5

-- migrate: up

CREATE TABLE IF NOT EXISTS task_leases (
    task_id TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    acquired_at BIGINT NOT NULL,
    expires_at BIGINT NOT NULL
);

-- migrate: down

SELECT 1;
