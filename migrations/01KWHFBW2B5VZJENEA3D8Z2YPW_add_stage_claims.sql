-- migrate: up
CREATE TABLE IF NOT EXISTS stage_claims (
    execution_id TEXT NOT NULL,
    claim_key TEXT NOT NULL,
    stage_id TEXT NOT NULL,
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (execution_id, claim_key)
);

-- migrate: down
DROP TABLE IF EXISTS stage_claims;
