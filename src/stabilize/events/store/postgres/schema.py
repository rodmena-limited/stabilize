"""
SQL schema constants for PostgreSQL event store.

Defines the DDL for events, snapshots, and subscription tables.
"""

# Schema for events table (PostgreSQL)
EVENTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    sequence BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(26) NOT NULL UNIQUE,
    event_type VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    entity_type VARCHAR(50) NOT NULL,
    entity_id VARCHAR(26) NOT NULL,
    workflow_id VARCHAR(26) NOT NULL,
    version INTEGER NOT NULL,
    data JSONB NOT NULL DEFAULT '{}',
    correlation_id VARCHAR(36) NOT NULL,
    causation_id VARCHAR(26),
    actor VARCHAR(255) DEFAULT 'system',
    source_handler VARCHAR(100),
    schema_version INTEGER DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_events_entity ON events(entity_type, entity_id, sequence);
CREATE INDEX IF NOT EXISTS idx_events_workflow ON events(workflow_id, sequence);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type, timestamp);
CREATE INDEX IF NOT EXISTS idx_events_correlation ON events(correlation_id);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);
"""

# Schema for snapshots table
SNAPSHOTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshots (
    id SERIAL PRIMARY KEY,
    entity_type VARCHAR(50) NOT NULL,
    entity_id VARCHAR(26) NOT NULL,
    workflow_id VARCHAR(26) NOT NULL,
    version INTEGER NOT NULL,
    sequence BIGINT NOT NULL,
    state JSONB NOT NULL,
    state_hash VARCHAR(64),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(entity_type, entity_id, version)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_entity ON snapshots(entity_type, entity_id);
"""

# Schema for durable subscriptions
SUBSCRIPTIONS_SCHEMA = """
CREATE TABLE IF NOT EXISTS event_subscriptions (
    id VARCHAR(100) PRIMARY KEY,
    event_types TEXT[],
    entity_filter JSONB,
    last_sequence BIGINT DEFAULT 0,
    webhook_url VARCHAR(500),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_commit_cursor TEXT DEFAULT '0'
);
"""


# Commit-ordered delivery for durable subscriptions.
#
# `sequence` is assigned at INSERT, not at COMMIT, so a transaction that
# inserted an earlier sequence can commit after a later one. A cursor that
# advances by sequence steps over it permanently. `commit_xid` plus
# pg_snapshot_xmin() gives a monotone, gap-free frontier instead.
#
# Applied as two statements deliberately: a single
# `ADD COLUMN ... DEFAULT pg_current_xact_id()` uses a volatile default and
# rewrites the whole table. Split, the ADD is instant and pre-existing rows keep
# NULL, which the reader coalesces to '0'::xid8 so they stay deliverable.
EVENTS_COMMIT_XID_MIGRATION = (
    "ALTER TABLE events ADD COLUMN IF NOT EXISTS commit_xid xid8",
    "ALTER TABLE events ALTER COLUMN commit_xid SET DEFAULT pg_current_xact_id()",
    "CREATE INDEX IF NOT EXISTS idx_events_commit ON events(commit_xid, sequence)",
)

# Applied on every version: the column is plain TEXT and the subscription code
# persists it whether or not commit-ordered delivery is available.
SUBSCRIPTIONS_CURSOR_MIGRATION = (
    "ALTER TABLE event_subscriptions ADD COLUMN IF NOT EXISTS last_commit_cursor TEXT DEFAULT '0'",
)

# xid8, pg_current_xact_id() and pg_snapshot_xmin() are PostgreSQL 13+.
MIN_COMMIT_XID_VERSION = 130000


REQUIRED_TABLES = ("events", "snapshots", "event_subscriptions")

REQUIRED_COLUMNS = (
    ("events", "commit_xid"),
    ("event_subscriptions", "last_commit_cursor"),
)

SETUP_DDL = "\n".join(
    (
        EVENTS_SCHEMA.strip(),
        "",
        SNAPSHOTS_SCHEMA.strip(),
        "",
        SUBSCRIPTIONS_SCHEMA.strip(),
        "",
        *(f"{statement};" for statement in SUBSCRIPTIONS_CURSOR_MIGRATION),
        "",
        *(f"{statement};" for statement in EVENTS_COMMIT_XID_MIGRATION),
    )
)


def setup_ddl(schema: str | None = None) -> str:
    """The DDL an operator applies, schema-qualified when *schema* is given."""
    if not schema:
        return SETUP_DDL
    return f"SET search_path TO {schema};\n\n{SETUP_DDL}"
