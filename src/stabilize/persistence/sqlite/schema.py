"""Database schema for SQLite persistence."""

from __future__ import annotations

import sqlite3

# SQLite schema definition
SCHEMA = """
CREATE TABLE IF NOT EXISTS pipeline_executions (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    application TEXT NOT NULL,
    name TEXT,
    status TEXT NOT NULL,
    context TEXT DEFAULT '{}',
    start_time INTEGER,
    end_time INTEGER,
    start_time_expiry INTEGER,
    trigger TEXT,
    is_canceled INTEGER DEFAULT 0,
    canceled_by TEXT,
    cancellation_reason TEXT,
    paused TEXT,
    pipeline_config_id TEXT,
    is_limit_concurrent INTEGER DEFAULT 0,
    max_concurrent_executions INTEGER DEFAULT 0,
    keep_waiting_pipelines INTEGER DEFAULT 0,
    origin TEXT,
    created_at TEXT DEFAULT (datetime('now', 'utc'))
);

CREATE TABLE IF NOT EXISTS stage_executions (
    id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL REFERENCES pipeline_executions(id) ON DELETE CASCADE,
    ref_id TEXT NOT NULL,
    type TEXT NOT NULL,
    name TEXT,
    status TEXT NOT NULL,
    context TEXT DEFAULT '{}',
    outputs TEXT DEFAULT '{}',
    requisite_stage_ref_ids TEXT,
    parent_stage_id TEXT,
    synthetic_stage_owner TEXT,
    start_time INTEGER,
    end_time INTEGER,
    start_time_expiry INTEGER,
    scheduled_time INTEGER,
    version INTEGER DEFAULT 0,
    join_type TEXT DEFAULT 'AND',
    join_threshold INTEGER DEFAULT 0,
    split_type TEXT DEFAULT 'AND',
    split_conditions TEXT DEFAULT '{}',
    mi_config TEXT,
    deferred_choice_group TEXT,
    milestone_ref_id TEXT,
    milestone_status TEXT,
    mutex_key TEXT,
    cancel_region TEXT,
    UNIQUE(execution_id, ref_id)
);

CREATE TABLE IF NOT EXISTS task_executions (
    id TEXT PRIMARY KEY,
    stage_id TEXT NOT NULL REFERENCES stage_executions(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    implementing_class TEXT NOT NULL,
    status TEXT NOT NULL,
    start_time INTEGER,
    end_time INTEGER,
    stage_start INTEGER DEFAULT 0,
    stage_end INTEGER DEFAULT 0,
    loop_start INTEGER DEFAULT 0,
    loop_end INTEGER DEFAULT 0,
    task_exception_details TEXT DEFAULT '{}',
    version INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS queue_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id TEXT NOT NULL UNIQUE,
    message_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    deliver_at TEXT NOT NULL DEFAULT (datetime('now', 'utc')),
    attempts INTEGER DEFAULT 0,
    max_attempts INTEGER DEFAULT 10,
    locked_until TEXT,
    version INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now', 'utc'))
);

CREATE INDEX IF NOT EXISTS idx_execution_application
    ON pipeline_executions(application);
CREATE INDEX IF NOT EXISTS idx_execution_config
    ON pipeline_executions(pipeline_config_id);
CREATE INDEX IF NOT EXISTS idx_execution_status
    ON pipeline_executions(status);
CREATE INDEX IF NOT EXISTS idx_stage_execution
    ON stage_executions(execution_id);
CREATE INDEX IF NOT EXISTS idx_task_stage
    ON task_executions(stage_id);
CREATE INDEX IF NOT EXISTS idx_queue_deliver
    ON queue_messages(deliver_at);
CREATE INDEX IF NOT EXISTS idx_queue_locked
    ON queue_messages(locked_until);

CREATE TABLE IF NOT EXISTS processed_messages (
    message_id TEXT PRIMARY KEY,
    processed_at TEXT NOT NULL DEFAULT (datetime('now', 'utc')),
    handler_type TEXT,
    execution_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_processed_messages_at
    ON processed_messages(processed_at);
"""


def create_tables(conn: sqlite3.Connection) -> None:
    """Create database tables if they don't exist."""
    for statement in SCHEMA.split(";"):
        statement = statement.strip()
        if statement:
            conn.execute(statement)
    conn.commit()
