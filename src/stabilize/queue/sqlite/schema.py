"""SQLite queue schema creation."""

from __future__ import annotations

import sqlite3


def create_queue_tables(conn: sqlite3.Connection, table_name: str) -> None:
    """Create the queue table and DLQ table if they don't exist.

    Args:
        conn: SQLite connection
        table_name: Base name for the queue table (DLQ uses table_name_dlq)
    """
    # Main queue table
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
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
        )
    """)
    conn.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_{table_name}_deliver
        ON {table_name}(deliver_at)
    """)
    conn.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_{table_name}_locked
        ON {table_name}(locked_until)
    """)
    # Serves recovery's has_pending_message_for_task exact-match lookup.
    conn.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_{table_name}_task_id
        ON {table_name}(json_extract(payload, '$.task_id'))
    """)

    # Dead Letter Queue table
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name}_dlq (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            original_id INTEGER,
            message_id TEXT NOT NULL,
            message_type TEXT NOT NULL,
            payload TEXT NOT NULL,
            attempts INTEGER,
            error TEXT,
            last_error_at TEXT,
            created_at TEXT DEFAULT (datetime('now', 'utc')),
            moved_at TEXT DEFAULT (datetime('now', 'utc'))
        )
    """)
    conn.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_{table_name}_dlq_type
        ON {table_name}_dlq(message_type)
    """)
    for statement in queue_lookup_indexes(table_name):
        conn.execute(statement)
    conn.commit()
    ensure_redelivery_columns(conn)


def queue_lookup_indexes(table_name: str) -> tuple[str, ...]:
    return tuple(
        f"CREATE INDEX IF NOT EXISTS idx_{table}_{field} ON {table}"
        f"(CASE WHEN json_valid(payload) THEN json_extract(payload, '$.{field}') END)"
        for table, field in (
            (table_name, "execution_id"),
            (table_name, "stage_id"),
            (f"{table_name}_dlq", "execution_id"),
        )
    )


REDELIVERY_COLUMNS: tuple[tuple[str, str], ...] = (
    ("store_retries", "INTEGER NOT NULL DEFAULT 0"),
    ("store_retry_since", "TEXT"),
)


def ensure_redelivery_columns(conn: sqlite3.Connection) -> list[str]:
    """Add the store-retry budget columns to every queue-shaped table that lacks them; returns the tables changed."""
    from stabilize.persistence.sqlite.tenant_schema import engine_shaped_tables

    changed: list[str] = []
    for table in engine_shaped_tables(conn):
        present = {str(row[1]) for row in conn.execute(f'PRAGMA table_info("{table}")').fetchall()}
        if "payload" not in present or "attempts" not in present:
            continue
        for column, ddl in REDELIVERY_COLUMNS:
            if column not in present:
                conn.execute(f'ALTER TABLE "{table}" ADD COLUMN {column} {ddl}')
                changed.append(table)
    if changed:
        conn.commit()
    return sorted(set(changed))
