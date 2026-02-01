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
    conn.commit()
