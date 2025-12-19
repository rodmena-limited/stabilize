from __future__ import annotations
import json
import logging
import sqlite3
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any
from stabilize.queue.messages import (
    Message,
    create_message_from_dict,
    get_message_type_name,
)
from stabilize.queue.queue import Queue
logger = logging.getLogger(__name__)

class SqliteQueue(Queue):
    """
    SQLite-backed queue implementation.

    Uses optimistic locking with a version column for concurrent access.
    Multiple workers can safely poll messages, though with lower
    throughput than PostgreSQL's SKIP LOCKED.

    The queue uses a `version` column to implement optimistic locking:
    1. SELECT a candidate message with its version
    2. UPDATE the message WHERE version = expected_version
    3. If 0 rows updated, another worker claimed it - retry

    Features:
    - Multi-worker support via optimistic locking
    - Configurable lock duration and max attempts
    - Automatic retry on lock contention
    - WAL mode for better concurrent read performance
    - Thread-local connections managed by singleton ConnectionManager
    """
    def __init__(
        self,
        connection_string: str,
        table_name: str = "queue_messages",
        lock_duration: timedelta = timedelta(minutes=5),
        max_attempts: int = 10,
    ) -> None:
        """
        Initialize the SQLite queue.

        Args:
            connection_string: SQLite connection string (e.g., sqlite:///./db.sqlite)
            table_name: Name of the queue table
            lock_duration: How long to lock messages during processing
            max_attempts: Maximum retry attempts before dropping message
        """
        from stabilize.persistence.connection import get_connection_manager

        self.connection_string = connection_string
        self.table_name = table_name
        self.lock_duration = lock_duration
        self.max_attempts = max_attempts
        self._manager = get_connection_manager()
        self._pending: dict[int, dict[str, Any]] = {}

    def _get_connection(self) -> sqlite3.Connection:
        """
        Get thread-local connection from ConnectionManager.

        Returns a connection configured with:
        - Row factory for dict-like access
        - WAL journal mode for concurrency
        - 30 second busy timeout
        """
        return self._manager.get_sqlite_connection(self.connection_string)

    def close(self) -> None:
        """Close SQLite connection for current thread."""
        self._manager.close_sqlite_connection(self.connection_string)

    def _create_table(self) -> None:
        """Create the queue table if it doesn't exist."""
        conn = self._get_connection()
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT NOT NULL UNIQUE,
                message_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                deliver_at TEXT NOT NULL DEFAULT (datetime('now')),
                attempts INTEGER DEFAULT 0,
                max_attempts INTEGER DEFAULT 10,
                locked_until TEXT,
                version INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """
        )
        conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_{self.table_name}_deliver
            ON {self.table_name}(deliver_at)
        """
        )
        conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_{self.table_name}_locked
            ON {self.table_name}(locked_until)
        """
        )
        conn.commit()

    def _serialize_message(self, message: Message) -> str:
        """Serialize a message to JSON."""
        from enum import Enum

        data = {}
        for key, value in message.__dict__.items():
            if key.startswith("_"):
                continue
            if isinstance(value, datetime):
                data[key] = value.isoformat()
            elif isinstance(value, Enum):
                data[key] = value.name
            else:
                data[key] = value
        return json.dumps(data)
