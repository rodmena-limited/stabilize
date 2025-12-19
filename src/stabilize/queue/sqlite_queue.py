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
