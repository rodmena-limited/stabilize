"""
SQLite execution repository.

Lightweight persistence using SQLite for development and small deployments.
Uses singleton ConnectionManager for efficient connection sharing.

Enterprise Features:
- Atomic transactions for store + queue operations
- Dead letter queue support
- Thread-safe connection management via WAL mode
"""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING

from stabilize.persistence.sqlite.operations import (
    cleanup_old_processed_messages as _cleanup_old_processed_messages,
)
from stabilize.persistence.sqlite.operations import (
    is_message_processed as _is_message_processed,
)
from stabilize.persistence.sqlite.operations import (
    mark_message_processed as _mark_message_processed,
)
from stabilize.persistence.sqlite.schema import create_tables
from stabilize.persistence.sqlite.store.queries import SqliteQueriesMixin
from stabilize.persistence.sqlite.store.stage_ops import SqliteStageOpsMixin
from stabilize.persistence.sqlite.store.workflow_crud import SqliteWorkflowCrudMixin
from stabilize.persistence.store import WorkflowStore

if TYPE_CHECKING:
    from stabilize.persistence.store import StoreTransaction
    from stabilize.queue import Queue

logger = logging.getLogger(__name__)


class SqliteWorkflowStore(
    SqliteWorkflowCrudMixin,
    SqliteStageOpsMixin,
    SqliteQueriesMixin,
    WorkflowStore,
):
    """
    SQLite implementation of WorkflowStore.

    Uses native sqlite3 for file-based or in-memory storage.
    Suitable for development, testing, and single-node deployments.

    Features:
    - WAL mode for better concurrent read performance
    - Foreign key support enabled
    - JSON stored as TEXT strings
    - Arrays stored as JSON strings
    - Thread-local connections managed by singleton ConnectionManager
    """

    def __init__(
        self,
        connection_string: str,
        create_tables: bool = False,
    ) -> None:
        """
        Initialize the repository.

        Args:
            connection_string: SQLite connection string (e.g., sqlite:///./db.sqlite)
            create_tables: Whether to create tables if they don't exist
        """
        from stabilize.persistence.connection import get_connection_manager

        self.connection_string = connection_string
        self._manager = get_connection_manager()

        # Verify connection works
        conn = self._get_connection()
        conn.execute("SELECT 1")

        if create_tables:
            self._create_tables()

    def _get_connection(self) -> sqlite3.Connection:
        """
        Get thread-local connection from ConnectionManager.

        Returns a connection configured with:
        - Row factory for dict-like access
        - Foreign keys enabled
        - WAL journal mode for concurrency
        - 30 second busy timeout
        """
        return self._manager.get_sqlite_connection(self.connection_string)

    def close(self) -> None:
        """Close SQLite connection for current thread."""
        self._manager.close_sqlite_connection(self.connection_string)

    def _create_tables(self) -> None:
        """Create database tables if they don't exist."""
        create_tables(self._get_connection())

    def is_healthy(self) -> bool:
        """Check if the database connection is healthy."""
        try:
            conn = self._get_connection()
            conn.execute("SELECT 1")
            return True
        except Exception:
            return False

    @contextmanager
    def transaction(self, queue: Queue | None = None) -> Iterator[StoreTransaction]:
        """Create atomic transaction for store + queue operations.

        Use this when you need to atomically update both stage state AND
        queue a message. This prevents orphaned workflows from crashes
        between separate store and queue operations.

        SQLite implementation writes directly to the queue_messages table
        in the same transaction, so the queue parameter is ignored.

        Args:
            queue: Ignored (for API compatibility with base class)

        Usage:
            with store.transaction() as txn:
                txn.store_stage(stage)
                txn.push_message(message)
            # Auto-commits on success, rolls back on exception

        Yields:
            AtomicTransaction with store_stage() and push_message() methods
        """
        from stabilize.persistence.sqlite.transaction import AtomicTransaction

        conn = self._get_connection()
        txn = AtomicTransaction(conn, self)
        try:
            yield txn
            conn.commit()
        except Exception:
            conn.rollback()
            # Restore in-memory versions to match rolled-back database state
            txn.rollback_versions()
            raise

    # ========== Message Deduplication ==========

    def is_message_processed(self, message_id: str) -> bool:
        """Check if a message has already been processed."""
        return _is_message_processed(self._get_connection(), message_id)

    def mark_message_processed(
        self,
        message_id: str,
        handler_type: str | None = None,
        execution_id: str | None = None,
    ) -> None:
        """Mark a message as successfully processed."""
        _mark_message_processed(self._get_connection(), message_id, handler_type, execution_id)

    def cleanup_old_processed_messages(self, max_age_hours: float = 24.0) -> int:
        """Clean up old processed message records."""
        return _cleanup_old_processed_messages(self._get_connection(), max_age_hours)
