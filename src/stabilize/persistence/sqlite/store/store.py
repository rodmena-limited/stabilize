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
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from stabilize.persistence.schema_check import check_sqlite_tables
from stabilize.persistence.sqlite.operations import (
    cleanup_old_processed_messages as _cleanup_old_processed_messages,
)
from stabilize.persistence.sqlite.operations import (
    cleanup_processed_not_in_queue as _cleanup_processed_not_in_queue,
)
from stabilize.persistence.sqlite.operations import (
    get_processed_message_ids as _get_processed_message_ids,
)
from stabilize.persistence.sqlite.operations import is_message_processed as _is_message_processed
from stabilize.persistence.sqlite.operations import (
    mark_message_processed as _mark_message_processed,
)
from stabilize.persistence.sqlite.schema import create_tables
from stabilize.persistence.sqlite.store.queries import SqliteQueriesMixin
from stabilize.persistence.sqlite.store.stage_ops import SqliteStageOpsMixin
from stabilize.persistence.sqlite.store.stage_query import SqliteStageQueryMixin
from stabilize.persistence.sqlite.store.workflow_crud import SqliteWorkflowCrudMixin
from stabilize.persistence.sqlite.tenant_schema import engine_shaped_tables, ensure_tenant_schema, read_mode
from stabilize.persistence.store import WorkflowStore
from stabilize.persistence.tenancy import check_mode_admits, require_same_tenant, validate_tenant
from stabilize.queue.ownership import DEFAULT_TABLE

if TYPE_CHECKING:
    from stabilize.persistence.store import StoreTransaction
    from stabilize.queue import Queue

logger = logging.getLogger(__name__)


class SqliteWorkflowStore(
    SqliteWorkflowCrudMixin,
    SqliteStageOpsMixin,
    SqliteQueriesMixin,
    SqliteStageQueryMixin,
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
        tenant: str = "",
    ) -> None:
        """
        Initialize the repository.

        Args:
            connection_string: SQLite connection string (e.g., sqlite:///./db.sqlite)
            create_tables: Whether to create tables if they don't exist
        """
        from stabilize.persistence.connection import get_connection_manager

        self.tenant = validate_tenant(tenant)
        self.connection_string = connection_string
        self._manager = get_connection_manager()

        conn = self._get_connection()
        conn.execute("SELECT 1")

        if create_tables:
            self._create_tables()
        if _has_engine_tables(conn):
            ensure_tenant_schema(conn)
            check_mode_admits(read_mode(conn), self.tenant)
            check_sqlite_tables(conn, engine_shaped_tables(conn), read_mode(conn))
        self._manager.bind_sqlite_tenant(connection_string, self.tenant)

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

        SQLite implementation writes pushed messages to queue.table_name
        (queue_messages when queue is None) on the store's own connection,
        in the same transaction; a queue on a different database file is refused.

        Args:
            queue: The queue whose table receives pushed messages

        Usage:
            with store.transaction() as txn:
                txn.store_stage(stage)
                txn.push_message(message)
            # Auto-commits on success, rolls back on exception

        Yields:
            AtomicTransaction with store_stage() and push_message() methods
        """
        from stabilize.events.txn_scope import (
            abort_store_transaction,
            begin_store_transaction,
            commit_store_transaction,
        )
        from stabilize.persistence.sqlite.transaction import AtomicTransaction

        require_same_tenant(store=self, queue=queue)
        conn = self._get_connection()
        txn = AtomicTransaction(conn, self, queue)
        # Bind a thread-local scope so event recording inside this block
        # joins the transaction (same-database event stores) and bus
        # publication is deferred until after commit.
        begin_store_transaction(conn, self.connection_string)
        try:
            yield txn
            conn.commit()
            txn.on_commit()
        except Exception:
            conn.rollback()
            # Restore in-memory versions to match rolled-back database state
            txn.rollback_versions()
            abort_store_transaction()
            raise
        commit_store_transaction()

    # ========== Message Deduplication ==========

    def count_active_workflows(self) -> int:
        row = (
            self._get_connection()
            .execute(
                "SELECT count(*) FROM pipeline_executions WHERE status = 'RUNNING' AND stabilize_tenant = :tenant",
                {"tenant": self.tenant},
            )
            .fetchone()
        )
        return int(row[0]) if row else 0

    def is_message_processed(self, message_id: str) -> bool:
        """Check if a message has already been processed."""
        return _is_message_processed(self._get_connection(), message_id, self.tenant)

    def mark_message_processed(
        self,
        message_id: str,
        handler_type: str | None = None,
        execution_id: str | None = None,
    ) -> None:
        """Mark a message as successfully processed."""
        _mark_message_processed(self._get_connection(), message_id, self.tenant, handler_type, execution_id)

    def claim_message(
        self,
        message_id: str,
        handler_type: str | None = None,
        execution_id: str | None = None,
    ) -> bool:
        from stabilize.persistence.sqlite.operations import claim_message as _claim_message

        return _claim_message(self._get_connection(), message_id, self.tenant, handler_type, execution_id)

    def cleanup_old_processed_messages(
        self,
        max_age_hours: float = 24.0,
        batch_size: int | None = None,
        max_rows: int | None = None,
        table_name: str | None = None,
        live: Callable[[list[str]], set[str]] | None = None,
    ) -> int:
        """Delete processed-message records older than max_age_hours, in batches; returns the count.

        With live, only records of table_name's messages are considered, and a record is kept while
        live() reports its message still in the queue.
        """
        if live is not None:
            return _cleanup_processed_not_in_queue(
                self._get_connection(),
                self.tenant,
                max_age_hours,
                batch_size,
                max_rows,
                table_name or DEFAULT_TABLE,
                live,
            )
        return _cleanup_old_processed_messages(self._get_connection(), self.tenant, max_age_hours, batch_size, max_rows)

    def completed_workflow_ids(self, completed_before_ms: int, limit: int, application: str | None = None) -> list[str]:
        from stabilize.maintenance import PURGEABLE

        marks = ", ".join("?" for _ in PURGEABLE)
        sql = (
            f"SELECT id FROM pipeline_executions WHERE +status IN ({marks}) "
            "AND end_time IS NOT NULL AND end_time < ? AND stabilize_tenant = ?"
        )
        params: list[Any] = [*PURGEABLE, completed_before_ms, self.tenant]
        if application is not None:
            sql += " AND application = ?"
            params.append(application)
        sql += " ORDER BY end_time LIMIT ?"
        params.append(limit)
        return [r[0] for r in self._get_connection().execute(sql, params).fetchall()]

    def cleanup_completed_stage_claims(self) -> int:
        """Delete stage claims of executions in terminal states."""
        from stabilize.persistence.sqlite.operations import (
            cleanup_completed_stage_claims as _cleanup_claims,
        )

        return _cleanup_claims(self._get_connection(), self.tenant)

    def cleanup_buffered_signals(
        self,
        only_complete: bool = True,
        statuses: list[str] | None = None,
    ) -> int:
        """Strip unconsumable WCP-24 signal buffers from stage contexts."""
        from stabilize.persistence.sqlite.operations import (
            cleanup_buffered_signals as _cleanup_signals,
        )

        return _cleanup_signals(self._get_connection(), self.tenant, only_complete, statuses)

    def count_buffered_signal_stages(
        self,
        only_complete: bool = True,
        statuses: list[str] | None = None,
    ) -> int:
        """Count stage rows carrying a WCP-24 signal buffer."""
        from stabilize.persistence.sqlite.operations import (
            count_buffered_signal_stages as _count_signals,
        )

        return _count_signals(self._get_connection(), self.tenant, only_complete, statuses)

    def get_processed_message_ids(self, limit: int | None = None) -> list[str] | None:
        """Return processed message IDs, for hydrating an in-memory dedup cache."""
        return _get_processed_message_ids(self._get_connection(), self.tenant, limit)

    def supports_signal_storage(self) -> bool:
        return True

    def buffer_signal(
        self,
        execution_id: str,
        stage_ref_id: str,
        signal_name: str,
        signal_data: dict[str, Any] | None = None,
        *,
        user: str = "",
        persistent: bool = True,
    ) -> int:
        from stabilize.persistence.sqlite.signals import buffer_signal as _buffer

        conn = self._get_connection()
        signal_id = _buffer(
            conn,
            execution_id,
            stage_ref_id,
            signal_name,
            signal_data,
            tenant=self.tenant,
            user=user,
            persistent=persistent,
        )
        conn.commit()
        return signal_id

    def consume_signal(
        self,
        execution_id: str,
        stage_ref_id: str,
        signal_name: str | None = None,
        *,
        row_id: int | None = None,
    ) -> dict[str, Any] | None:
        from stabilize.persistence.sqlite.signals import consume_signal as _consume

        conn = self._get_connection()
        signal = _consume(conn, execution_id, stage_ref_id, signal_name, tenant=self.tenant, row_id=row_id)
        conn.commit()
        if signal is None:
            return None
        return {"signal_name": signal.signal_name, "signal_data": signal.signal_data}

    def pending_signal_count(self, execution_id: str, stage_ref_id: str) -> int:
        from stabilize.persistence.sqlite.signals import get_pending_signals

        return len(get_pending_signals(self._get_connection(), execution_id, stage_ref_id, tenant=self.tenant))

    def pending_signals(self, execution_id: str, stage_ref_id: str) -> list[dict[str, Any]]:
        from stabilize.persistence.sqlite.signals import get_pending_signals

        rows = get_pending_signals(self._get_connection(), execution_id, stage_ref_id, tenant=self.tenant)
        return [
            {"id": int(r.id), "signal_name": r.signal_name, "signal_data": r.signal_data, "user": r.user} for r in rows
        ]

    def pending_signal_latest_ms(self, execution_id: str) -> int | None:
        """The creation time (epoch ms) of the newest unconsumed signal of a workflow, or None."""
        if not self.supports_signal_storage():
            return None
        conn = self._get_connection()
        row = conn.execute(
            "SELECT CAST(strftime('%s', max(created_at)) AS INTEGER) * 1000 FROM workflow_signals "
            "WHERE execution_id = :execution_id AND consumed = 0 AND stabilize_tenant = :tenant",
            {"execution_id": execution_id, "tenant": self.tenant},
        ).fetchone()
        return int(row[0]) if row and row[0] is not None else None

    def discard_signals(self, execution_id: str, stage_ref_id: str) -> int:
        conn = self._get_connection()
        cur = conn.execute(
            "DELETE FROM workflow_signals WHERE execution_id = :e AND stage_ref_id = :s AND consumed = 0 "
            "AND stabilize_tenant = :tenant",
            {"e": execution_id, "s": stage_ref_id, "tenant": self.tenant},
        )
        conn.commit()
        return cur.rowcount or 0


def _has_engine_tables(conn: sqlite3.Connection) -> bool:
    return bool(engine_shaped_tables(conn))
