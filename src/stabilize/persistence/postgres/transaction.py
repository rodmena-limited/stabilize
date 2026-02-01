"""Atomic transaction support for PostgreSQL persistence."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from stabilize.persistence.store import StoreTransaction

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.task import TaskExecution
    from stabilize.models.workflow import Workflow
    from stabilize.queue import Queue
    from stabilize.queue.messages import Message


class PostgresTransaction(StoreTransaction):
    """Atomic transaction spanning store and queue operations for PostgreSQL.

    This transaction ensures true database-level atomicity for all operations.
    All store operations and message pushes within this transaction will either
    all succeed (on commit) or all fail (on rollback).
    """

    @property
    def is_atomic(self) -> bool:
        """PostgreSQL transactions provide true database-level atomicity."""
        return True

    def __init__(self, conn: Any, store: PostgresWorkflowStore, queue: Queue | None) -> None:
        """Initialize atomic transaction.

        Args:
            conn: PostgreSQL connection for the transaction
            store: The PostgresWorkflowStore for store operations
            queue: The queue for message operations. If None, push_message()
                   will raise RuntimeError if called.
        """
        self._conn = conn
        self._store = store
        self._queue = queue
        # Track stage/task objects and their original versions for rollback
        self._staged_objects: list[tuple[StageExecution | TaskExecution, int]] = []

    def rollback_versions(self) -> None:
        """Restore original versions on rollback.

        Called by the transaction context manager when rolling back to ensure
        in-memory stage/task versions match the database state.
        """
        for obj, original_version in self._staged_objects:
            obj.version = original_version
        self._staged_objects.clear()

    def store_stage(self, stage: StageExecution) -> None:
        """Store or update a stage within the transaction."""
        # Track original version before store (which may increment it)
        original_version = stage.version
        self._store.store_stage(stage, connection=self._conn)
        # Track for potential rollback (store after because store_stage increments version)
        self._staged_objects.append((stage, original_version))
        # Also track tasks that were updated
        for task in stage.tasks:
            if task.version != 0:  # Only track tasks that were updated
                self._staged_objects.append((task, task.version - 1))

    def update_workflow_status(self, workflow: Workflow) -> None:
        """Update workflow status within the transaction."""
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline_executions SET
                    status = %(status)s,
                    start_time = %(start_time)s,
                    end_time = %(end_time)s
                WHERE id = %(id)s
                """,
                {
                    "id": workflow.id,
                    "status": workflow.status.name,
                    "start_time": workflow.start_time,
                    "end_time": workflow.end_time,
                },
            )

    def push_message(self, message: Message, delay: int = 0) -> None:
        """Push a message to the queue within the transaction.

        Raises:
            RuntimeError: If no queue was configured for this transaction.
        """
        if self._queue is None:
            raise RuntimeError(
                "Cannot push message: no queue configured for this transaction. "
                "Pass a queue to repository.transaction(queue) for message operations."
            )
        from datetime import timedelta

        # Use the connection to push, ensuring atomicity
        self._queue.push(message, delay=timedelta(seconds=delay) if delay else None, connection=self._conn)

    def mark_message_processed(
        self,
        message_id: str,
        handler_type: str | None = None,
        execution_id: str | None = None,
    ) -> None:
        """Mark a message as successfully processed within the transaction."""
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO processed_messages (
                    message_id, processed_at, handler_type, execution_id
                ) VALUES (
                    %(message_id)s, NOW(), %(handler_type)s, %(execution_id)s
                )
                ON CONFLICT (message_id) DO NOTHING
                """,
                {
                    "message_id": message_id,
                    "handler_type": handler_type,
                    "execution_id": execution_id,
                },
            )


# Forward reference for type hint
from stabilize.persistence.postgres.store import PostgresWorkflowStore  # noqa: E402, F401
