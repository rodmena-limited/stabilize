"""Atomic transaction support for SQLite persistence."""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import TYPE_CHECKING

from stabilize.errors import ConcurrencyError
from stabilize.persistence.sqlite.helpers import insert_stage, upsert_task
from stabilize.persistence.store import StoreTransaction

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.task import TaskExecution
    from stabilize.models.workflow import Workflow
    from stabilize.queue.messages import Message


class AtomicTransaction(StoreTransaction):
    """Atomic transaction spanning store and queue operations.

    This class ensures that stage updates and message pushes happen
    atomically. If either operation fails, both are rolled back.

    This solves the critical issue where:
    1. store_stage() succeeds
    2. push_message() fails (or crash)
    3. Workflow is stuck forever (stage saved but no message to continue)

    With AtomicTransaction, both operations use the same SQLite connection
    and commit only when both succeed.
    """

    @property
    def is_atomic(self) -> bool:
        """SQLite transactions provide true database-level atomicity."""
        return True

    def __init__(self, conn: sqlite3.Connection, store: SqliteWorkflowStore) -> None:
        """Initialize atomic transaction.

        Args:
            conn: SQLite connection (will not auto-commit)
            store: Parent store for helper methods
        """
        self._conn = conn
        self._store = store
        # Track stage objects and their original versions for rollback
        self._staged_objects: list[tuple[StageExecution | TaskExecution, int]] = []

    def rollback_versions(self) -> None:
        """Restore original versions on rollback.

        Called by the transaction context manager when rolling back to ensure
        in-memory stage versions match the database state.
        """
        for stage, original_version in self._staged_objects:
            stage.version = original_version
        self._staged_objects.clear()

    def store_stage(self, stage: StageExecution) -> None:
        """Store or update a stage within the transaction.

        This performs the same operations as SqliteWorkflowStore.store_stage()
        but does NOT commit - the commit happens when the transaction
        context manager exits successfully.

        Args:
            stage: Stage to store/update
        """
        # Check if stage exists
        result = self._conn.execute(
            "SELECT id FROM stage_executions WHERE id = :id",
            {"id": stage.id},
        )
        exists = result.fetchone() is not None

        if exists:
            # Update with optimistic locking
            cursor = self._conn.execute(
                """
                UPDATE stage_executions SET
                    status = :status,
                    context = :context,
                    outputs = :outputs,
                    start_time = :start_time,
                    end_time = :end_time,
                    version = version + 1
                WHERE id = :id AND version = :version
                """,
                {
                    "id": stage.id,
                    "status": stage.status.name,
                    "context": json.dumps(stage.context),
                    "outputs": json.dumps(stage.outputs),
                    "start_time": stage.start_time,
                    "end_time": stage.end_time,
                    "version": stage.version,
                },
            )

            if cursor.rowcount == 0:
                raise ConcurrencyError(f"Optimistic lock failed for stage {stage.id} (version {stage.version})")

            # Track original version for rollback before incrementing
            self._staged_objects.append((stage, stage.version))

            # Update local version to reflect change
            stage.version += 1

            # Update tasks
            for task in stage.tasks:
                # Track original version for rollback
                self._staged_objects.append((task, task.version))
                upsert_task(self._conn, task, stage.id)
        else:
            insert_stage(self._conn, stage, stage.execution.id)

    def update_workflow_status(self, workflow: Workflow) -> None:
        """Update workflow status within the transaction.

        This performs the same operations as SqliteWorkflowStore.update_status()
        but does NOT commit - the commit happens when the transaction
        context manager exits successfully.

        Args:
            workflow: Workflow with updated status
        """
        self._conn.execute(
            """
            UPDATE pipeline_executions SET
                status = :status,
                start_time = :start_time,
                end_time = :end_time
            WHERE id = :id
            """,
            {
                "id": workflow.id,
                "status": workflow.status.name,
                "start_time": workflow.start_time,
                "end_time": workflow.end_time,
            },
        )

    def push_message(
        self,
        message: Message,
        delay: int = 0,
    ) -> None:
        """Push a message to the queue within the transaction.

        This inserts directly into queue_messages table using the same
        connection as store_stage(), ensuring atomicity.

        Args:
            message: Message to push
            delay: Delay in seconds before message is deliverable
        """
        from stabilize.queue.messages import get_message_type_name

        deliver_at = datetime.now(UTC)
        if delay > 0:
            deliver_at = deliver_at + timedelta(seconds=delay)

        # Generate message_id if not present
        message_id = getattr(message, "id", None) or str(uuid.uuid4())

        # Get message type name using the helper function
        message_type = get_message_type_name(message)

        # Serialize message (same logic as SqliteQueue._serialize_message)
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
        payload = json.dumps(data)

        self._conn.execute(
            """
            INSERT INTO queue_messages (
                message_id, message_type, payload, deliver_at,
                attempts, max_attempts, version
            ) VALUES (
                :message_id, :message_type, :payload, :deliver_at,
                0, :max_attempts, 0
            )
            """,
            {
                "message_id": message_id,
                "message_type": message_type,
                "payload": payload,
                "deliver_at": deliver_at.isoformat(),
                "max_attempts": getattr(message, "max_attempts", 10),
            },
        )

    def mark_message_processed(
        self,
        message_id: str,
        handler_type: str | None = None,
        execution_id: str | None = None,
    ) -> None:
        """Mark a message as successfully processed within the transaction."""
        self._conn.execute(
            """
            INSERT OR IGNORE INTO processed_messages (
                message_id, processed_at, handler_type, execution_id
            ) VALUES (
                :message_id, datetime('now', 'utc'), :handler_type, :execution_id
            )
            """,
            {
                "message_id": message_id,
                "handler_type": handler_type,
                "execution_id": execution_id,
            },
        )


# Forward reference for type hint
from stabilize.persistence.sqlite.store import SqliteWorkflowStore  # noqa: E402, F401
