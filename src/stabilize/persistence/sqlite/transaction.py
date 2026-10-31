"""Atomic transaction support for SQLite persistence."""

from __future__ import annotations

import json
import os
import sqlite3
import uuid
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import TYPE_CHECKING, Any

from stabilize.errors import ConcurrencyError
from stabilize.persistence.sqlite.helpers import has_execution_column, has_queue_table_column, insert_stage, upsert_task
from stabilize.persistence.store import StoreTransaction
from stabilize.persistence.task_state import Captured, capture, commit_captured
from stabilize.persistence.tenancy import refuse_foreign_row

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.task import TaskExecution
    from stabilize.models.workflow import Workflow
    from stabilize.queue.messages import Message


_QUEUE_TABLE_SET = ",\n                queue_table = COALESCE(:queue_table, queue_table)"


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

    def __init__(self, conn: sqlite3.Connection, store: SqliteWorkflowStore, queue: Any = None) -> None:
        """Initialize atomic transaction.

        Args:
            conn: SQLite connection (will not auto-commit)
            store: Parent store for helper methods
            queue: The queue whose table push_message writes to (queue_messages when None)
        """
        self._conn = conn
        self._store = store
        self._tenant = store.tenant
        self._queue_table = _queue_table_for(store, queue)
        # Track stage objects and their original versions for rollback
        self._staged_objects: list[tuple[StageExecution | TaskExecution, int]] = []
        self._written_tasks: Captured = []

    def on_commit(self) -> None:
        """Record the task rows this transaction wrote, once it has committed."""
        commit_captured(self._written_tasks)
        self._written_tasks = []

    def rollback_versions(self) -> None:
        """Restore original versions on rollback.

        Called by the transaction context manager when rolling back to ensure
        in-memory stage versions match the database state.
        """
        for stage, original_version in self._staged_objects:
            stage.version = original_version
        self._staged_objects.clear()
        self._written_tasks = []

    def store_stage(self, stage: StageExecution, expected_phase: str | None = None) -> None:
        """Store or update a stage within the transaction.

        This performs the same operations as SqliteWorkflowStore.store_stage()
        but does NOT commit - the commit happens when the transaction
        context manager exits successfully.

        Args:
            stage: Stage to store/update
            expected_phase: If provided, adds status check to WHERE clause for
                           phase-aware optimistic locking (CAS pattern).
        """
        # Check if stage exists
        result = self._conn.execute(
            "SELECT stabilize_tenant FROM stage_executions WHERE id = :id",
            {"id": stage.id},
        )
        held = result.fetchone()
        if held is not None:
            refuse_foreign_row("stage_executions", stage.id, held[0], self._tenant)
        exists = held is not None

        if exists:
            # Update with optimistic locking
            if expected_phase is not None:
                cursor = self._conn.execute(
                    """
                    UPDATE stage_executions SET
                        status = :status,
                        context = :context,
                        outputs = :outputs,
                        start_time = :start_time,
                        end_time = :end_time,
                        version = version + 1
                    WHERE id = :id AND version = :version AND status = :expected_phase AND stabilize_tenant = :tenant
                    """,
                    {
                        "id": stage.id,
                        "tenant": self._tenant,
                        "status": stage.status.name,
                        "context": json.dumps(stage.context, default=str),
                        "outputs": json.dumps(stage.outputs, default=str),
                        "start_time": stage.start_time,
                        "end_time": stage.end_time,
                        "version": stage.version,
                        "expected_phase": expected_phase,
                    },
                )
            else:
                cursor = self._conn.execute(
                    """
                    UPDATE stage_executions SET
                        status = :status,
                        context = :context,
                        outputs = :outputs,
                        start_time = :start_time,
                        end_time = :end_time,
                        version = version + 1
                    WHERE id = :id AND version = :version AND stabilize_tenant = :tenant
                    """,
                    {
                        "id": stage.id,
                        "tenant": self._tenant,
                        "status": stage.status.name,
                        "context": json.dumps(stage.context, default=str),
                        "outputs": json.dumps(stage.outputs, default=str),
                        "start_time": stage.start_time,
                        "end_time": stage.end_time,
                        "version": stage.version,
                    },
                )

            if cursor.rowcount == 0:
                if expected_phase is not None:
                    raise ConcurrencyError(
                        f"Optimistic lock failed for stage {stage.id} "
                        f"(version {stage.version}, expected_phase {expected_phase})"
                    )
                raise ConcurrencyError(f"Optimistic lock failed for stage {stage.id} (version {stage.version})")

            # Track original version for rollback before incrementing
            self._staged_objects.append((stage, stage.version))

            # Update local version to reflect change
            stage.version += 1

            # Update tasks
            for task in stage.tasks:
                # Track original version for rollback
                self._staged_objects.append((task, task.version))
                upsert_task(self._conn, task, stage.id, self._tenant, only_changed=True)
        else:
            insert_stage(self._conn, stage, stage.execution.id, self._tenant)
        self._written_tasks.extend(capture(stage.tasks))

    def set_config_version(self, workflow_id: str, config_version: str) -> None:
        if not has_execution_column(self._conn, "config_version"):
            return
        self._conn.execute(
            "UPDATE pipeline_executions SET config_version = :v WHERE id = :id AND stabilize_tenant = :tenant",
            {"v": config_version, "id": workflow_id, "tenant": self._tenant},
        )

    def update_workflow_status(self, workflow: Workflow) -> None:
        """Update workflow status within the transaction.

        This performs the same operations as SqliteWorkflowStore.update_status()
        but does NOT commit - the commit happens when the transaction
        context manager exits successfully.

        Args:
            workflow: Workflow with updated status
        """
        self._conn.execute(
            f"""
            UPDATE pipeline_executions SET
                status = :status,
                start_time = :start_time,
                end_time = :end_time{_QUEUE_TABLE_SET if has_queue_table_column(self._conn) else ""}
            WHERE id = :id AND stabilize_tenant = :tenant
            """,
            {
                "id": workflow.id,
                "tenant": self._tenant,
                "status": workflow.status.name,
                "start_time": workflow.start_time,
                "end_time": workflow.end_time,
                "queue_table": workflow.queue_table,
            },
        )

    def push_message(
        self,
        message: Message,
        delay: float = 0,
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
            f"""
            INSERT INTO {self._queue_table} (
                message_id, message_type, payload, deliver_at,
                attempts, max_attempts, version, stabilize_tenant
            ) VALUES (
                :message_id, :message_type, :payload, :deliver_at,
                0, :max_attempts, 0, :tenant
            )
            """,
            {
                "message_id": message_id,
                "message_type": message_type,
                "payload": payload,
                "deliver_at": deliver_at.isoformat(),
                "max_attempts": getattr(message, "max_attempts", 10),
                "tenant": self._tenant,
            },
        )

    def mark_message_processed(
        self,
        message_id: str,
        handler_type: str | None = None,
        execution_id: str | None = None,
    ) -> bool:
        """Mark a message as successfully processed within the transaction."""
        cursor = self._conn.execute(
            """
            INSERT OR IGNORE INTO processed_messages (
                message_id, processed_at, handler_type, execution_id, stabilize_tenant
            ) VALUES (
                :message_id, :processed_at, :handler_type, :execution_id, :tenant
            )
            """,
            {
                "message_id": message_id,
                "processed_at": datetime.now(UTC).isoformat(),
                "handler_type": handler_type,
                "execution_id": execution_id,
                "tenant": self._tenant,
            },
        )
        if cursor.rowcount == 0:
            held = self._conn.execute(
                "SELECT stabilize_tenant FROM processed_messages WHERE message_id = :message_id",
                {"message_id": message_id},
            ).fetchone()
            refuse_foreign_row("processed_messages", message_id, held[0] if held else None, self._tenant)
        return bool(cursor.rowcount == 1)

    def acquire_claim(
        self,
        execution_id: str,
        claim_key: str,
        stage_id: str,
        steal_if_owner_terminal: bool = False,
    ) -> bool:
        """Atomically acquire a named claim within the transaction.

        See StoreTransaction.acquire_claim for semantics.
        """
        params = {
            "execution_id": execution_id,
            "claim_key": claim_key,
            "stage_id": stage_id,
            "tenant": self._tenant,
        }
        cursor = self._conn.execute(
            """
            INSERT OR IGNORE INTO stage_claims (execution_id, claim_key, stage_id, stabilize_tenant)
            VALUES (:execution_id, :claim_key, :stage_id, :tenant)
            """,
            params,
        )
        if cursor.rowcount == 1:
            return True

        row = self._conn.execute(
            "SELECT stage_id, stabilize_tenant FROM stage_claims "
            "WHERE execution_id = :execution_id AND claim_key = :claim_key",
            params,
        ).fetchone()
        if row is not None:
            refuse_foreign_row("stage_claims", f"{execution_id}/{claim_key}", row[1], self._tenant)
        if row is None:
            # Claim row vanished between insert and select; retry the insert.
            cursor = self._conn.execute(
                """
                INSERT OR IGNORE INTO stage_claims (execution_id, claim_key, stage_id, stabilize_tenant)
                VALUES (:execution_id, :claim_key, :stage_id, :tenant)
                """,
                params,
            )
            return cursor.rowcount == 1

        owner_id = row[0]
        if owner_id == stage_id:
            return True

        if steal_if_owner_terminal:
            from stabilize.models.status import WorkflowStatus

            owner_row = self._conn.execute(
                "SELECT status FROM stage_executions WHERE id = :id AND stabilize_tenant = :tenant",
                {"id": owner_id, "tenant": self._tenant},
            ).fetchone()
            owner_gone = owner_row is None
            owner_terminal = owner_row is not None and WorkflowStatus[owner_row[0]].is_complete
            if owner_gone or owner_terminal:
                cursor = self._conn.execute(
                    """
                    UPDATE stage_claims
                    SET stage_id = :stage_id, claimed_at = datetime('now', 'utc')
                    WHERE execution_id = :execution_id
                      AND claim_key = :claim_key
                      AND stage_id = :owner_id AND stabilize_tenant = :tenant
                    """,
                    {**params, "owner_id": owner_id},
                )
                return cursor.rowcount == 1

        return False

    def lock_admission(self, key: str) -> None:
        if self._conn.in_transaction:
            raise RuntimeError("lock_admission must be the first statement of its transaction")
        self._conn.execute("BEGIN IMMEDIATE")

    def count_running(self, pipeline_config_id: str) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) FROM pipeline_executions "
            "WHERE pipeline_config_id = ? AND status = 'RUNNING' AND stabilize_tenant = ?",
            (pipeline_config_id, self._tenant),
        ).fetchone()
        return int(row[0])

    def buffered_ids(self, pipeline_config_id: str) -> list[str]:
        rows = self._conn.execute(
            "SELECT id FROM pipeline_executions "
            "WHERE pipeline_config_id = ? AND status = 'BUFFERED' AND stabilize_tenant = ? ORDER BY id",
            (pipeline_config_id, self._tenant),
        ).fetchall()
        return [r[0] for r in rows]

    def workflow_status(self, workflow_id: str) -> tuple[str | None, bool]:
        row = self._conn.execute(
            "SELECT status, is_canceled FROM pipeline_executions WHERE id = ? AND stabilize_tenant = ?",
            (workflow_id, self._tenant),
        ).fetchone()
        return (None, False) if row is None else (str(row[0]), bool(row[1]))

    def paused_stage_ids(self, workflow_id: str) -> list[str]:
        rows = self._conn.execute(
            "SELECT id FROM stage_executions "
            "WHERE execution_id = ? AND status = 'PAUSED' AND stabilize_tenant = ? ORDER BY id",
            (workflow_id, self._tenant),
        ).fetchall()
        return [str(r[0]) for r in rows]


def _queue_table_for(store: Any, queue: Any) -> str:
    if queue is None:
        return "queue_messages"
    from stabilize.persistence.connection import get_connection_manager

    queue_cs = getattr(queue, "connection_string", None)
    if queue_cs is not None:
        parse = get_connection_manager()._parse_sqlite_path

        def location(cs: str) -> str:
            path = parse(cs).split("?", 1)[0]
            return path if path.startswith(":memory:") or path.startswith("file:") else os.path.abspath(path)

        if location(queue_cs) != location(store.connection_string):
            raise ValueError(
                "store.transaction(queue): the queue's SQLite database differs from the store's; "
                "a push cannot be atomic with the store write"
            )
    return str(getattr(queue, "table_name", None) or "queue_messages")


# Forward reference for type hint
from stabilize.persistence.sqlite.store import SqliteWorkflowStore  # noqa: E402, F401
