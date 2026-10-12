"""Atomic transaction support for PostgreSQL persistence."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from stabilize.persistence.store import StoreTransaction
from stabilize.persistence.task_state import Captured, capture, commit_captured, versions

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.task import TaskExecution
    from stabilize.models.workflow import Workflow
    from stabilize.queue import Queue
    from stabilize.queue.messages import Message


ADMISSION_LOCK_CLASS = 0x5354424C


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
        self._marked_ids: list[str] = []
        self._written_tasks: Captured = []

    def on_commit(self) -> None:
        """Record the message ids this transaction marked processed, once it has committed."""
        from stabilize.persistence.committed_marks import record_committed

        record_committed(self._marked_ids)
        commit_captured(self._written_tasks)
        self._written_tasks = []

    def rollback_versions(self) -> None:
        """Restore original versions on rollback.

        Called by the transaction context manager when rolling back to ensure
        in-memory stage/task versions match the database state.
        """
        for obj, original_version in self._staged_objects:
            obj.version = original_version
        self._staged_objects.clear()
        self._written_tasks = []

    def store_stage(self, stage: StageExecution, expected_phase: str | None = None) -> None:
        """Store or update a stage within the transaction.

        Args:
            stage: Stage to store
            expected_phase: If provided, passed to store.store_stage() for CAS.
        """
        original_version = stage.version
        task_versions = versions(stage.tasks)
        self._store.store_stage(stage, expected_phase=expected_phase, connection=self._conn)
        self._staged_objects.append((stage, original_version))
        self._staged_objects.extend(task_versions)
        self._written_tasks.extend(capture(stage.tasks))

    def set_config_version(self, workflow_id: str, config_version: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                "UPDATE pipeline_executions SET config_version = %(v)s WHERE id = %(id)s",
                {"v": config_version, "id": workflow_id},
            )

    def update_workflow_status(self, workflow: Workflow) -> None:
        """Update workflow status within the transaction."""
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline_executions SET
                    status = %(status)s,
                    start_time = %(start_time)s,
                    end_time = %(end_time)s,
                    queue_table = COALESCE(%(queue_table)s, queue_table)
                WHERE id = %(id)s
                """,
                {
                    "id": workflow.id,
                    "status": workflow.status.name,
                    "start_time": workflow.start_time,
                    "end_time": workflow.end_time,
                    "queue_table": workflow.queue_table,
                },
            )

    def push_message(self, message: Message, delay: float = 0) -> None:
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
    ) -> bool:
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
            inserted = bool(cur.rowcount == 1)
        if inserted:
            self._marked_ids.append(message_id)
        return inserted

    def acquire_claim(
        self,
        execution_id: str,
        claim_key: str,
        stage_id: str,
        steal_if_owner_terminal: bool = False,
    ) -> bool:
        """Atomically acquire a named claim within the transaction.

        See StoreTransaction.acquire_claim for semantics. Under READ COMMITTED
        a concurrent inserter blocks on the unique index until the winner
        commits, after which the conflict resolves and the loser observes the
        committed owner.
        """
        params = {
            "execution_id": execution_id,
            "claim_key": claim_key,
            "stage_id": stage_id,
        }
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stage_claims (execution_id, claim_key, stage_id)
                VALUES (%(execution_id)s, %(claim_key)s, %(stage_id)s)
                ON CONFLICT (execution_id, claim_key) DO NOTHING
                """,
                params,
            )
            if cur.rowcount == 1:
                return True

            cur.execute(
                """
                SELECT stage_id FROM stage_claims
                WHERE execution_id = %(execution_id)s AND claim_key = %(claim_key)s
                """,
                params,
            )
            row = cur.fetchone()
            if row is None:
                cur.execute(
                    """
                    INSERT INTO stage_claims (execution_id, claim_key, stage_id)
                    VALUES (%(execution_id)s, %(claim_key)s, %(stage_id)s)
                    ON CONFLICT (execution_id, claim_key) DO NOTHING
                    """,
                    params,
                )
                return bool(cur.rowcount == 1)

            # The pool uses dict_row, so rows are dict-like: access by name.
            owner_id = row["stage_id"]
            if owner_id == stage_id:
                return True

            if steal_if_owner_terminal:
                from stabilize.models.status import WorkflowStatus

                cur.execute(
                    "SELECT status FROM stage_executions WHERE id = %(id)s",
                    {"id": owner_id},
                )
                owner_row = cur.fetchone()
                owner_gone = owner_row is None
                owner_terminal = owner_row is not None and WorkflowStatus[owner_row["status"]].is_complete
                if owner_gone or owner_terminal:
                    cur.execute(
                        """
                        UPDATE stage_claims
                        SET stage_id = %(stage_id)s, claimed_at = NOW()
                        WHERE execution_id = %(execution_id)s
                          AND claim_key = %(claim_key)s
                          AND stage_id = %(owner_id)s
                        """,
                        {**params, "owner_id": owner_id},
                    )
                    return bool(cur.rowcount == 1)

            return False

    def lock_admission(self, key: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute("SET LOCAL lock_timeout = '30s'")
            cur.execute(
                "SELECT pg_advisory_xact_lock(%(class_id)s, hashtext(%(key)s))",
                {"class_id": ADMISSION_LOCK_CLASS, "key": key},
            )

    def count_running(self, pipeline_config_id: str) -> int:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) AS n FROM pipeline_executions "
                "WHERE pipeline_config_id = %(c)s AND status = 'RUNNING'",
                {"c": pipeline_config_id},
            )
            row = cur.fetchone()
        return int(row["n"] if isinstance(row, dict) else row[0])

    def buffered_ids(self, pipeline_config_id: str) -> list[str]:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM pipeline_executions "
                "WHERE pipeline_config_id = %(c)s AND status = 'BUFFERED' ORDER BY id",
                {"c": pipeline_config_id},
            )
            rows = cur.fetchall()
        return [r["id"] if isinstance(r, dict) else r[0] for r in rows]

    def workflow_status(self, workflow_id: str) -> tuple[str | None, bool]:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT status, is_canceled FROM pipeline_executions WHERE id = %(id)s", {"id": workflow_id}
            )
            row = cur.fetchone()
        if row is None:
            return None, False
        if isinstance(row, dict):
            return str(row["status"]), bool(row["is_canceled"])
        return str(row[0]), bool(row[1])


# Forward reference for type hint
from stabilize.persistence.postgres.store import (  # noqa: E402, F401
    PostgresWorkflowStore,
)
