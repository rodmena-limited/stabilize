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

    def store_stage(self, stage: StageExecution, expected_phase: str | None = None) -> None:
        """Store or update a stage within the transaction.

        Args:
            stage: Stage to store
            expected_phase: If provided, passed to store.store_stage() for CAS.
        """
        # Track original version before store (which may increment it)
        original_version = stage.version
        self._store.store_stage(stage, expected_phase=expected_phase, connection=self._conn)
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
                return cur.rowcount == 1

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
                    return cur.rowcount == 1

            return False


# Forward reference for type hint
from stabilize.persistence.postgres.store import (  # noqa: E402, F401
    PostgresWorkflowStore,
)
