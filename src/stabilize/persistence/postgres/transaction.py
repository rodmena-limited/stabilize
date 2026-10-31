"""Atomic transaction support for PostgreSQL persistence."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from stabilize.errors.delivery import DuplicateDeliveryError
from stabilize.persistence.store import StoreTransaction
from stabilize.persistence.task_state import Captured, capture, commit_captured, versions
from stabilize.persistence.tenancy import refuse_foreign_row

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
        self._tenant = store.tenant
        self._queue = queue
        # Track stage/task objects and their original versions for rollback
        self._staged_objects: list[tuple[StageExecution | TaskExecution, int]] = []
        self._marked_ids: list[str] = []
        self._pushed = False
        self._written_tasks: Captured = []

    def on_commit(self) -> None:
        """Record the message ids this transaction marked processed, once it has committed."""
        from stabilize.persistence.committed_marks import record_committed

        record_committed(self._marked_ids, pushed=self._pushed)
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

    def store_workflow(self, workflow: Workflow) -> None:
        from stabilize.persistence.postgres.helpers import insert_execution
        from stabilize.persistence.store.ids import check_ids

        check_ids(workflow, self._tenant)
        with self._conn.cursor() as cur:
            insert_execution(cur, workflow, self._tenant)

    def set_config_version(self, workflow_id: str, config_version: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                "UPDATE pipeline_executions SET config_version = %(v)s "
                "WHERE id = %(id)s AND stabilize_tenant = %(tenant)s",
                {"v": config_version, "id": workflow_id, "tenant": self._tenant},
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
                WHERE id = %(id)s AND stabilize_tenant = %(tenant)s
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

        self._pushed = True
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
                    message_id, processed_at, handler_type, execution_id, stabilize_tenant
                ) VALUES (
                    %(message_id)s, NOW(), %(handler_type)s, %(execution_id)s, %(tenant)s
                )
                ON CONFLICT (message_id) DO NOTHING
                """,
                {
                    "message_id": message_id,
                    "handler_type": handler_type,
                    "execution_id": execution_id,
                    "tenant": self._tenant,
                },
            )
            inserted = bool(cur.rowcount == 1)
            if not inserted:
                cur.execute(
                    "SELECT stabilize_tenant AS owner FROM processed_messages WHERE message_id = %(message_id)s",
                    {"message_id": message_id},
                )
                owner = cur.fetchone()
                refuse_foreign_row("processed_messages", message_id, owner["owner"] if owner else None, self._tenant)
                raise DuplicateDeliveryError(message_id)
        self._marked_ids.append(message_id)
        return True

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
            "tenant": self._tenant,
        }
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stage_claims (execution_id, claim_key, stage_id, stabilize_tenant)
                VALUES (%(execution_id)s, %(claim_key)s, %(stage_id)s, %(tenant)s)
                ON CONFLICT (execution_id, claim_key) DO NOTHING
                """,
                params,
            )
            if cur.rowcount == 1:
                return True

            cur.execute(
                """
                SELECT stage_id, stabilize_tenant AS owner FROM stage_claims
                WHERE execution_id = %(execution_id)s AND claim_key = %(claim_key)s
                """,
                params,
            )
            row = cur.fetchone()
            if row is None:
                cur.execute(
                    """
                    INSERT INTO stage_claims (execution_id, claim_key, stage_id, stabilize_tenant)
                    VALUES (%(execution_id)s, %(claim_key)s, %(stage_id)s, %(tenant)s)
                    ON CONFLICT (execution_id, claim_key) DO NOTHING
                    """,
                    params,
                )
                return bool(cur.rowcount == 1)

            refuse_foreign_row("stage_claims", f"{execution_id}/{claim_key}", row["owner"], self._tenant)
            owner_id = row["stage_id"]
            if owner_id == stage_id:
                return True

            if steal_if_owner_terminal:
                from stabilize.models.status import WorkflowStatus

                cur.execute(
                    "SELECT status FROM stage_executions WHERE id = %(id)s AND stabilize_tenant = %(tenant)s",
                    {"id": owner_id, "tenant": self._tenant},
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
                          AND stabilize_tenant = %(tenant)s
                        """,
                        {**params, "owner_id": owner_id},
                    )
                    return bool(cur.rowcount == 1)

            return False

    def _queue_table(self) -> str:
        return str(getattr(self._queue, "table_name", None) or "queue_messages")

    def purge_stage_messages(self, stage_id: str, *, except_message_id: str | None = None) -> int:
        keep = _queue_row_id(except_message_id)
        with self._conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {self._queue_table()} WHERE payload ->> 'stage_id' = %(stage_id)s "
                "AND stabilize_tenant = %(tenant)s AND (%(keep)s::bigint IS NULL OR id <> %(keep)s::bigint)",
                {"stage_id": stage_id, "tenant": self._tenant, "keep": keep},
            )
            return int(cur.rowcount or 0)

    def release_claims(self, execution_id: str, stage_id: str) -> int:
        with self._conn.cursor() as cur:
            cur.execute(
                "DELETE FROM stage_claims WHERE execution_id = %(execution_id)s AND stage_id = %(stage_id)s "
                "AND stabilize_tenant = %(tenant)s",
                {"execution_id": execution_id, "stage_id": stage_id, "tenant": self._tenant},
            )
            return int(cur.rowcount or 0)

    def count_claims(self, execution_id: str, prefix: str) -> int:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) AS n FROM stage_claims WHERE execution_id = %(execution_id)s "
                "AND left(claim_key, %(length)s) = %(prefix)s AND stabilize_tenant = %(tenant)s",
                {"execution_id": execution_id, "length": len(prefix), "prefix": prefix, "tenant": self._tenant},
            )
            row = cur.fetchone()
        return int(row["n"]) if row else 0

    def claim_owner(self, execution_id: str, claim_key: str) -> str | None:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT stage_id FROM stage_claims WHERE execution_id = %(execution_id)s "
                "AND claim_key = %(claim_key)s AND stabilize_tenant = %(tenant)s",
                {"execution_id": execution_id, "claim_key": claim_key, "tenant": self._tenant},
            )
            row = cur.fetchone()
        return str(row["stage_id"]) if row else None

    def remove_stage(self, stage_id: str) -> None:
        self.purge_stage_messages(stage_id)
        with self._conn.cursor() as cur:
            params = {"stage_id": stage_id, "tenant": self._tenant}
            cur.execute(
                "DELETE FROM task_executions WHERE stage_id = %(stage_id)s AND stabilize_tenant = %(tenant)s", params
            )
            cur.execute(
                "DELETE FROM stage_claims WHERE stage_id = %(stage_id)s AND stabilize_tenant = %(tenant)s", params
            )
            cur.execute(
                "DELETE FROM stage_executions WHERE id = %(stage_id)s AND stabilize_tenant = %(tenant)s", params
            )

    def try_lock_recovery(self, key: str) -> bool:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT pg_try_advisory_xact_lock(hashtextextended(%(key)s, 0)) AS taken",
                {"key": f"recovery\x1f{self._tenant}\x1f{key}"},
            )
            row = cur.fetchone()
        return bool(row["taken"]) if row else False

    def lock_admission(self, key: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute("SET LOCAL lock_timeout = '30s'")
            if self._tenant:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%(key)s, 0))",
                    {"key": f"admission\x1f{self._tenant}\x1f{key}"},
                )
            else:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(%(class_id)s, hashtext(%(key)s))",
                    {"class_id": ADMISSION_LOCK_CLASS, "key": key},
                )

    def count_running(self, pipeline_config_id: str) -> int:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) AS n FROM pipeline_executions "
                "WHERE pipeline_config_id = %(c)s AND status = 'RUNNING' AND stabilize_tenant = %(tenant)s",
                {"c": pipeline_config_id, "tenant": self._tenant},
            )
            row = cur.fetchone()
        return int(row["n"] if isinstance(row, dict) else row[0])

    def buffered_ids(self, pipeline_config_id: str, queue_table: str | None = None) -> list[str]:
        scope = ""
        params: dict[str, Any] = {"c": pipeline_config_id, "tenant": self._tenant}
        if queue_table is not None:
            scope = " AND (queue_table = %(queue_table)s OR (%(own_default)s AND queue_table IS NULL))"
            params["queue_table"] = queue_table
            params["own_default"] = queue_table == "queue_messages"
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM pipeline_executions "
                "WHERE pipeline_config_id = %(c)s AND status = 'BUFFERED' "
                f"AND stabilize_tenant = %(tenant)s{scope} ORDER BY id",
                params,
            )
            rows = cur.fetchall()
        return [r["id"] if isinstance(r, dict) else r[0] for r in rows]

    def workflow_status(self, workflow_id: str) -> tuple[str | None, bool]:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT status, is_canceled FROM pipeline_executions "
                "WHERE id = %(id)s AND stabilize_tenant = %(tenant)s",
                {"id": workflow_id, "tenant": self._tenant},
            )
            row = cur.fetchone()
        if row is None:
            return None, False
        if isinstance(row, dict):
            return str(row["status"]), bool(row["is_canceled"])
        return str(row[0]), bool(row[1])

    def paused_stage_ids(self, workflow_id: str) -> list[str]:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM stage_executions "
                "WHERE execution_id = %(id)s AND status = 'PAUSED' AND stabilize_tenant = %(tenant)s ORDER BY id",
                {"id": workflow_id, "tenant": self._tenant},
            )
            rows = cur.fetchall()
        return [str(row["id"]) if isinstance(row, dict) else str(row[0]) for row in rows]


# Forward reference for type hint
from stabilize.persistence.postgres.store import (  # noqa: E402, F401
    PostgresWorkflowStore,
)


def _queue_row_id(message_id: str | None) -> int | None:
    """The queue row behind a delivered message id; None when the id names no row (a test or foreign id)."""
    if not message_id:
        return None
    from stabilize.queue.message_ids import row_id

    try:
        return int(row_id(message_id))
    except (TypeError, ValueError):
        return None
