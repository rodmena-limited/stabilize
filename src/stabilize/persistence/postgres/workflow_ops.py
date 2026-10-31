"""The PostgreSQL store's workflow-row operations: summaries, status, deletion, listings, health."""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, Any

from stabilize.models.workflow import Workflow
from stabilize.persistence.columns import select_list
from stabilize.persistence.postgres.converters import (
    paused_to_dict,
    row_to_execution,
)
from stabilize.persistence.postgres.operations import (
    cancel_execution,
    pause_execution,
    resume_execution,
)
from stabilize.persistence.postgres.queries import get_all_pending_workflows as _get_all_pending_workflows
from stabilize.persistence.postgres.queries import (
    retrieve_by_application as _retrieve_by_application,
)
from stabilize.persistence.postgres.queries import (
    retrieve_by_pipeline_config_id as _retrieve_by_pipeline_config_id,
)
from stabilize.persistence.store import (
    WorkflowCriteria,
    WorkflowNotFoundError,
)
from stabilize.persistence.store.owned_rows import WORKFLOW_OWNED_ROWS

_PIPELINE = select_list("pipeline_executions", "postgres")


class PostgresWorkflowOpsMixin:
    tenant: str
    _engine: Any
    _pool: Any
    _health_timeout: float

    def retrieve(self, execution_id: str) -> Workflow:
        raise NotImplementedError

    def _borrow(self) -> AbstractContextManager[Any]:
        raise NotImplementedError

    if TYPE_CHECKING:

        def transaction(self, queue: Any | None = None) -> AbstractContextManager[Any]: ...

    if TYPE_CHECKING:

        def transaction(self, queue: Any | None = None) -> AbstractContextManager[Any]: ...

    def recovery_candidate_ids(self, queue_table: str | None, after_id: str | None, limit: int) -> list[str]:
        """Ids of this tenant's live workflows on *queue_table*, keyset-paged by id; the default table owns NULL."""
        own_default = (queue_table or "queue_messages") == "queue_messages"
        with self._borrow() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM pipeline_executions WHERE stabilize_tenant = %(tenant)s "
                    "AND status = ANY(%(statuses)s) AND (queue_table = %(queue_table)s "
                    "OR (%(own_default)s AND queue_table IS NULL)) "
                    "AND (%(after_id)s::text IS NULL OR id > %(after_id)s) ORDER BY id LIMIT %(limit)s",
                    {
                        "tenant": self.tenant,
                        "statuses": ["RUNNING", "NOT_STARTED", "BUFFERED"],
                        "queue_table": queue_table or "queue_messages",
                        "own_default": own_default,
                        "after_id": after_id,
                        "limit": int(limit),
                    },
                )
                rows = cur.fetchall()
        return [str(r["id"]) for r in rows]

    def retrieve_execution_summary(self, execution_id: str) -> Workflow:
        """Retrieve execution metadata without stages."""
        with self._borrow() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT {_PIPELINE} FROM pipeline_executions WHERE id = %(id)s AND stabilize_tenant = %(tenant)s",
                    {"id": execution_id, "tenant": self.tenant},
                )
                row = cur.fetchone()
                if not row:
                    raise WorkflowNotFoundError(execution_id)

                return row_to_execution(row)

    def update_status(self, execution: Workflow) -> None:
        """Update execution status."""
        with self._borrow() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE pipeline_executions SET
                        status = %(status)s,
                        start_time = %(start_time)s,
                        end_time = %(end_time)s,
                        is_canceled = %(is_canceled)s,
                        canceled_by = %(canceled_by)s,
                        cancellation_reason = %(cancellation_reason)s,
                        paused = %(paused)s::jsonb
                    WHERE id = %(id)s AND stabilize_tenant = %(tenant)s
                    """,
                    {
                        "id": execution.id,
                        "tenant": self.tenant,
                        "status": execution.status.name,
                        "start_time": execution.start_time,
                        "end_time": execution.end_time,
                        "is_canceled": execution.is_canceled,
                        "canceled_by": execution.canceled_by,
                        "cancellation_reason": execution.cancellation_reason,
                        "paused": (json.dumps(paused_to_dict(execution.paused)) if execution.paused else None),
                    },
                )
            conn.commit()

    def delete(self, execution_id: str) -> None:
        """Delete an execution and every store row it owns.

        Queue and dead-letter rows belong to the queue: see Queue.purge_workflow.
        """
        with self._borrow() as conn:
            with conn.cursor() as cur:
                for table, column in WORKFLOW_OWNED_ROWS:
                    cur.execute(
                        f"DELETE FROM {table} WHERE {column} = %(id)s AND stabilize_tenant = %(tenant)s",
                        {"id": execution_id, "tenant": self.tenant},
                    )
            conn.commit()

    def retrieve_by_pipeline_config_id(
        self,
        pipeline_config_id: str,
        criteria: WorkflowCriteria | None = None,
    ) -> Iterator[Workflow]:
        """Retrieve executions by pipeline config ID."""
        return _retrieve_by_pipeline_config_id(self._engine, pipeline_config_id, criteria, self.retrieve)

    def retrieve_by_application(
        self,
        application: str,
        criteria: WorkflowCriteria | None = None,
    ) -> Iterator[Workflow]:
        """Retrieve executions by application."""
        return _retrieve_by_application(self._engine, application, criteria, self.retrieve)

    def get_all_pending_workflows(
        self,
        criteria: WorkflowCriteria | None = None,
    ) -> Iterator[Workflow]:
        """Get all workflows matching criteria across all applications (for recovery)."""
        return _get_all_pending_workflows(self._engine, criteria, self.retrieve)

    def pause(self, execution_id: str, paused_by: str) -> None:
        """Pause an execution, recording workflow.paused in the same transaction."""
        from stabilize.events.recorder.instance import record_workflow_paused_for
        from stabilize.events.txn_scope import scope_connection

        with self.transaction():
            paused = pause_execution(scope_connection(), execution_id, paused_by, self.tenant)
            if paused is not None:
                record_workflow_paused_for(self.tenant, execution_id, paused)

    def resume(self, execution_id: str) -> None:
        """Resume a paused execution, recording workflow.resumed in the same transaction."""
        from stabilize.events.recorder.instance import record_workflow_resumed_for
        from stabilize.events.txn_scope import scope_connection

        with self.transaction():
            paused = resume_execution(scope_connection(), execution_id, self.tenant)
            if paused is not None:
                record_workflow_resumed_for(self.tenant, execution_id, paused, "WorkflowStore.resume")

    def cancel(self, execution_id: str, canceled_by: str, reason: str) -> None:
        """Cancel an execution."""
        cancel_execution(self._engine, execution_id, canceled_by, reason)

    def is_healthy(self, timeout: float | None = None) -> bool:
        """Whether the database answers, within a bounded interval.

        The failure path is the only path this method exists for, so it must
        not inherit the pool's 30s acquisition default: a liveness probe with a
        5s budget records a timeout rather than a negative answer, and the
        caller cannot then tell "unhealthy" from "did not reply".

        Worst case is ``timeout`` seconds (default
        ``DEFAULT_HEALTH_TIMEOUT_SECONDS``) waiting for a connection, plus the
        round trip of ``SELECT 1`` on a connection already borrowed.
        """
        budget = self._health_timeout if timeout is None else timeout
        try:
            with self._pool.connection(timeout=budget) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            return True
        except Exception:
            return False
