"""In-memory transaction implementation."""

from __future__ import annotations

import copy
from typing import TYPE_CHECKING

from stabilize.persistence.store import StoreTransaction

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.workflow import Workflow
    from stabilize.queue import Queue
    from stabilize.queue.messages import Message

    from .store import InMemoryWorkflowStore


class InMemoryTransaction(StoreTransaction):
    """Atomic transaction for InMemoryWorkflowStore."""

    def __init__(self, store: InMemoryWorkflowStore, queue: Queue | None) -> None:
        self._store = store
        self._queue = queue
        self._stages: list[StageExecution] = []
        self._workflows: list[Workflow] = []
        self._messages: list[tuple[Message, int]] = []
        self._processed: list[tuple[str, str | None, str | None]] = []

    def store_stage(self, stage: StageExecution) -> None:
        self._stages.append(stage)

    def update_workflow_status(self, workflow: Workflow) -> None:
        self._workflows.append(workflow)

    def push_message(self, message: Message, delay: int = 0) -> None:
        self._messages.append((message, delay))

    def mark_message_processed(
        self,
        message_id: str,
        handler_type: str | None = None,
        execution_id: str | None = None,
    ) -> None:
        self._processed.append((message_id, handler_type, execution_id))

    def commit(self) -> None:
        """Apply changes atomically."""
        from datetime import timedelta

        from stabilize.queue import QueueFullError

        with self._store._lock:
            # 1. Snapshot affected executions for rollback
            snapshot: dict[str, Workflow] = {}
            affected_ids = set()

            for s in self._stages:
                affected_ids.add(s.execution.id)
            for w in self._workflows:
                affected_ids.add(w.id)

            for eid in affected_ids:
                if eid in self._store._executions:
                    snapshot[eid] = copy.deepcopy(self._store._executions[eid])

            try:
                # 2. Apply store updates
                # Use internal methods or direct access to avoid locking (we already have lock)
                # But store methods take lock. We should duplicate logic or use reentrant lock?
                # Threading.Lock is NOT reentrant. RLock is.
                # store._lock is Lock().
                # So we must modify _executions directly.

                # Update workflows
                for workflow in self._workflows:
                    if workflow.id not in self._store._executions:
                        continue  # Should raise?
                    stored = self._store._executions[workflow.id]
                    stored.status = workflow.status
                    stored.start_time = workflow.start_time
                    stored.end_time = workflow.end_time
                    stored.is_canceled = workflow.is_canceled
                    stored.canceled_by = workflow.canceled_by
                    stored.cancellation_reason = workflow.cancellation_reason
                    stored.paused = workflow.paused

                # Update stages
                for stage in self._stages:
                    execution_id = stage.execution.id
                    if execution_id not in self._store._executions:
                        continue  # Should raise?

                    execution = self._store._executions[execution_id]

                    # Find and update or add
                    found = False
                    for i, s in enumerate(execution.stages):
                        if s.id == stage.id:
                            execution.stages[i] = copy.deepcopy(stage)
                            execution.stages[i].execution = execution
                            found = True
                            break

                    if not found:
                        new_stage = copy.deepcopy(stage)
                        new_stage.execution = execution
                        execution.stages.append(new_stage)

                # 3. Push messages (can fail if queue full)
                if self._queue and self._messages:
                    for message, delay in self._messages:
                        if delay > 0:
                            self._queue.push(message, timedelta(seconds=delay))
                        else:
                            self._queue.push(message)

                # 4. Mark processed (no-op in memory usually, but if tracked...)
                # InMemoryWorkflowStore doesn't track processed messages in this impl?
                # mark_message_processed is in WorkflowStore base class as no-op.
                # If we want to support it, we should add it to InMemoryWorkflowStore.
                # But for now, we just ignore or log.

            except QueueFullError:
                # Rollback!
                for eid, backup in snapshot.items():
                    self._store._executions[eid] = backup
                raise
            except Exception:
                # Rollback for any other error
                for eid, backup in snapshot.items():
                    self._store._executions[eid] = backup
                raise
