"""
In-memory execution repository.

Useful for testing and development.
"""

from __future__ import annotations

import copy
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.workflow import PausedDetails, Workflow
from stabilize.persistence.store import (
    StoreTransaction,
    WorkflowCriteria,
    WorkflowNotFoundError,
    WorkflowStore,
)

if TYPE_CHECKING:
    from stabilize.queue.messages import Message
    from stabilize.queue.queue import Queue


class InMemoryWorkflowStore(WorkflowStore):
    """
    In-memory implementation of WorkflowStore.

    Thread-safe storage for testing and single-process execution.
    """

    def __init__(self, max_entries: int = 10000) -> None:
        self._executions: dict[str, Workflow] = {}
        self._lock = threading.Lock()
        self.max_entries = max_entries

    def store(self, execution: Workflow) -> None:
        """Store a complete execution."""
        with self._lock:
            # Check for eviction if new entry
            if execution.id not in self._executions and len(self._executions) >= self.max_entries:
                # Evict oldest (FIFO)
                oldest_id = next(iter(self._executions))
                del self._executions[oldest_id]

            # Deep copy to prevent external modifications
            self._executions[execution.id] = copy.deepcopy(execution)

    def retrieve(self, execution_id: str) -> Workflow:
        """Retrieve an execution by ID."""
        with self._lock:
            if execution_id not in self._executions:
                raise WorkflowNotFoundError(execution_id)
            # Return a deep copy to prevent external modifications
            execution = copy.deepcopy(self._executions[execution_id])
            # Fixup weakrefs after deepcopy
            for stage in execution.stages:
                stage.execution = execution
                for task in stage.tasks:
                    task.stage = stage
            return execution

    def retrieve_execution_summary(self, execution_id: str) -> Workflow:
        """Retrieve execution metadata without stages."""
        with self._lock:
            if execution_id not in self._executions:
                raise WorkflowNotFoundError(execution_id)

            # Deep copy but clear stages
            execution = copy.deepcopy(self._executions[execution_id])
            execution.stages = []
            return execution

    def update_status(self, execution: Workflow) -> None:
        """Update execution status."""
        with self._lock:
            if execution.id not in self._executions:
                raise WorkflowNotFoundError(execution.id)

            stored = self._executions[execution.id]
            stored.status = execution.status
            stored.start_time = execution.start_time
            stored.end_time = execution.end_time
            stored.is_canceled = execution.is_canceled
            stored.canceled_by = execution.canceled_by
            stored.cancellation_reason = execution.cancellation_reason
            stored.paused = execution.paused

    def delete(self, execution_id: str) -> None:
        """Delete an execution."""
        with self._lock:
            if execution_id in self._executions:
                del self._executions[execution_id]

    def store_stage(self, stage: StageExecution) -> None:
        """Store or update a stage."""
        with self._lock:
            execution_id = stage.execution.id
            if execution_id not in self._executions:
                raise WorkflowNotFoundError(execution_id)

            execution = self._executions[execution_id]

            # Find and update or add
            for i, s in enumerate(execution.stages):
                if s.id == stage.id:
                    # Update existing stage
                    execution.stages[i] = copy.deepcopy(stage)
                    execution.stages[i].execution = execution
                    return

            # Add new stage
            new_stage = copy.deepcopy(stage)
            new_stage.execution = execution
            execution.stages.append(new_stage)

    def add_stage(self, stage: StageExecution) -> None:
        """Add a new stage."""
        self.store_stage(stage)

    def remove_stage(
        self,
        execution: Workflow,
        stage_id: str,
    ) -> None:
        """Remove a stage."""
        with self._lock:
            if execution.id not in self._executions:
                raise WorkflowNotFoundError(execution.id)

            stored = self._executions[execution.id]
            stored.stages = [s for s in stored.stages if s.id != stage_id]

    def retrieve_stage(self, stage_id: str) -> StageExecution:
        """Retrieve a single stage by ID."""
        with self._lock:
            for execution in self._executions.values():
                for stage in execution.stages:
                    if stage.id == stage_id:
                        # Return deep copy with lightweight execution
                        stage_copy = copy.deepcopy(stage)

                        exec_copy = copy.deepcopy(execution)
                        exec_copy.stages = [stage_copy]  # Only include this stage
                        # Use strong reference because we are returning the stage,
                        # and the partial execution is owned by the stage in this context.
                        stage_copy.set_execution_strong(exec_copy)

                        return stage_copy

            raise ValueError(f"Stage {stage_id} not found")

    def get_upstream_stages(
        self,
        execution_id: str,
        stage_ref_id: str,
    ) -> list[StageExecution]:
        """Get upstream stages."""
        with self._lock:
            if execution_id not in self._executions:
                return []

            execution = self._executions[execution_id]

            # Find target stage to get requisites
            target_stage = next((s for s in execution.stages if s.ref_id == stage_ref_id), None)
            if not target_stage:
                return []

            return [copy.deepcopy(s) for s in execution.stages if s.ref_id in target_stage.requisite_stage_ref_ids]

    def get_downstream_stages(
        self,
        execution_id: str,
        stage_ref_id: str,
    ) -> list[StageExecution]:
        """Get downstream stages."""
        with self._lock:
            if execution_id not in self._executions:
                return []

            execution = self._executions[execution_id]

            return [copy.deepcopy(s) for s in execution.stages if stage_ref_id in s.requisite_stage_ref_ids]

    def get_synthetic_stages(
        self,
        execution_id: str,
        parent_stage_id: str,
    ) -> list[StageExecution]:
        """Get synthetic stages."""
        with self._lock:
            if execution_id not in self._executions:
                return []

            execution = self._executions[execution_id]

            return [copy.deepcopy(s) for s in execution.stages if s.parent_stage_id == parent_stage_id]

    def get_merged_ancestor_outputs(
        self,
        execution_id: str,
        stage_ref_id: str,
    ) -> dict[str, Any]:
        """Get merged outputs from all ancestor stages."""
        with self._lock:
            if execution_id not in self._executions:
                return {}

            execution = self._executions[execution_id]

            # Use Workflow.get_context logic but filtered for this stage
            # Or reuse the graph logic from other stores

            # Since we have the full execution in memory here, we can use
            # topological sort on the full graph and just filter ancestors
            from stabilize.dag.topological import topological_sort

            # Build ancestor set
            target_stage = next((s for s in execution.stages if s.ref_id == stage_ref_id), None)
            if not target_stage:
                return {}

            ancestors = set()
            queue = [target_stage]
            visited = {target_stage.id}

            stage_map = {s.ref_id: s for s in execution.stages}

            while queue:
                current = queue.pop(0)
                for req_ref in current.requisite_stage_ref_ids:
                    if req_ref in stage_map:
                        req_stage = stage_map[req_ref]
                        if req_stage.id not in visited:
                            visited.add(req_stage.id)
                            ancestors.add(req_stage.id)
                            queue.append(req_stage)

            # Sort full list and filter
            sorted_stages = topological_sort(execution.stages)

            result: dict[str, Any] = {}
            for stage in sorted_stages:
                if stage.id in ancestors:
                    for key, value in stage.outputs.items():
                        if key in result and isinstance(result[key], list) and isinstance(value, list):
                            existing = result[key]
                            for item in value:
                                if item not in existing:
                                    existing.append(item)
                        else:
                            result[key] = value

            return result

    def retrieve_by_pipeline_config_id(
        self,
        pipeline_config_id: str,
        criteria: WorkflowCriteria | None = None,
    ) -> Iterator[Workflow]:
        """Retrieve executions by pipeline config ID."""
        with self._lock:
            executions = [
                copy.deepcopy(e) for e in self._executions.values() if e.pipeline_config_id == pipeline_config_id
            ]

        # Fixup weakrefs
        for execution in executions:
            for stage in execution.stages:
                stage.execution = execution
                for task in stage.tasks:
                    task.stage = stage

        # Apply criteria
        executions = self._apply_criteria(executions, criteria)

        yield from executions

    def retrieve_by_application(
        self,
        application: str,
        criteria: WorkflowCriteria | None = None,
    ) -> Iterator[Workflow]:
        """Retrieve executions by application."""
        with self._lock:
            executions = [copy.deepcopy(e) for e in self._executions.values() if e.application == application]

        # Fixup weakrefs
        for execution in executions:
            for stage in execution.stages:
                stage.execution = execution
                for task in stage.tasks:
                    task.stage = stage

        # Apply criteria
        executions = self._apply_criteria(executions, criteria)

        yield from executions

    def _apply_criteria(
        self,
        executions: list[Workflow],
        criteria: WorkflowCriteria | None,
    ) -> list[Workflow]:
        """Apply query criteria to executions."""
        if criteria is None:
            return executions

        # Filter by status
        if criteria.statuses:
            executions = [e for e in executions if e.status in criteria.statuses]

        # Filter by start time
        if criteria.start_time_before:
            executions = [e for e in executions if e.start_time and e.start_time < criteria.start_time_before]

        if criteria.start_time_after:
            executions = [e for e in executions if e.start_time and e.start_time > criteria.start_time_after]

        # Sort by start time (newest first) and limit
        executions.sort(key=lambda e: e.start_time or 0, reverse=True)
        return executions[: criteria.page_size]

    def pause(self, execution_id: str, paused_by: str) -> None:
        """Pause an execution."""
        with self._lock:
            if execution_id not in self._executions:
                raise WorkflowNotFoundError(execution_id)

            execution = self._executions[execution_id]
            execution.paused = PausedDetails(
                paused_by=paused_by,
                pause_time=int(time.time() * 1000),
            )
            execution.status = WorkflowStatus.PAUSED

    def resume(self, execution_id: str) -> None:
        """Resume a paused execution."""
        with self._lock:
            if execution_id not in self._executions:
                raise WorkflowNotFoundError(execution_id)

            execution = self._executions[execution_id]
            if execution.paused:
                current_time = int(time.time() * 1000)
                execution.paused.resume_time = current_time
                if execution.paused.pause_time:
                    execution.paused.paused_ms = current_time - execution.paused.pause_time
            execution.status = WorkflowStatus.RUNNING

    def cancel(
        self,
        execution_id: str,
        canceled_by: str,
        reason: str,
    ) -> None:
        """Cancel an execution."""
        with self._lock:
            if execution_id not in self._executions:
                raise WorkflowNotFoundError(execution_id)

            execution = self._executions[execution_id]
            execution.is_canceled = True
            execution.canceled_by = canceled_by
            execution.cancellation_reason = reason

    def clear(self) -> None:
        """Clear all executions."""
        with self._lock:
            self._executions.clear()

    def count(self) -> int:
        """Get total number of executions."""
        with self._lock:
            return len(self._executions)

    @contextmanager
    def transaction(self, queue: Queue | None = None) -> Iterator[StoreTransaction]:
        """Create atomic transaction for store + queue operations."""
        txn = InMemoryTransaction(self, queue)
        try:
            yield txn
            txn.commit()
        except Exception:
            # No rollback needed here as commit() does it internally or hasn't run yet
            raise


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

        from stabilize.queue.queue import QueueFullError

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
